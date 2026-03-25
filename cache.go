package goldlapel

import (
	"fmt"
	"net"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	ddlSentinel             = "__ddl__"
	DefaultInvalidationPort = 7934
)

var (
	txStartRe = regexp.MustCompile(`(?i)^\s*(BEGIN|START\s+TRANSACTION)\b`)
	txEndRe   = regexp.MustCompile(`(?i)^\s*(COMMIT|ROLLBACK|END)\b`)

	tablePattern = regexp.MustCompile(`(?i)\b(?:FROM|JOIN)\s+(?:ONLY\s+)?(?:(\w+)\.)?(\w+)`)

	sqlKeywords = map[string]bool{
		"select": true, "from": true, "where": true, "and": true, "or": true,
		"not": true, "in": true, "exists": true, "between": true, "like": true,
		"is": true, "null": true, "true": true, "false": true, "as": true,
		"on": true, "left": true, "right": true, "inner": true, "outer": true,
		"cross": true, "full": true, "natural": true, "group": true, "order": true,
		"having": true, "limit": true, "offset": true, "union": true, "intersect": true,
		"except": true, "all": true, "distinct": true, "lateral": true, "values": true,
	}
)

type cacheEntry struct {
	Rows   interface{}
	Fields interface{}
	Tables map[string]bool
}

type NativeCache struct {
	cache        map[string]*cacheEntry
	tableIndex   map[string]map[string]bool
	accessOrder  map[string]uint64
	counter      uint64
	maxEntries   int
	enabled      bool
	mu           sync.Mutex
	invConnected bool
	invStop      chan struct{}
	invRunning   bool
	invPort      int
	reconnectN   int

	StatsHits          int64
	StatsMisses        int64
	StatsInvalidations int64
}

var (
	cacheInstance   *NativeCache
	cacheInstanceMu sync.Mutex
)

func GetNativeCache() *NativeCache {
	cacheInstanceMu.Lock()
	defer cacheInstanceMu.Unlock()
	if cacheInstance == nil {
		cacheInstance = newNativeCache()
	}
	return cacheInstance
}

func ResetNativeCache() {
	cacheInstanceMu.Lock()
	defer cacheInstanceMu.Unlock()
	if cacheInstance != nil {
		cacheInstance.StopInvalidation()
		cacheInstance = nil
	}
}

func newNativeCache() *NativeCache {
	maxEntries := 32768
	if s := os.Getenv("GOLDLAPEL_NATIVE_CACHE_SIZE"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			maxEntries = n
		}
	}
	enabled := true
	if s := os.Getenv("GOLDLAPEL_NATIVE_CACHE"); strings.EqualFold(s, "false") {
		enabled = false
	}
	return &NativeCache{
		cache:       make(map[string]*cacheEntry),
		tableIndex:  make(map[string]map[string]bool),
		accessOrder: make(map[string]uint64),
		maxEntries:  maxEntries,
		enabled:     enabled,
	}
}

func (nc *NativeCache) Connected() bool {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	return nc.invConnected
}

func (nc *NativeCache) Enabled() bool {
	return nc.enabled
}

func (nc *NativeCache) Size() int {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	return len(nc.cache)
}

func (nc *NativeCache) Get(sql string, args []interface{}) *cacheEntry {
	if !nc.enabled {
		return nil
	}
	nc.mu.Lock()
	defer nc.mu.Unlock()
	if !nc.invConnected {
		return nil
	}
	key := makeKey(sql, args)
	entry, ok := nc.cache[key]
	if ok {
		nc.counter++
		nc.accessOrder[key] = nc.counter
		nc.StatsHits++
		return entry
	}
	nc.StatsMisses++
	return nil
}

func (nc *NativeCache) Put(sql string, args []interface{}, rows interface{}, fields interface{}) {
	if !nc.enabled {
		return
	}
	nc.mu.Lock()
	defer nc.mu.Unlock()
	if !nc.invConnected {
		return
	}
	key := makeKey(sql, args)
	tables := extractTables(sql)
	if _, exists := nc.cache[key]; !exists && len(nc.cache) >= nc.maxEntries {
		nc.evictOne()
	}
	nc.cache[key] = &cacheEntry{Rows: rows, Fields: fields, Tables: tables}
	nc.counter++
	nc.accessOrder[key] = nc.counter
	for table := range tables {
		if nc.tableIndex[table] == nil {
			nc.tableIndex[table] = make(map[string]bool)
		}
		nc.tableIndex[table][key] = true
	}
}

func (nc *NativeCache) InvalidateTable(table string) {
	table = strings.ToLower(table)
	nc.mu.Lock()
	defer nc.mu.Unlock()
	keys, ok := nc.tableIndex[table]
	if !ok {
		return
	}
	delete(nc.tableIndex, table)
	for key := range keys {
		entry, entryOk := nc.cache[key]
		delete(nc.cache, key)
		delete(nc.accessOrder, key)
		if entryOk && entry != nil {
			for otherTable := range entry.Tables {
				if otherTable != table {
					if otherKeys, ok2 := nc.tableIndex[otherTable]; ok2 {
						delete(otherKeys, key)
						if len(otherKeys) == 0 {
							delete(nc.tableIndex, otherTable)
						}
					}
				}
			}
		}
	}
	nc.StatsInvalidations += int64(len(keys))
}

func (nc *NativeCache) InvalidateAll() {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	count := int64(len(nc.cache))
	nc.cache = make(map[string]*cacheEntry)
	nc.tableIndex = make(map[string]map[string]bool)
	nc.accessOrder = make(map[string]uint64)
	nc.StatsInvalidations += count
}

func (nc *NativeCache) ConnectInvalidation(port int) {
	nc.mu.Lock()
	if nc.invRunning {
		nc.mu.Unlock()
		return
	}
	nc.invPort = port
	nc.invStop = make(chan struct{})
	nc.invRunning = true
	nc.reconnectN = 0
	nc.mu.Unlock()

	go nc.invalidationLoop()
}

func (nc *NativeCache) StopInvalidation() {
	nc.mu.Lock()
	if !nc.invRunning {
		nc.mu.Unlock()
		return
	}
	close(nc.invStop)
	nc.invRunning = false
	nc.invConnected = false
	nc.mu.Unlock()
}

func (nc *NativeCache) invalidationLoop() {
	port := nc.invPort
	sockPath := fmt.Sprintf("/tmp/goldlapel-%d.sock", port)

	for {
		select {
		case <-nc.invStop:
			return
		default:
		}

		var conn net.Conn

		// Try Unix socket first (faster), fall back to TCP
		if runtime.GOOS != "windows" {
			if _, statErr := os.Stat(sockPath); statErr == nil {
				conn, _ = net.DialTimeout("unix", sockPath, 5*time.Second)
			}
		}
		if conn == nil {
			var err error
			conn, err = net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)
			if err != nil {
				nc.scheduleReconnect()
				continue
			}
		}

		nc.mu.Lock()
		nc.invConnected = true
		nc.reconnectN = 0
		nc.mu.Unlock()

		nc.readInvalidations(conn)

		conn.Close()

		nc.mu.Lock()
		wasConnected := nc.invConnected
		nc.invConnected = false
		nc.mu.Unlock()

		if wasConnected {
			nc.InvalidateAll()
		}

		nc.scheduleReconnect()
	}
}

func (nc *NativeCache) readInvalidations(conn net.Conn) {
	buf := make([]byte, 4096)
	var partial string

	for {
		select {
		case <-nc.invStop:
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := conn.Read(buf)
		if n > 0 {
			partial += string(buf[:n])
			for {
				idx := strings.IndexByte(partial, '\n')
				if idx < 0 {
					break
				}
				line := partial[:idx]
				partial = partial[idx+1:]
				nc.processSignal(line)
			}
		}
		if err != nil {
			return
		}
	}
}

func (nc *NativeCache) processSignal(line string) {
	if strings.HasPrefix(line, "I:") {
		table := strings.TrimSpace(line[2:])
		if table == "*" {
			nc.InvalidateAll()
		} else {
			nc.InvalidateTable(table)
		}
	}
	// P: keepalive — ignore
}

func (nc *NativeCache) scheduleReconnect() {
	nc.mu.Lock()
	delay := 1 << nc.reconnectN
	if delay > 15 {
		delay = 15
	}
	nc.reconnectN++
	stopCh := nc.invStop
	nc.mu.Unlock()

	select {
	case <-stopCh:
	case <-time.After(time.Duration(delay) * time.Second):
	}
}

func (nc *NativeCache) evictOne() {
	if len(nc.accessOrder) == 0 {
		return
	}
	var lruKey string
	var minCounter uint64 = ^uint64(0)
	for key, cnt := range nc.accessOrder {
		if cnt < minCounter {
			minCounter = cnt
			lruKey = key
		}
	}
	entry := nc.cache[lruKey]
	delete(nc.cache, lruKey)
	delete(nc.accessOrder, lruKey)
	if entry != nil {
		for table := range entry.Tables {
			if keys, ok := nc.tableIndex[table]; ok {
				delete(keys, lruKey)
				if len(keys) == 0 {
					delete(nc.tableIndex, table)
				}
			}
		}
	}
}

// --- SQL parsing functions ---

func makeKey(sql string, args []interface{}) string {
	if len(args) == 0 {
		return sql + "\x00"
	}
	return sql + "\x00" + fmt.Sprint(args)
}

func detectWrite(sql string) string {
	trimmed := strings.TrimSpace(sql)
	tokens := strings.Fields(trimmed)
	if len(tokens) == 0 {
		return ""
	}
	first := strings.ToUpper(tokens[0])

	switch first {
	case "INSERT":
		if len(tokens) < 3 || !strings.EqualFold(tokens[1], "INTO") {
			return ""
		}
		return bareTable(tokens[2])
	case "UPDATE":
		if len(tokens) < 2 {
			return ""
		}
		return bareTable(tokens[1])
	case "DELETE":
		if len(tokens) < 3 || !strings.EqualFold(tokens[1], "FROM") {
			return ""
		}
		return bareTable(tokens[2])
	case "TRUNCATE":
		if len(tokens) < 2 {
			return ""
		}
		if strings.EqualFold(tokens[1], "TABLE") {
			if len(tokens) < 3 {
				return ""
			}
			return bareTable(tokens[2])
		}
		return bareTable(tokens[1])
	case "CREATE", "ALTER", "DROP", "REFRESH", "DO", "CALL":
		return ddlSentinel
	case "MERGE":
		if len(tokens) < 3 || !strings.EqualFold(tokens[1], "INTO") {
			return ""
		}
		return bareTable(tokens[2])
	case "SELECT":
		sawInto := false
		intoTarget := ""
		for _, tok := range tokens[1:] {
			upper := strings.ToUpper(tok)
			if upper == "INTO" && !sawInto {
				sawInto = true
				continue
			}
			if sawInto && intoTarget == "" {
				if upper == "TEMPORARY" || upper == "TEMP" || upper == "UNLOGGED" {
					continue
				}
				intoTarget = tok
				continue
			}
			if sawInto && intoTarget != "" && upper == "FROM" {
				return ddlSentinel
			}
			if upper == "FROM" {
				return ""
			}
		}
		return ""
	case "COPY":
		if len(tokens) < 2 {
			return ""
		}
		raw := tokens[1]
		if strings.HasPrefix(raw, "(") {
			return ""
		}
		tablePart := strings.SplitN(raw, "(", 2)[0]
		for _, tok := range tokens[2:] {
			upper := strings.ToUpper(tok)
			if upper == "FROM" {
				return bareTable(tablePart)
			}
			if upper == "TO" {
				return ""
			}
		}
		return ""
	case "WITH":
		restUpper := strings.ToUpper(trimmed[len(tokens[0]):])
		for _, token := range strings.Fields(restUpper) {
			word := strings.TrimLeft(token, "(")
			if word == "INSERT" || word == "UPDATE" || word == "DELETE" {
				return ddlSentinel
			}
		}
		return ""
	}

	return ""
}

func bareTable(raw string) string {
	table := strings.SplitN(raw, "(", 2)[0]
	parts := strings.Split(table, ".")
	table = parts[len(parts)-1]
	return strings.ToLower(table)
}

func extractTables(sql string) map[string]bool {
	tables := make(map[string]bool)
	matches := tablePattern.FindAllStringSubmatch(sql, -1)
	for _, m := range matches {
		table := strings.ToLower(m[2])
		if !sqlKeywords[table] {
			tables[table] = true
		}
	}
	return tables
}

func isTxStart(sql string) bool {
	return txStartRe.MatchString(sql)
}

func isTxEnd(sql string) bool {
	return txEndRe.MatchString(sql)
}
