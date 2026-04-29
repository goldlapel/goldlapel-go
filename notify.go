package goldlapel

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
)

// DocWatch listens for changes on a collection. Like MongoDB's change streams.
// Creates a trigger that fires NOTIFY on INSERT/UPDATE/DELETE, then streams
// change events to the callback via LISTEN/NOTIFY.
// The callback receives the operation ("INSERT", "UPDATE", "DELETE") and the
// affected row's data as a JSON string.
// Runs in a background goroutine. The returned channel receives an error if
// setup fails; otherwise it is closed on success. The goroutine exits when
// ctx is cancelled.
//
// Pass a connection string (DSN) for listenConn, not the transactional q:
// LISTEN requires its own dedicated connection outside the pool.
func DocWatch(ctx context.Context, q execQuerier, listenConn, collection string, callback func(op, data string)) (chan error, error) {
	if err := validateIdentifier(collection); err != nil {
		return nil, err
	}
	if err := ensureCollection(ctx, q, collection); err != nil {
		return nil, err
	}
	return docWatchOn(ctx, q, listenConn, collection, collection, callback)
}

// docWatchOn installs the change-stream trigger against tableName and uses
// nameBase (typically the user-supplied collection) for trigger / function /
// notify-channel naming so the names stay readable and stable even when
// tableName is the proxy-canonical (_goldlapel.doc_<name>).
func docWatchOn(ctx context.Context, q execQuerier, listenConn, tableName, nameBase string, callback func(op, data string)) (chan error, error) {
	channel := nameBase + "_changes"
	funcName := nameBase + "_notify_changes"
	triggerName := nameBase + "_watch_trigger"

	createFunc := "CREATE OR REPLACE FUNCTION " + funcName + "() RETURNS trigger AS $$ " +
		"BEGIN " +
		"IF TG_OP = 'DELETE' THEN " +
		"PERFORM pg_notify('" + channel + "', TG_OP || '|' || OLD.data::text); " +
		"RETURN OLD; " +
		"ELSE " +
		"PERFORM pg_notify('" + channel + "', TG_OP || '|' || NEW.data::text); " +
		"RETURN NEW; " +
		"END IF; " +
		"END; " +
		"$$ LANGUAGE plpgsql"
	if _, err := q.ExecContext(ctx, createFunc); err != nil {
		return nil, fmt.Errorf("create watch function: %w", err)
	}

	// CREATE OR REPLACE TRIGGER (Postgres 14+) is atomic — avoids the race
	// where a DROP + CREATE pair could have two concurrent DocWatch calls
	// replace each other's triggers mid-flight and end up with a partially
	// dropped one. GL targets PG14+ across the product, so this is safe.
	createTrigger := "CREATE OR REPLACE TRIGGER " + triggerName +
		" AFTER INSERT OR UPDATE OR DELETE ON " + tableName +
		" FOR EACH ROW EXECUTE FUNCTION " + funcName + "()"
	if _, err := q.ExecContext(ctx, createTrigger); err != nil {
		return nil, fmt.Errorf("create watch trigger: %w", err)
	}

	errCh := make(chan error, 1)
	go func() {
		minReconn := 10 * time.Second
		maxReconn := time.Minute
		listener := pq.NewListener(listenConn, minReconn, maxReconn, nil)
		defer listener.Close()

		if err := listener.Listen(channel); err != nil {
			errCh <- fmt.Errorf("listen on channel %q: %w", channel, err)
			return
		}
		close(errCh)

		for {
			select {
			case <-ctx.Done():
				return
			case n := <-listener.Notify:
				if n == nil {
					continue
				}
				parts := strings.SplitN(n.Extra, "|", 2)
				if len(parts) == 2 {
					callback(parts[0], parts[1])
				}
			}
		}
	}()

	return errCh, nil
}

// DocUnwatch removes the change stream trigger and function from a collection.
func DocUnwatch(ctx context.Context, q execQuerier, collection string) error {
	if err := validateIdentifier(collection); err != nil {
		return err
	}
	return docUnwatchOn(ctx, q, collection, collection)
}

func docUnwatchOn(ctx context.Context, q execQuerier, tableName, nameBase string) error {
	triggerName := nameBase + "_watch_trigger"
	funcName := nameBase + "_notify_changes"

	dropTrigger := "DROP TRIGGER IF EXISTS " + triggerName + " ON " + tableName
	if _, err := q.ExecContext(ctx, dropTrigger); err != nil {
		return fmt.Errorf("drop watch trigger: %w", err)
	}

	dropFunc := "DROP FUNCTION IF EXISTS " + funcName + "()"
	if _, err := q.ExecContext(ctx, dropFunc); err != nil {
		return fmt.Errorf("drop watch function: %w", err)
	}

	return nil
}
