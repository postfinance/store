package hash

import (
	"context"
	"fmt"

	"github.com/postfinance/store"
)

// Watch a key or prefix
// WithPrefix, WithContext, WithNotifyCreated, WithErrorHandler are supported
func (h *Backend) Watch(key string, w store.Watcher, ops ...store.WatchOption) error {
	opts := &store.WatchOptions{}

	for _, op := range ops {
		op.SetWatchOption(opts)
	}

	if err := w.BeforeWatch(); err != nil {
		return err
	}

	ctx := context.Background() // will never cancel
	if opts.Context != nil {
		ctx = opts.Context
	}

	// send errors to nirvana or, if given, to the error channel
	handleError := func(err error) error {
		return nil
	}
	if opts.ErrorHandler != nil {
		handleError = func(err error) error {
			if err == nil {
				return nil
			}

			return opts.ErrorHandler(err)
		}
	}

	if err := w.BeforeLoop(); err != nil {
		return err
	}

	// loop is here a registered callback
	h.register(h.AbsKey(key), opts.Prefix, func(msg changeNotification) {
		key := []byte(h.RelKey(msg.Data.Key))

		switch msg.Type {
		case put:
			_ = handleError(w.OnPut(key, msg.Data.Value))
		case del:
			_ = handleError(w.OnDelete(key, msg.Data.Value))
		default:
			_ = handleError(fmt.Errorf("watch received an unknown event type: %s", msg.Type))
		}
	})

	// notify the caller that Watch is created and ready to receive events
	if opts.NotifyCreated != nil {
		opts.NotifyCreated()
	}

	// wait for cancellation of the context
	<-ctx.Done()

	return w.OnDone()
}
