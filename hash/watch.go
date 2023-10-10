package hash

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/postfinance/store"
	"github.com/postfinance/store/internal/common"
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
		handleError = opts.ErrorHandler
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

// WatchChan creates a watcher for a key or prefix and unmarshal events into channel.
// The channel elements have to implement the store.KeyOpSetter interface.
func (h *Backend) WatchChan(key string, channel interface{}, errChan chan error, ops ...store.WatchOption) (store.WatchStarter, error) {
	if errChan == nil {
		return nil, errors.New("error channel cannot be nil")
	}

	w, err := common.NewChannelSender(channel, nil, h.SplitKey)
	if err != nil {
		return nil, err
	}

	opts := &store.WatchOptions{}

	for _, op := range ops {
		op.SetWatchOption(opts)
	}

	ctx := context.Background() // will never cancel
	if opts.Context != nil {
		ctx = opts.Context
	}

	h.register(h.AbsKey(key), opts.Prefix, h.handleChangeNotification(w, errChan))

	// notify the caller that Watch is created and ready to receive events
	if opts.NotifyCreated != nil {
		opts.NotifyCreated()
	}

	return &ChannelWatcher{
		ctx: ctx,
		w:   w,
	}, nil
}

// ChannelWatcher implements the WatchStarter interface.
type ChannelWatcher struct {
	ctx context.Context
	w   *common.ChannelSender
}

// Start starts the watcher and blocks until context is canceled.
func (c ChannelWatcher) Start() {
	// wait for cancellation of the context
	<-c.ctx.Done()
}

// Start starts the watcher and blocks until context is canceled.
func (h *Backend) handleChangeNotification(w *common.ChannelSender, errChan chan error) func(msg changeNotification) {
	return func(msg changeNotification) {
		var (
			key = h.RelKey(msg.Data.Key)
			op  store.Operation
		)

		switch msg.Type {
		case put:
			if msg.isCreate {
				op = store.Create
			} else {
				op = store.Update
			}
		case del:
			op = store.Delete
		default:
			errChan <- fmt.Errorf("watch received an unknown event type: %s", msg.Type)
		}

		_ = w.Send(key, op, msg.Data.Value)
	}
}
