package etcd

import (
	"context"
	"fmt"
	"time"

	"github.com/postfinance/store"
	"github.com/postfinance/store/internal/common"

	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Watch installs a watcher on key
func (e *Backend) Watch(key string, w store.Watcher, ops ...store.WatchOption) error {
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

	// context to cancel the watcher on exit of Watch the function
	// we cannot use an external context here because the Watch
	// could be canceled and therefore the channel closed before
	// we stopped reading from the watch channel
	wCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// setup Watch
	etcdOpts := []clientv3.OpOption{clientv3.WithCreatedNotify()}
	if opts.Prefix {
		etcdOpts = append(etcdOpts, clientv3.WithPrefix())
	}

	rch := e.client.Watch(clientv3.WithRequireLeader(wCtx), e.AbsKey(key), etcdOpts...)

	// wait until the Watch is created or timeout exceeded
	if err := waitForCreateEvent(ctx, rch, 5*time.Second); err != nil {
		return err
	}

	handleError := func(err error) error {
		return nil
	}
	if opts.ErrorHandler != nil {
		handleError = opts.ErrorHandler
	}

	if err := w.BeforeLoop(); err != nil {
		return err
	}

	// notify the caller that Watch is created and ready to receive events
	if opts.NotifyCreated != nil {
		opts.NotifyCreated()
	}

	for {
		select {
		case <-ctx.Done():
			return w.OnDone()
		case wresp, ok := <-rch:
			if !ok {
				return errors.New("read from closed watch channel")
			}

			if wresp.Canceled {
				return errors.Wrap(wresp.Err(), "watch channel canceled")
			}

			for _, ev := range wresp.Events {
				key := []byte(e.RelKey(string(ev.Kv.Key)))

				switch ev.Type {
				case mvccpb.PUT:
					_ = handleError(w.OnPut(key, ev.Kv.Value))
				case mvccpb.DELETE:
					_ = handleError(w.OnDelete(key, ev.Kv.Value))
				default:
					_ = handleError(fmt.Errorf("watch received an unknown event type: %s", ev.Type.String()))
				}
			}
		}
	}
}

// WatchChan creates a watcher for a key or prefix and unmarshals events into channel.
// The channel elements have to implement the store.KeyOpSetter interface.
func (e *Backend) WatchChan(key string, channel interface{}, errChan chan error, ops ...store.WatchOption) (store.WatchStarter, error) {
	if errChan == nil {
		return nil, errors.New("error channal cannot be nil")
	}

	w, err := common.NewChannelSender(channel, nil)
	if err != nil {
		return nil, err
	}

	opts := &store.WatchOptions{}

	for _, op := range ops {
		op.SetWatchOption(opts)
	}

	// setup Watch
	etcdOpts := []clientv3.OpOption{clientv3.WithCreatedNotify()}
	if opts.Prefix {
		etcdOpts = append(etcdOpts, clientv3.WithPrefix())
	}

	ctx := context.Background() // will never cancel
	if opts.Context != nil {
		ctx = opts.Context
	}

	// context to cancel the watcher on exit of Watch the function
	// we cannot use an external context here because the Watch
	// could be canceled and therefore the channel closed before
	// we stopped reading from the watch channel
	wCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srcChan := e.client.Watch(clientv3.WithRequireLeader(ctx), e.AbsKey(key), etcdOpts...)

	// wait until the Watch is created or timeout exceeded
	if err := waitForCreateEvent(wCtx, srcChan, 5*time.Second); err != nil {
		return nil, err
	}

	if opts.NotifyCreated != nil {
		opts.NotifyCreated()
	}

	// send errors to nirvana or, if given, to the error channel
	handleError := func(err error) error {
		return nil
	}
	if opts.ErrorHandler != nil {
		handleError = opts.ErrorHandler
	}

	return &ChannelWatcher{
		srcChan:     srcChan,
		errChan:     errChan,
		ctx:         ctx,
		relKey:      e.RelKey,
		w:           w,
		handleError: handleError,
	}, nil
}

// ChannelWatcher implements the WatchStarter interface.
type ChannelWatcher struct {
	srcChan     clientv3.WatchChan
	errChan     chan error
	ctx         context.Context
	relKey      func(string) string
	w           *common.ChannelSender
	handleError store.ErrorFunc
}

// Start starts the watcher and blocks until context is canceled.
func (c *ChannelWatcher) Start() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case wresp, ok := <-c.srcChan:
			if !ok {
				c.errChan <- errors.New("read from closed watch channel")
				return
			}

			if wresp.Canceled {
				c.errChan <- errors.Wrap(wresp.Err(), "watch channel canceled")
				return
			}

			for _, ev := range wresp.Events {
				c.handleEvent(ev)
			}
		}
	}
}

func (c *ChannelWatcher) handleEvent(ev *clientv3.Event) {
	var (
		key = c.relKey(string(ev.Kv.Key))
		op  store.Operation
	)

	switch ev.Type {
	case mvccpb.PUT:
		if ev.IsCreate() {
			op = store.Create
		}

		if ev.IsModify() {
			op = store.Update
		}
	case mvccpb.DELETE:
		op = store.Delete
	default:
		c.errChan <- fmt.Errorf("watch received an unknown event type: %s", ev.Type.String())
	}

	if err := c.w.Send(key, op, ev.Kv.Value); err != nil {
		_ = c.handleError(err)
	}
}

func waitForCreateEvent(ctx context.Context, c clientv3.WatchChan, timeout time.Duration) error {
	tCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case resp := <-c:
		if !resp.Created {
			return fmt.Errorf("watch not created: %s", resp.Err())
		}
	case <-tCtx.Done():
		return fmt.Errorf("watch not created, timeout of %v exceeded", timeout)
	}

	return nil
}
