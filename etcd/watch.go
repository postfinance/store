package etcd

import (
	"context"
	"fmt"
	"time"

	"github.com/postfinance/store"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Watch installs a watcher on key
//nolint:gocognit,gocyclo
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
	if err := func() error {
		//nolint:godox // TODO: use global etcd timeout
		timeout := time.Second * 5
		tCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		select {
		case resp := <-rch:
			if !resp.Created {
				return fmt.Errorf("watch not created: %s", resp.Err())
			}
		case <-tCtx.Done():
			return fmt.Errorf("watch not created, timeout of %v exceeded", timeout)
		}
		return nil
	}(); err != nil {
		return err
	}

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

				switch ev.Type.String() {
				case "PUT":
					_ = handleError(w.OnPut(key, ev.Kv.Value))
				case "DELETE":
					_ = handleError(w.OnDelete(key, ev.Kv.Value))
				default:
					_ = handleError(fmt.Errorf("watch received an unknown event type: %s", ev.Type.String()))
				}
			}
		}
	}
}
