package etcd

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/tests/v3/integration"

	"github.com/postfinance/store"
)

//nolint:gochecknoglobals
var (
	watcherCheckCount int
	prefix            = "etcd|test"
	testData          = []struct {
		key    string
		value  []byte
		update []byte
	}{
		{
			fmt.Sprintf("%s|keyA", prefix),
			[]byte("etcd-test-keyA"),
			[]byte("etcd-test-keyA-update"),
		},
		{
			fmt.Sprintf("%s|keyB", prefix),
			[]byte("etcd-test-keyB"),
			[]byte("etcd-test-keyB-update"),
		},
		{
			fmt.Sprintf("%s|keyC", prefix),
			[]byte("etcd-test-keyC"),
			[]byte("etcd-test-keyC-update"),
		},
	}
	watchReady, putDone, delDone, watchDone chan struct{}
	errOnPut                                = errors.New("error on put")
)

type watcher struct {
	t *testing.T
}

func (w watcher) BeforeWatch() error {
	watcherCheckCount++
	w.t.Log("BeforeWatch +1 ->", watcherCheckCount)

	return nil
}

func (w watcher) BeforeLoop() error {
	watcherCheckCount += 2
	w.t.Log("BeforeLoop +2 ->", watcherCheckCount)

	return nil
}

func (w watcher) OnDone() error {
	watcherCheckCount += 4
	w.t.Log("OnDone +4 ->", watcherCheckCount)

	return nil
}

func (w watcher) OnPut(k, v []byte) error {
	if string(v) == "error" {
		defer close(putDone)
		return errOnPut
	}

	watcherCheckCount += 8
	w.t.Log("OnPut +8 ->", watcherCheckCount)
	close(putDone)

	return nil
}

func (w watcher) OnDelete(k, v []byte) error {
	watcherCheckCount += 16
	w.t.Log("OnDelete +16 ->", watcherCheckCount)
	close(delDone)

	return nil
}

//nolint:funlen
func TestWatch(t *testing.T) {
	integration.BeforeTestExternal(t)

	for _, p := range []string{"", "root"} {
		opts := []Opt{}
		if p != "" {
			opts = append(opts, WithPrefix(p))
		}

		b, _, teardown := setupTestStore(t, false, opts)

		t.Run("watch key", func(t *testing.T) {
			w := watcher{
				t: t,
			}

			watcherCheckCount = 0
			watchReady = make(chan struct{})
			putDone = make(chan struct{})
			delDone = make(chan struct{})
			watchDone = make(chan struct{})

			ctx, cancel := context.WithCancel(context.Background())
			notifyCreated := func() {
				close(watchReady)
			}
			go func() {
				err := b.Watch(testData[0].key, w,
					store.WithContext(ctx),
					store.WithNotifyCreated(notifyCreated),
				)
				require.NoError(t, err)
				close(watchDone)
			}()
			<-watchReady // wait for the channel created an test store.WithNotifyCreated()

			entry := store.Entry{
				Key:   testData[0].key,
				Value: testData[0].value,
			}

			_, err := b.Put(&entry)
			require.NoError(t, err)
			<-putDone

			_, err = b.Del(testData[0].key)
			require.NoError(t, err)
			<-delDone

			cancel() // cancel the Watch and test store.WithContext()
			<-watchDone
			assert.Equal(t, 31, watcherCheckCount)
		})

		t.Run("watch key with prefix", func(t *testing.T) {
			w := watcher{
				t: t,
			}

			watcherCheckCount = 0
			watchReady = make(chan struct{})
			putDone = make(chan struct{})
			delDone = make(chan struct{})
			watchDone = make(chan struct{})

			ctx, cancel := context.WithCancel(context.Background())
			notifyCreated := func() {
				close(watchReady)
			}
			go func() {
				err := b.Watch(prefix, w,
					store.WithPrefix(),
					store.WithContext(ctx),
					store.WithNotifyCreated(notifyCreated),
				)
				require.NoError(t, err)
				close(watchDone)
			}()
			<-watchReady // wait for the channel created an test store.WithNotifyCreated()

			entry := store.Entry{
				Key:   testData[0].key,
				Value: testData[0].value,
			}

			_, err := b.Put(&entry)
			require.NoError(t, err)
			<-putDone

			_, err = b.Del(testData[0].key)
			require.NoError(t, err)
			<-delDone

			cancel() // cancel the Watch and test store.WithContext()
			<-watchDone
			assert.Equal(t, 31, watcherCheckCount)
		})

		t.Run("watch key with error channel", func(t *testing.T) {
			w := watcher{
				t: t,
			}

			watcherCheckCount = 0
			watchReady = make(chan struct{})
			putDone = make(chan struct{})
			delDone = make(chan struct{})
			watchDone = make(chan struct{})

			errChan := make(chan error, 1)
			errHandler := func(err error) error {
				errChan <- err
				return err
			}

			ctx, cancel := context.WithCancel(context.Background())
			notifyCreated := func() {
				close(watchReady)
			}
			go func() {
				err := b.Watch(prefix, w,
					store.WithPrefix(),
					store.WithContext(ctx),
					store.WithNotifyCreated(notifyCreated),
					store.WithErrorHandler(errHandler),
				)
				require.NoError(t, err)
				close(watchDone)
			}()
			<-watchReady // wait for the channel created an test store.WithNotifyCreated()

			entry := store.Entry{
				Key:   testData[0].key,
				Value: []byte("error"),
			}

			_, err := b.Put(&entry)
			require.NoError(t, err)
			<-putDone

			ctxt, cancelTimeout := context.WithTimeout(context.Background(), 5*time.Second)
			select {
			case <-ctxt.Done():
				assert.Fail(t, "no error received")
			case err := <-errChan:
				assert.Equal(t, err, errOnPut)
			}
			cancelTimeout()

			cancel() // cancel the Watch and test store.WithContext()
			<-watchDone
			assert.Equal(t, 7, watcherCheckCount)
		})

		teardown()
	}
}

type Item struct {
	store.EventMeta
	Value string
}

func (i *Item) path() string {
	return i.Key()
}

var _ store.KeyOpSetter = (*Item)(nil)

//nolint:funlen
func TestWatchChan(t *testing.T) {
	integration.BeforeTestExternal(t)

	b, _, teardown := setupTestStore(t, false, []Opt{WithPrefix("")})

	t.Run("watch key with prefix", func(t *testing.T) {
		watchReady = make(chan struct{})
		itemChan := make(chan *Item)
		errChan := make(chan error)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		notifyCreated := func() {
			close(watchReady)
		}

		w, err := b.WatchChan("", itemChan, errChan,
			store.WithPrefix(),
			store.WithNotifyCreated(notifyCreated),
			store.WithContext(ctx),
		)
		require.NoError(t, err)
		<-watchReady // wait for the channel created an test store.WithNotifyCreated()

		go w.Start()

		items := []Item{
			{Value: "1", EventMeta: eventMeta("/1", store.Create)},
			{Value: "2", EventMeta: eventMeta("/1", store.Update)},
			{Value: "3", EventMeta: eventMeta("/2", store.Create)},
		}

		// eventMeta is added to easily use assert.Equal to compare expected
		// with actual values.
		expected := []Item{}
		expected = append(expected, items...)
		expected = append(expected, Item{EventMeta: eventMeta("/2", store.Delete)})

		go func() {
			for i := range items {
				item := Item{
					Value: items[i].Value,
				}
				path := items[i].path()
				_, err = store.Put(b, path, item)
				require.NoError(t, err)
			}

			_, err = b.Del(items[2].path())
			require.NoError(t, err)
		}()

		actual := []Item{}

		for {
			select {
			case item := <-itemChan:
				actual = append(actual, *item)
				if len(actual) == len(expected) {
					assert.Equal(t, expected, actual)
					return
				}
			case err := <-errChan:
				t.Errorf("an error occurred: %v", err)
				return
			case <-ctx.Done():
				t.Error("timeout occurred")
				return
			}
		}
	})

	teardown()
}

func eventMeta(key string, op store.Operation) store.EventMeta {
	e := store.EventMeta{}
	e.SetKey(key)
	e.SetOp(op)

	return e
}
