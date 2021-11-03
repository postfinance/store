package hash_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/postfinance/store"
	"github.com/postfinance/store/hash"
)

//nolint:gochecknoglobals
var (
	prefix   = "etcd|test"
	testData = []struct {
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
	errOnPut = errors.New("error on put")
)

type watcher1 struct {
	CheckCount int
	t          *testing.T
}

func (w *watcher1) add(i int) int {
	w.CheckCount += i
	return w.CheckCount
}

func (w *watcher1) BeforeWatch() error {
	w.t.Log("BeforeWatch +1 ->", w.add(1))
	return nil
}

func (w *watcher1) BeforeLoop() error {
	w.t.Log("BeforeLoop +2 ->", w.add(2))
	return nil
}

func (w *watcher1) OnDone() error {
	w.t.Log("OnDone +4 ->", w.add(4))
	return nil
}

func (w *watcher1) OnPut(k, v []byte) error {
	if string(v) == "error" {
		return errOnPut
	}

	w.t.Log("OnPut +8 ->", w.add(8))

	return nil
}

func (w *watcher1) OnDelete(k, v []byte) error {
	w.t.Log("OnDelete +16 ->", w.add(16))
	return nil
}

//nolint:funlen
func TestWatch(t *testing.T) {
	t.Run("watch key", func(t *testing.T) {
		for _, p := range []string{"", "root"} {
			opts := []hash.Opt{}
			if p != "" {
				opts = append(opts, hash.WithPrefix(p))
			}

			b, err := hash.New(opts...)
			require.NoError(t, err)

			w := &watcher1{
				t: t,
			}

			ctx, cancel := context.WithCancel(context.Background())
			watchReady := make(chan struct{})
			watchDone := make(chan struct{})
			go func() {
				err := b.Watch(testData[0].key, w,
					store.WithContext(ctx),
					store.WithNotifyCreated(func() {
						close(watchReady)
					}),
				)
				require.NoError(t, err)
				close(watchDone)
			}()
			<-watchReady

			entry := store.Entry{
				Key:   testData[0].key,
				Value: testData[0].value,
			}
			_, err = b.Put(&entry)
			require.NoError(t, err)

			_, err = b.Del(testData[0].key)
			require.NoError(t, err)

			cancel()
			<-watchDone
			assert.Equal(t, 31, w.CheckCount)
		}
	})

	t.Run("watch key with prefix", func(t *testing.T) {
		for _, p := range []string{"", "root"} {
			opts := []hash.Opt{}
			if p != "" {
				opts = append(opts, hash.WithPrefix(p))
			}

			b, err := hash.New(opts...)
			require.NoError(t, err)

			w := &watcher1{
				t: t,
			}

			ctx, cancel := context.WithCancel(context.Background())
			watchReady := make(chan struct{})
			watchDone := make(chan struct{})
			go func() {
				err := b.Watch(prefix, w,
					store.WithPrefix(),
					store.WithContext(ctx),
					store.WithNotifyCreated(func() {
						close(watchReady)
					}),
				)
				require.NoError(t, err)
				close(watchDone)
			}()
			<-watchReady

			entry := store.Entry{
				Key:   testData[0].key,
				Value: testData[0].value,
			}
			_, err = b.Put(&entry)
			require.NoError(t, err)

			_, err = b.Del(testData[0].key)
			require.NoError(t, err)

			cancel()
			<-watchDone
			assert.Equal(t, 31, w.CheckCount)
		}
	})

	t.Run("watch key with error channel", func(t *testing.T) {
		for _, p := range []string{"", "root"} {
			opts := []hash.Opt{}
			if p != "" {
				opts = append(opts, hash.WithPrefix(p))
			}

			b, err := hash.New(opts...)
			require.NoError(t, err)

			w := &watcher1{
				t: t,
			}

			ctx, cancel := context.WithCancel(context.Background())
			watchReady := make(chan struct{})
			errorChan := make(chan error, 1)
			watchDone := make(chan struct{})
			go func() {
				err := b.Watch(prefix, w,
					store.WithPrefix(),
					store.WithContext(ctx),
					store.WithNotifyCreated(func() {
						close(watchReady)
					}),
					store.WithErrorHandler(func(err error) error {
						errorChan <- err
						return nil
					}),
				)
				require.NoError(t, err)
				close(watchDone)
			}()
			<-watchReady

			testDone := make(chan struct{})
			go func() {
				assert.Equal(t, <-errorChan, errOnPut)
				close(testDone)
			}()

			entry := store.Entry{
				Key:   testData[0].key,
				Value: []byte("error"),
			}
			_, err = b.Put(&entry)
			require.NoError(t, err)
			<-testDone

			cancel()
			<-watchDone
			assert.Equal(t, 7, w.CheckCount)
		}
	})
}

type watcher2 struct {
	OnPutChan    chan struct{}
	OnDeleteChan chan struct{}
}

func (w *watcher2) BeforeWatch() error {
	return nil
}

func (w *watcher2) BeforeLoop() error {
	return nil
}

func (w *watcher2) OnDone() error {
	return nil
}

func (w *watcher2) OnPut(k, v []byte) error {
	w.OnPutChan <- struct{}{}
	return nil
}

func (w *watcher2) OnDelete(k, v []byte) error {
	w.OnDeleteChan <- struct{}{}
	return nil
}

func TestWatchLoad(t *testing.T) {
	n := 100

	t.Run("put and delete", func(t *testing.T) {
		for _, p := range []string{"", "root"} {
			opts := []hash.Opt{}
			if p != "" {
				opts = append(opts, hash.WithPrefix(p))
			}

			b, err := hash.New(opts...)
			require.NoError(t, err)

			w := &watcher2{
				OnPutChan:    make(chan struct{}, n),
				OnDeleteChan: make(chan struct{}, n),
			}

			ctx, cancel := context.WithCancel(context.Background())
			watchReady := make(chan struct{})
			watchDone := make(chan struct{})
			go func() {
				err := b.Watch(prefix, w,
					store.WithPrefix(),
					store.WithContext(ctx),
					store.WithNotifyCreated(func() {
						close(watchReady)
					}),
				)
				require.NoError(t, err)
				close(watchDone)
			}()
			<-watchReady

			// put
			for i := 0; i < n; i++ {
				entry := store.Entry{
					Key:   fmt.Sprintf("%s|key%d", prefix, i),
					Value: []byte(fmt.Sprintf("value%d", i)),
				}
				_, err := b.Put(&entry)
				require.NoError(t, err)
			}
			require.Equal(t, n, len(w.OnPutChan))

			// delete
			for i := 0; i < n; i++ {
				_, err := b.Del(fmt.Sprintf("%s|key%d", prefix, i))
				require.NoError(t, err)
			}
			require.Equal(t, n, len(w.OnDeleteChan))

			cancel()
			<-watchDone
		}
	})
}
