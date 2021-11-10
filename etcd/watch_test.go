package etcd

import (
	"context"
	"fmt"
	"reflect"
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

type A struct {
	key string
	op  string
}

func (a *A) SetKey(k string) {
	a.key = k
}

func (a *A) SetOp(o string) {
	a.op = o
}

var _ EventHandler = (*A)(nil)

func TestRene(t *testing.T) {
	c := make(chan *A)
	// b := make(chan<- A)

	ch, err := getChannel(c)
	require.NoError(t, err)

	tp, err := getChannelType(ch)
	require.NoError(t, err)
	item := reflect.New(tp).Interface()
	fmt.Println(item)
	return

	/*
		// fmt.Println(reflect.TypeOf(c).Elem().Kind())
		fmt.Println(reflect.TypeOf(b).Elem())
		dereferencedType := reflect.TypeOf(c).Elem().Elem()
		item := reflect.New(dereferencedType).Interface()
		eh, ok := item.(EventHandler)
		fmt.Println(ok)

		// ch := reflect.ValueOf(c)

		eh.SetOp("op")
		eh.SetKey("key")

		go func() {
			ch.Send(reflect.ValueOf(eh))
			ch.Close()
		}()

		for a := range c {
			fmt.Println(a)
		}
	*/
}

func getChannel(v interface{}) (reflect.Value, error) {
	ch := reflect.ValueOf(v)

	if ch.Kind() != reflect.Chan {
		return reflect.Value{}, errors.New("underlying type is not a channel")
	}

	if ch.Type().ChanDir() == reflect.RecvDir {
		return reflect.Value{}, errors.New("cannot send to channel")
	}

	return ch, nil
}

func getChannelType(v reflect.Value) (reflect.Type, error) {
	e := v.Type().Elem()
	if e.Kind() == reflect.Ptr {
		e = e.Elem()
	}

	if _, ok := reflect.New(e).Interface().(EventHandler); !ok {
		return nil, errors.New("channel type does not implement EventHandler type")
	}

	return e, nil
}
