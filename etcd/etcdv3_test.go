package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/integration"
	"github.com/coreos/pkg/capnslog"
	"github.com/pkg/errors"
	"github.com/postfinance/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyFunctions(t *testing.T) {
	const (
		prefix = "root"
		middle = "branch"
		suffix = "leaf"
		relkey = middle + "/" + suffix
		abskey = prefix + "/" + relkey
	)

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	cli := cluster.RandClient()

	defer cluster.Terminate(t)

	t.Run("absolute key", func(t *testing.T) {
		b, err := New(WithClient(cli), WithPrefix(prefix))
		assert.NoError(t, err)
		assert.Equal(t, b.AbsKey(relkey), abskey)
	})

	t.Run("absolute key from key with leading separator", func(t *testing.T) {
		b, err := New(WithClient(cli), WithPrefix(prefix))
		assert.NoError(t, err)
		assert.NotEqual(t, b.AbsKey("/"+relkey), abskey)
	})

	t.Run("relative key", func(t *testing.T) {
		b, err := New(WithClient(cli), WithPrefix(prefix))
		assert.NoError(t, err)
		assert.Equal(t, b.RelKey(abskey), relkey)
	})

	t.Run("join key", func(t *testing.T) {
		b, err := New(WithClient(cli), WithPrefix(prefix))
		assert.NoError(t, err)
		assert.Equal(t, b.JoinKey(middle, suffix), relkey)
	})

	t.Run("split key", func(t *testing.T) {
		b, err := New(WithClient(cli), WithPrefix(prefix))
		assert.NoError(t, err)
		assert.Equal(t, b.SplitKey(relkey), []string{middle, suffix})
	})

	t.Run("split key", func(t *testing.T) {
		b, err := New(WithClient(cli), WithPrefix(prefix))
		assert.NoError(t, err)
		assert.Equal(t, b.KeyLeaf(relkey), suffix)
	})
}

// nolint: funlen
func TestGet(t *testing.T) {
	for _, p := range []string{"", "root"} {
		opts := []Opt{}
		if p != "" {
			opts = append(opts, WithPrefix(p))
		}

		b, cli, teardown := setupTestStore(t, false, opts)
		defer teardown()

		t.Run("key not found", func(t *testing.T) {
			e, err := b.Get("key1")
			require.Error(t, err)
			assert.Equal(t, store.ErrKeyNotFound, err)
			assert.Empty(t, e)
		})

		expEntries, err := createEntries(10, b.(*Backend), cli)
		require.NoError(t, err)
		require.Len(t, expEntries, 10)

		t.Run("get without prefix", func(t *testing.T) {
			e, err := b.Get("key0")
			require.NoError(t, err)
			require.Len(t, e, 1)
			assert.Equal(t, expEntries[0], e[0])
		})

		t.Run("get with prefix", func(t *testing.T) {
			e, err := b.Get("key", store.WithPrefix())
			require.NoError(t, err)
			assert.Equal(t, expEntries, e)
		})

		t.Run("get with handler", func(t *testing.T) {
			entry := store.Entry{}
			handler := func(k []byte, v []byte) error {
				entry.Key = string(k)
				entry.Value = v
				return nil
			}
			_, err := b.Get("key0", store.WithHandler(handler))
			require.NoError(t, err)
			expected := store.Entry{
				Key:   "key0",
				Value: []byte(`{"key0": "val0"}`),
			}
			assert.Equal(t, expected, entry)
		})

		t.Run("get with Unmarshal", func(t *testing.T) {
			actual := make(map[string]string)
			expected := map[string]string{
				"key0": "val0",
			}
			_, err := b.Get("key0", store.WithUnmarshal(&actual))
			require.NoError(t, err)
			assert.Equal(t, expected, actual)
		})

		t.Run("get with Unmarshal and prefix", func(t *testing.T) {
			actual := []map[string]string{}
			expected := []map[string]string{
				{
					"key0": "val0",
				},
				{
					"key1": "val1",
				},
				{
					"key2": "val2",
				},
				{
					"key3": "val3",
				},
				{
					"key4": "val4",
				},
				{
					"key5": "val5",
				},
				{
					"key6": "val6",
				},
				{
					"key7": "val7",
				},
				{
					"key8": "val8",
				},
				{
					"key9": "val9",
				},
			}

			_, err := b.Get("key", store.WithUnmarshal(&actual), store.WithPrefix())
			require.NoError(t, err)
			assert.Equal(t, expected, actual)
		})

		t.Run("get with handler that returns error", func(t *testing.T) {
			handlerErr := errors.New("error")
			handler := func(k []byte, v []byte) error {
				return handlerErr
			}
			_, err := b.Get("key0", store.WithHandler(handler))
			require.Equal(t, handlerErr, err)
		})

		t.Run("get with timeout", func(t *testing.T) {
			e, _ := b.(*Backend)
			e.RequestTimeout = 1
			defer func(to time.Duration) { e.RequestTimeout = to }(0)
			_, err := b.Get("key0")
			require.Equal(t, context.DeadlineExceeded, err)
		})

		t.Run("get with context", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := b.Get("key0", store.WithContext(ctx))
			require.Equal(t, err, context.Canceled)
		})
	}
}

// nolint: funlen
func TestPut(t *testing.T) {
	for _, p := range []string{"", "root"} {
		opts := []Opt{}
		if p != "" {
			opts = append(opts, WithPrefix(p))
		}

		b, cli, teardown := setupTestStore(t, false, opts)
		defer teardown()

		entries, err := createEntries(10, nil, nil)
		require.NoError(t, err)
		require.Len(t, entries, 10)

		t.Run("put", func(t *testing.T) {
			entry := &entries[0]
			ok, err := b.Put(entry)
			assert.NoError(t, err)
			assert.True(t, ok)
			r, err := cli.Get(context.Background(), b.(*Backend).AbsKey(entry.Key))
			require.NoError(t, err)
			require.NotEmpty(t, r.Kvs)
			assert.Equal(t, entry.Value, r.Kvs[0].Value)
		})

		t.Run("put again", func(t *testing.T) {
			entry := &entries[0]
			ok, err := b.Put(entry)
			assert.NoError(t, err)
			assert.False(t, ok)
		})

		t.Run("put again with different value", func(t *testing.T) {
			entry := &entries[0]
			entry.Value = []byte("new")
			ok, err := b.Put(entry)
			assert.NoError(t, err)
			assert.True(t, ok)
		})

		t.Run("put with insert", func(t *testing.T) {
			entry := &entries[1]
			ok, err := b.Put(entry, store.WithInsert())
			assert.NoError(t, err)
			assert.True(t, ok)
			r, err := cli.Get(context.Background(), b.(*Backend).AbsKey(entry.Key))
			require.NoError(t, err)
			require.NotEmpty(t, r.Kvs)
			assert.Equal(t, entry.Value, r.Kvs[0].Value)
		})

		t.Run("put with insert again", func(t *testing.T) {
			entry := &entries[1]
			ok, err := b.Put(entry, store.WithInsert())
			assert.NoError(t, err)
			assert.False(t, ok)
		})

		t.Run("put again with insert and different value", func(t *testing.T) {
			entry := &entries[1]
			entry.Value = []byte("new")
			ok, err := b.Put(entry, store.WithInsert())
			assert.NoError(t, err)
			assert.False(t, ok)
		})

		t.Run("put with ttl", func(t *testing.T) {
			entry := &entries[2]
			ok, err := b.Put(entry, store.WithTTL(1*time.Second))
			require.NoError(t, err)
			require.True(t, ok)
			r, err := cli.Get(context.Background(), b.(*Backend).AbsKey(entry.Key))
			require.NoError(t, err)
			require.NotEmpty(t, r.Kvs)
			require.Equal(t, entry.Value, r.Kvs[0].Value)
			time.Sleep(1500 * time.Millisecond)
			r, err = cli.Get(context.Background(), b.(*Backend).AbsKey(entry.Key))
			require.NoError(t, err)
			assert.Empty(t, r.Kvs)
		})

		t.Run("put with timeout", func(t *testing.T) {
			entry := &entries[3]
			e, _ := b.(*Backend)
			e.RequestTimeout = 1
			defer func(to time.Duration) { e.RequestTimeout = to }(0)
			_, err := b.Put(entry)
			assert.Equal(t, context.DeadlineExceeded, err)
		})

		t.Run("put with context", func(t *testing.T) {
			entry := &entries[4]
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := b.Put(entry, store.WithContext(ctx))
			assert.Equal(t, err, context.Canceled)
		})

		t.Run("put with keep-alive and cancel", func(t *testing.T) {
			entry := &entries[5]
			ech := make(chan error)
			ctx, cancel := context.WithCancel(context.Background())
			ok, err := b.Put(entry,
				store.WithContext(ctx),
				store.WithKeepAlive(ech),
			)
			require.NoError(t, err)
			require.True(t, ok)
			cancel()
			e := <-ech
			// can't predict which error is first read by select
			assert.True(t, e == context.Canceled || e == store.ErrResponseChannelClosed)
			// entry still exists because default TTL is 30s
			r, err := cli.Get(context.Background(), b.(*Backend).AbsKey(entry.Key))
			require.NoError(t, err)
			require.NotEmpty(t, r.Kvs)
		})

		t.Run("put with keep-alive and wait", func(t *testing.T) {
			entry := &entries[6]
			ech := make(chan error)
			ctx, cancel := context.WithCancel(context.Background())
			ok, err := b.Put(entry,
				store.WithContext(ctx),
				store.WithTTL(1*time.Second),
				store.WithKeepAlive(ech),
			)
			require.NoError(t, err)
			require.True(t, ok)
			// after TTL is expired the entry should still exist
			time.Sleep(1500 * time.Millisecond)
			r, err := cli.Get(context.Background(), b.(*Backend).AbsKey(entry.Key))
			require.NoError(t, err)
			require.NotEmpty(t, r.Kvs)
			// cancel the keep-alive
			cancel()
			e := <-ech
			// can't predict which error is first read by select
			assert.True(t, e == context.Canceled || e == store.ErrResponseChannelClosed)
			// after TTL is expired the entry should no longer exist
			time.Sleep(1500 * time.Millisecond)
			r, err = cli.Get(context.Background(), b.(*Backend).AbsKey(entry.Key))
			require.NoError(t, err)
			assert.Empty(t, r.Kvs)
		})
	}
}

func TestDel(t *testing.T) {
	for _, p := range []string{"", "root"} {
		opts := []Opt{}
		if p != "" {
			opts = append(opts, WithPrefix(p))
		}

		b, cli, teardown := setupTestStore(t, false, opts)
		defer teardown()

		expEntries, err := createEntries(10, b.(*Backend), cli)
		require.NoError(t, err)
		require.Len(t, expEntries, 10)

		t.Run("del without prefix", func(t *testing.T) {
			e := &store.Entry{
				Key:   "xyz",
				Value: []byte("new"),
			}
			// key xyz does not exist yet
			num, err := b.Del(e.Key)
			require.NoError(t, err)
			assert.Equal(t, int64(0), num)
			_, err = b.Put(e)
			require.NoError(t, err)
			num, err = b.Del(e.Key)
			require.NoError(t, err)
			assert.Equal(t, num, int64(1))
		})
		t.Run("del without prefix", func(t *testing.T) {
			num, err := b.Del("key0")
			require.NoError(t, err)
			require.EqualValues(t, 1, num)
			r, err := cli.Get(context.Background(), b.(*Backend).AbsKey("key"), clientv3.WithPrefix())
			require.NoError(t, err)
			require.Len(t, r.Kvs, 9)
		})

		t.Run("del context", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := b.Del("key1", store.WithContext(ctx))
			require.Equal(t, err, context.Canceled)
		})

		t.Run("del with timeout", func(t *testing.T) {
			e, _ := b.(*Backend)
			e.RequestTimeout = 1
			defer func(to time.Duration) { e.RequestTimeout = to }(0)
			_, err := b.Del("key1")
			require.Equal(t, context.DeadlineExceeded, err)
		})

		t.Run("del with prefix", func(t *testing.T) {
			num, err := b.Del("key", store.WithPrefix())
			require.NoError(t, err)
			require.EqualValues(t, 9, num)
			r, err := cli.Get(context.Background(), "key", clientv3.WithPrefix())
			require.NoError(t, err)
			require.Len(t, r.Kvs, 0)
		})
	}
}

func TestMarshal(t *testing.T) {
	for _, p := range []string{"", "root"} {
		opts := []Opt{}
		if p != "" {
			opts = append(opts, WithPrefix(p))
		}

		b, _, teardown := setupTestStore(t, false, opts)
		defer teardown()

		type testStruct struct {
			Value1 string
			Value2 int
		}

		exp := testStruct{
			Value1: "value1",
			Value2: 42,
		}
		key := "key"
		value, err := json.Marshal(exp)
		require.NoError(t, err)
		_, err = b.Put(&store.Entry{Key: key, Value: value})
		require.NoError(t, err)

		actual := testStruct{}
		_, err = b.Get(key, store.WithHandler(func(k, v []byte) error {
			return json.Unmarshal(v, &actual)
		}))
		require.NoError(t, err)
		assert.EqualValues(t, exp, actual)
	}
}

func TestMarshal2(t *testing.T) {
	for _, p := range []string{"", "root"} {
		opts := []Opt{}
		if p != "" {
			opts = append(opts, WithPrefix(p))
		}

		b, _, teardown := setupTestStore(t, false, opts)
		defer teardown()

		type testStruct struct {
			Value1 string
			Value2 int
		}

		exp := testStruct{
			Value1: "value1",
			Value2: 42,
		}
		key := "key"
		value, err := json.Marshal(exp)
		require.NoError(t, err)
		_, err = b.Put(&store.Entry{Key: key, Value: value})
		require.NoError(t, err)

		bla := []testStruct{}
		err = store.UnmarshalJSONList(&bla, key, b)
		require.NoError(t, err)
	}
}

/*
SetupTestStore starts an embedded etcd server.
The returned function shall be used as teardown function.

If log is true, the integrated server logs output to stdout.

This can be used in integration tests as follows:

	func TestSomething(m *testing.M) {
		b, s, teardown := SetupTestStore(t, false. []Opt{})
		defer teardown()
	}
*/
// nolint: unparam // `log` always receives `false` (unparam)
func setupTestStore(t *testing.T, log bool, opts []Opt) (store.Backend, *clientv3.Client, func()) {
	if !log {
		capnslog.SetGlobalLogLevel(capnslog.ERROR)
	}

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})

	cli := cluster.RandClient()

	backend, err := New(append([]Opt{WithClient(cli)}, opts...)...)
	if err != nil {
		panic(errors.Wrap(err, "OK"))
	}

	return backend, cli, func() { cluster.Terminate(t) }
}

func createEntries(max int, b *Backend, cli *clientv3.Client) ([]store.Entry, error) {
	expEntries := []store.Entry{}

	for i := 0; i < max; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf(`{"key%d": "val%d"}`, i, i)

		if cli != nil {
			if _, err := cli.Put(context.Background(), b.AbsKey(key), val); err != nil {
				return nil, err
			}
		}

		expEntries = append(expEntries, store.Entry{
			Key:   key,
			Value: []byte(val),
		})
	}

	return expEntries, nil
}
