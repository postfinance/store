package hash_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/postfinance/store"
	"github.com/postfinance/store/hash"
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

	t.Run("absolute key", func(t *testing.T) {
		h, err := hash.New(hash.WithPrefix(prefix))
		assert.NoError(t, err)
		assert.Equal(t, h.AbsKey(relkey), abskey)
		assert.NoError(t, h.Close())
	})

	t.Run("absolute key from key with leading separator", func(t *testing.T) {
		h, err := hash.New(hash.WithPrefix(prefix))
		assert.NoError(t, err)
		assert.NotEqual(t, h.AbsKey("/"+relkey), abskey)
		assert.NoError(t, h.Close())
	})

	t.Run("relative key", func(t *testing.T) {
		h, err := hash.New(hash.WithPrefix(prefix))
		assert.NoError(t, err)
		assert.Equal(t, h.RelKey(abskey), relkey)
		assert.NoError(t, h.Close())
	})

	t.Run("join key", func(t *testing.T) {
		h, err := hash.New(hash.WithPrefix(prefix))
		assert.NoError(t, err)
		assert.Equal(t, h.JoinKey(middle, suffix), relkey)
		assert.NoError(t, h.Close())
	})

	t.Run("split key", func(t *testing.T) {
		h, err := hash.New(hash.WithPrefix(prefix))
		assert.NoError(t, err)
		assert.Equal(t, h.SplitKey(relkey), []string{middle, suffix})
		assert.NoError(t, h.Close())
	})

	t.Run("split key", func(t *testing.T) {
		h, err := hash.New(hash.WithPrefix(prefix))
		assert.NoError(t, err)
		assert.Equal(t, h.KeyLeaf(relkey), suffix)
		assert.NoError(t, h.Close())
	})
}

// nolint: funlen
func TestGet(t *testing.T) {
	for _, p := range []string{"", "root"} {
		opts := []hash.Opt{}
		if p != "" {
			opts = append(opts, hash.WithPrefix(p))
		}

		b, err := hash.New(opts...)
		require.NoError(t, err)

		defer func() {
			assert.NoError(t, b.Close())
		}()

		t.Run("key not found", func(t *testing.T) {
			e, err := b.Get("key1")
			assert.Error(t, err)
			assert.Equal(t, store.ErrKeyNotFound, err)
			assert.Empty(t, e)
		})

		expEntries, err := createEntries(10, b)
		require.NoError(t, err)
		require.Len(t, expEntries, 10)

		t.Run("get without prefix", func(t *testing.T) {
			e, err := b.Get(expEntries[0].Key)
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
				entry.Value = []byte("a different value")
				return nil
			}
			_, err := b.Get(expEntries[0].Key, store.WithHandler(handler))
			require.NoError(t, err)
			expected := store.Entry{
				Key:   expEntries[0].Key,
				Value: []byte("a different value"),
			}
			assert.Equal(t, expected, entry)
		})

		t.Run("get with handler that returns error", func(t *testing.T) {
			handlerErr := errors.New("error")
			handler := func(k []byte, v []byte) error {
				return handlerErr
			}
			_, err := b.Get(expEntries[0].Key, store.WithHandler(handler))
			require.Equal(t, handlerErr, err)
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

		t.Run("get with context", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // doesn't matter, context will be ignored
			_, err := b.Get(expEntries[0].Key, store.WithContext(ctx))
			require.NoError(t, err)
		})
	}
}

// nolint: funlen
func TestPut(t *testing.T) {
	for _, p := range []string{"", "root"} {
		opts := []hash.Opt{}
		if p != "" {
			opts = append(opts, hash.WithPrefix(p))
		}

		b, err := hash.New(opts...)
		require.NoError(t, err)

		defer func() {
			assert.NoError(t, b.Close())
		}()

		entries, err := createEntries(10, nil)
		require.NoError(t, err)
		require.Len(t, entries, 10)

		t.Run("put", func(t *testing.T) {
			entry := &entries[0]
			ok, err := b.Put(entry)
			assert.NoError(t, err)
			assert.True(t, ok)
			r, err := b.Get(entry.Key)
			require.NoError(t, err)
			assert.Equal(t, entry.Value, r[0].Value)
		})

		t.Run("put with insert empty value", func(t *testing.T) {
			entry := &store.Entry{
				Key:   "emptyvalue",
				Value: []byte{},
			}
			ok, err := b.Put(entry, store.WithInsert())
			require.NoError(t, err)
			assert.True(t, ok)
			r, err := b.Get(entry.Key)
			require.NoError(t, err)
			assert.Equal(t, *entry, r[0])
			ok, err = b.Put(entry, store.WithInsert())
			require.NoError(t, err)
			assert.False(t, ok)
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
			r, err := b.Get(entry.Key)
			require.NoError(t, err)
			assert.Equal(t, entry.Value, r[0].Value)
			ok, err = b.Put(entry, store.WithInsert())
			assert.NoError(t, err)
			assert.False(t, ok)
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
			ok, err := b.Put(entry, store.WithTTL(10*time.Millisecond))
			assert.NoError(t, err)
			assert.True(t, ok)

			r, err := b.Get(entry.Key)
			require.NoError(t, err)
			assert.Equal(t, entry.Value, r[0].Value)

			time.Sleep(100 * time.Millisecond)

			r, err = b.Get(entry.Key)
			require.Error(t, store.ErrKeyNotFound, err)
			assert.Empty(t, r)
		})

		t.Run("put with context", func(t *testing.T) {
			entry := &entries[4]
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // doesn't matter, context will be ignored
			_, err := b.Put(entry, store.WithContext(ctx))
			require.NoError(t, err)
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
			cancel()
			assert.Equal(t, context.Canceled, <-ech)
			// entry still exists because default TTL is 30s
			r, err := b.Get(entry.Key)
			require.NoError(t, err)
			require.NotEmpty(t, r)
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
			r, err := b.Get(entry.Key)
			require.NoError(t, err)
			require.NotEmpty(t, r)
			// cancel the keep-alive
			cancel()
			assert.Equal(t, context.Canceled, <-ech)
			// after TTL is expired the entry should no longer exist
			time.Sleep(1500 * time.Millisecond)
			r, err = b.Get(entry.Key)
			require.Equal(t, store.ErrKeyNotFound, err)
			assert.Empty(t, r)
		})
	}
}

func TestDel(t *testing.T) {
	for _, p := range []string{"", "root"} {
		opts := []hash.Opt{}
		if p != "" {
			opts = append(opts, hash.WithPrefix(p))
		}

		b, err := hash.New(opts...)
		require.NoError(t, err)

		defer func() {
			assert.NoError(t, b.Close())
		}()

		expEntries, err := createEntries(10, b)
		require.NoError(t, err)
		require.Len(t, expEntries, 10)

		t.Run("del without prefix", func(t *testing.T) {
			num, err := b.Del(expEntries[0].Key)
			require.NoError(t, err)
			require.EqualValues(t, 1, num)
			r, err := b.Get("key", store.WithPrefix())
			require.NoError(t, err)
			require.Len(t, r, 9)
		})

		t.Run("del with context", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // doesn't matter, context will be ignored
			num, err := b.Del(expEntries[1].Key, store.WithContext(ctx))
			require.EqualValues(t, 1, num)
			require.NoError(t, err)
		})

		t.Run("del with prefix", func(t *testing.T) {
			num, err := b.Del("key", store.WithPrefix())
			require.NoError(t, err)
			require.EqualValues(t, 8, num)
			r, err := b.Get("key", store.WithPrefix())
			require.NoError(t, err)
			require.Len(t, r, 0)
		})
	}
}

func TestMarshal(t *testing.T) {
	for _, p := range []string{"", "root"} {
		opts := []hash.Opt{}
		if p != "" {
			opts = append(opts, hash.WithPrefix(p))
		}

		b, err := hash.New(opts...)
		require.NoError(t, err)

		defer func() {
			assert.NoError(t, b.Close())
		}()

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

func TestBackendWithTTL(t *testing.T) {
	for _, p := range []string{"", "root"} {
		opts := []hash.Opt{hash.WithTTL(10 * time.Millisecond)}
		if p != "" {
			opts = append(opts, hash.WithPrefix(p))
		}

		b, err := hash.New(opts...)
		require.NoError(t, err)

		defer func() {
			assert.NoError(t, b.Close())
		}()

		entries, err := createEntries(10, nil)
		require.NoError(t, err)
		require.Len(t, entries, 10)

		t.Run("put without ttl", func(t *testing.T) {
			entry := &entries[0]
			ok, err := b.Put(entry)
			assert.NoError(t, err)
			assert.True(t, ok)

			r, err := b.Get(entry.Key)
			require.NoError(t, err)
			assert.Equal(t, entry.Value, r[0].Value)

			time.Sleep(100 * time.Millisecond)

			r, err = b.Get(entry.Key)
			require.Equal(t, store.ErrKeyNotFound, err)
			assert.Empty(t, r)
		})

		t.Run("put with ttl", func(t *testing.T) {
			entry := &entries[0]
			ok, err := b.Put(entry, store.WithTTL(500*time.Millisecond))
			assert.NoError(t, err)
			assert.True(t, ok)

			r, err := b.Get(entry.Key)
			require.NoError(t, err)
			assert.Equal(t, entry.Value, r[0].Value)

			time.Sleep(100 * time.Millisecond)

			r, err = b.Get(entry.Key)
			require.NoError(t, err)
			assert.Len(t, r, 1)
		})
	}
}

// nolint: funlen, gocognit
func TestExpire(t *testing.T) {
	entries, err := createEntries(10, nil)
	require.NoError(t, err)
	require.Len(t, entries, 10)

	t.Run("get only unexpired keys", func(t *testing.T) {
		for _, p := range []string{"", "root"} {
			opts := []hash.Opt{}
			if p != "" {
				opts = append(opts, hash.WithPrefix(p))
			}

			b, err := hash.New(opts...)
			require.NoError(t, err)

			defer func() {
				assert.NoError(t, b.Close())
			}()

			for i := 0; i < 10; i++ {
				entry := &entries[i]
				if i%2 == 0 {
					ok, err := b.Put(entry)
					assert.NoError(t, err)
					assert.True(t, ok)
				} else {
					ok, err := b.Put(entry, store.WithTTL(10*time.Millisecond))
					assert.NoError(t, err)
					assert.True(t, ok)
				}
			}

			time.Sleep(100 * time.Millisecond)

			r, err := b.Get("key", store.WithPrefix())
			require.NoError(t, err)
			require.Len(t, r, 5)
		}
	})

	t.Run("insert expired entry", func(t *testing.T) {
		for _, p := range []string{"", "root"} {
			opts := []hash.Opt{}
			if p != "" {
				opts = append(opts, hash.WithPrefix(p))
			}

			b, err := hash.New(opts...)
			require.NoError(t, err)

			defer func() {
				assert.NoError(t, b.Close())
			}()

			entry := &entries[0]
			ok, err := b.Put(entry, store.WithInsert(), store.WithTTL(10*time.Millisecond))
			assert.NoError(t, err)
			assert.True(t, ok)

			ok, err = b.Put(entry, store.WithInsert(), store.WithTTL(10*time.Millisecond))
			assert.NoError(t, err)
			assert.False(t, ok)

			time.Sleep(100 * time.Millisecond)

			ok, err = b.Put(entry, store.WithInsert(), store.WithTTL(10*time.Millisecond))
			assert.NoError(t, err)
			assert.True(t, ok)
		}
	})

	t.Run("del an expired entry", func(t *testing.T) {
		for _, p := range []string{"", "root"} {
			opts := []hash.Opt{}
			if p != "" {
				opts = append(opts, hash.WithPrefix(p))
			}

			b, err := hash.New(opts...)
			require.NoError(t, err)

			defer func() {
				assert.NoError(t, b.Close())
			}()

			entry := &entries[0]
			ok, err := b.Put(entry, store.WithInsert(), store.WithTTL(10*time.Millisecond))
			assert.NoError(t, err)
			assert.True(t, ok)

			time.Sleep(100 * time.Millisecond)

			num, err := b.Del(entry.Key)
			require.NoError(t, err)
			require.EqualValues(t, 0, num)
		}
	})

	t.Run("del with prefix and count only unexpired entries", func(t *testing.T) {
		for _, p := range []string{"", "root"} {
			opts := []hash.Opt{}
			if p != "" {
				opts = append(opts, hash.WithPrefix(p))
			}

			b, err := hash.New(opts...)
			require.NoError(t, err)

			defer func() {
				assert.NoError(t, b.Close())
			}()

			for i := 0; i < 10; i++ {
				entry := &entries[i]
				if i%2 == 0 {
					ok, err := b.Put(entry)
					assert.NoError(t, err)
					assert.True(t, ok)
				} else {
					ok, err := b.Put(entry, store.WithTTL(10*time.Millisecond))
					assert.NoError(t, err)
					assert.True(t, ok)
				}
			}

			time.Sleep(100 * time.Millisecond)

			num, err := b.Del("key", store.WithPrefix())
			require.NoError(t, err)
			require.EqualValues(t, 5, num)
		}
	})
}

// nolint: unparam // `max` always receives `10` (unparam)
func createEntries(max int, b store.Backend) ([]store.Entry, error) {
	expEntries := []store.Entry{}

	for i := 0; i < max; i++ {
		entry := store.Entry{
			Key:   fmt.Sprintf("key%d", i),
			Value: []byte(fmt.Sprintf(`{"key%d": "val%d"}`, i, i)),
		}

		if b != nil {
			if _, err := b.Put(&entry); err != nil {
				return nil, err
			}
		}

		expEntries = append(expEntries, entry)
	}

	return expEntries, nil
}
