package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/postfinance/store"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	OK = "OK"
)

func TestGetOptions(t *testing.T) {
	t.Run("WithPrefix", func(t *testing.T) {
		opts := &store.GetOptions{}
		store.WithPrefix().SetGetOption(opts)
		assert.True(t, opts.Prefix)
	})

	t.Run("WithHandler", func(t *testing.T) {
		var exp, act string
		opts := &store.GetOptions{}
		exp = OK
		handler := func(k, v []byte) error {
			act = exp
			return nil
		}
		store.WithHandler(handler).SetGetOption(opts)
		_ = opts.Handler([]byte{}, []byte{})
		assert.Equal(t, exp, act)
	})

	t.Run("WithContext", func(t *testing.T) {
		opts := &store.GetOptions{}
		store.WithContext(context.Background()).SetGetOption(opts)
		assert.NotNil(t, opts.Context)
	})

	t.Run("WithUnmarshal no pointer", func(t *testing.T) {
		v := map[string]interface{}{}
		opts := &store.GetOptions{}
		assert.Panics(t, func() { store.WithUnmarshal(v).SetGetOption(opts) })
	})

	t.Run("WithUnmarshal value", func(t *testing.T) {
		v := map[string]interface{}{}
		opts := &store.GetOptions{}
		store.WithUnmarshal(&v).SetGetOption(opts)
		require.NotNil(t, opts.Unmarshal)
		assert.Equal(t, &v, opts.Unmarshal.Input)
		assert.False(t, opts.Unmarshal.IsSlice())
		assert.Nil(t, opts.Unmarshal.Element())
	})

	t.Run("WithUnmarshal slice", func(t *testing.T) {
		v := []map[string]interface{}{}
		opts := &store.GetOptions{}
		store.WithUnmarshal(&v).SetGetOption(opts)
		require.NotNil(t, opts.Unmarshal)
		assert.Equal(t, &v, opts.Unmarshal.Input)
		assert.True(t, opts.Unmarshal.IsSlice())
		assert.NotNil(t, opts.Unmarshal.Element())
	})
}

func TestPutOptions(t *testing.T) {
	t.Run("WithInsert", func(t *testing.T) {
		opts := &store.PutOptions{}
		store.WithInsert().SetPutOption(opts)
		assert.True(t, opts.Insert)
	})
	t.Run("WithTTL", func(t *testing.T) {
		opts := &store.PutOptions{}
		store.WithTTL(1).SetPutOption(opts)
		assert.EqualValues(t, opts.TTL, 1)
	})
	t.Run("WithContext", func(t *testing.T) {
		opts := &store.PutOptions{}
		store.WithContext(context.Background()).SetPutOption(opts)
		assert.NotNil(t, opts.Context)
	})
}

func TestDelOptions(t *testing.T) {
	t.Run("WithPrefix", func(t *testing.T) {
		opts := &store.DelOptions{}
		store.WithPrefix().SetDelOption(opts)
		assert.True(t, opts.Prefix)
	})
	t.Run("WithContext", func(t *testing.T) {
		opts := &store.DelOptions{}
		store.WithContext(context.Background()).SetDelOption(opts)
		assert.NotNil(t, opts.Context)
	})
}

func TestWatchOptions(t *testing.T) {
	t.Run("WithPrefix", func(t *testing.T) {
		opts := &store.WatchOptions{}
		store.WithPrefix().SetWatchOption(opts)
		assert.True(t, opts.Prefix)
	})

	t.Run("WithContext", func(t *testing.T) {
		opts := &store.WatchOptions{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store.WithContext(ctx).SetWatchOption(opts)
		assert.Equal(t, ctx, opts.Context)
	})

	t.Run("WithNotifyCreated", func(t *testing.T) {
		var exp, act string
		opts := &store.WatchOptions{}
		exp = OK
		handler := func() {
			act = exp
		}
		store.WithNotifyCreated(handler).SetWatchOption(opts)
		opts.NotifyCreated()
		assert.Equal(t, exp, act)
	})

	t.Run("WithErrorHandler", func(t *testing.T) {
		var exp, act string
		opts := &store.WatchOptions{}
		exp = OK
		handler := func(err error) error {
			act = err.Error()
			return err
		}
		store.WithErrorHandler(handler).SetWatchOption(opts)
		_ = opts.ErrorHandler(errors.New(exp))
		assert.Equal(t, exp, act)
	})
}

func TestPutWraper(t *testing.T) {
	type testStruct struct {
		A string
		B string
	}

	v := testStruct{"a", "b"}
	b := &mockBackend{}
	_, err := store.Put(b, "key", v)
	require.NoError(t, err)
	require.Len(t, b.entries, 1)

	expected, err := json.Marshal(v)
	require.NoError(t, err)
	assert.Equal(t, expected, b.entries[0].Value)
}

type mockBackend struct {
	entries []store.Entry
}

func (m *mockBackend) Get(key string, ops ...store.GetOption) ([]store.Entry, error) {
	opts := &store.GetOptions{}

	for _, op := range ops {
		op.SetGetOption(opts)
	}

	if opts.Handler == nil {
		opts.Handler = func([]byte, []byte) error {
			return nil
		}
	}

	for _, e := range m.entries {
		if err := opts.Handler([]byte(e.Key), e.Value); err != nil {
			return m.entries, err
		}
	}

	return m.entries, nil
}

func (*mockBackend) Del(string, ...store.DelOption) (int64, error) {
	panic("not implemented")
}

func (*mockBackend) Watch(string, store.Watcher, ...store.WatchOption) error {
	panic("not implemented")
}

func (m *mockBackend) Put(e *store.Entry, opst ...store.PutOption) (bool, error) {
	m.entries = append(m.entries, *e)
	return true, nil
}

func (*mockBackend) Close() error {
	return nil
}
