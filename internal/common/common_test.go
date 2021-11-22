package common

import (
	"errors"
	"strings"
	"testing"

	"github.com/postfinance/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type Item1 struct {
	store.EventMeta
	Value string
}

// FromKey sets values from splitted key.
func (i *Item1) MarshalKey(key []string) error {
	if len(key) < 1 {
		return errors.New("not enough elements in key")
	}

	i.Value = key[0]

	return nil
}

type Item2 struct {
	Value string
	*store.EventMeta
}

type Item3 struct {
	Value string
	key   string
	op    store.Operation
}

func (i *Item3) SetKey(key string) {
	i.key = key
}

func (i *Item3) SetOp(op store.Operation) {
	i.op = op
}

const (
	dataJSON = `{"Value":"value1"}`
	dataYAML = `value: value1`
)

var (
	_ store.KeyOpSetter   = (*Item1)(nil)
	_ store.KeyMarshaller = (*Item1)(nil)
	_ store.KeyOpSetter   = Item2{}
	_ store.KeyOpSetter   = (*Item2)(nil)
	_ store.KeyOpSetter   = (*Item3)(nil)
)

func TestChannel(t *testing.T) {
	t.Run("channel is pointer type with embedded store.EventMeta", func(t *testing.T) {
		channel := make(chan *Item1, 1)
		w, err := NewChannelSender(channel, nil, nil)
		require.NoError(t, err)
		err = w.Send("key", store.Update, []byte(dataJSON))
		require.NoError(t, err)
		assert.Equal(t, &Item1{Value: "value1", EventMeta: *newMeta("key", store.Update)}, <-channel)
	})

	t.Run("channel is pointer type with embedded *store.EventMeta", func(t *testing.T) {
		channel := make(chan *Item2, 1)
		w, err := NewChannelSender(channel, nil, nil)
		require.NoError(t, err)
		err = w.Send("key", store.Update, []byte(dataJSON))
		require.NoError(t, err)
		assert.Equal(t, &Item2{Value: "value1", EventMeta: newMeta("key", store.Update)}, <-channel)
	})

	t.Run("channel is not pointer type with embedded *store.EventMeta", func(t *testing.T) {
		channel := make(chan Item2, 1)
		w, err := NewChannelSender(channel, nil, nil)
		require.NoError(t, err)
		err = w.Send("key", store.Update, []byte(dataJSON))
		require.NoError(t, err)
		assert.Equal(t, Item2{Value: "value1", EventMeta: newMeta("key", store.Update)}, <-channel)
	})

	t.Run("channel is pointer type with which implements store.KeyOpSetter natively", func(t *testing.T) {
		channel := make(chan *Item3, 1)
		w, err := NewChannelSender(channel, nil, nil)
		require.NoError(t, err)
		err = w.Send("key", store.Update, []byte(dataJSON))
		require.NoError(t, err)
		assert.Equal(t, &Item3{Value: "value1", key: "key", op: store.Update}, <-channel)
	})

	t.Run("custom unmarshal func", func(t *testing.T) {
		channel := make(chan *Item1, 1)
		w, err := NewChannelSender(channel, yaml.Unmarshal, nil)
		require.NoError(t, err)
		err = w.Send("key", store.Update, []byte(dataYAML))
		require.NoError(t, err)
		assert.Equal(t, &Item1{Value: "value1", EventMeta: *newMeta("key", store.Update)}, <-channel)
	})

	t.Run("channel is pointer type with embedded store.EventMeta and implements store.FromKeyer", func(t *testing.T) {
		channel := make(chan *Item1, 1)
		w, err := NewChannelSender(channel, nil, func(key string) []string { return strings.Split(key, ",") })
		require.NoError(t, err)
		err = w.Send("key", store.Delete, []byte{})
		require.NoError(t, err)
		assert.Equal(t, &Item1{Value: "key", EventMeta: *newMeta("key", store.Delete)}, <-channel)
	})
}

func newMeta(key string, op store.Operation) *store.EventMeta {
	m := &store.EventMeta{}
	m.SetKey(key)
	m.SetOp(op)

	return m
}
