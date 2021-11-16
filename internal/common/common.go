// Package common contains helper methods used by all store implementations.
package common

import (
	"encoding/json"
	"errors"
	"reflect"

	"github.com/postfinance/store"
)

// NewChannelSender creates a ChannelSender. The passed channel type must implmement
// the store.KeyOpSetter interface. Additionally, if the channel is a receive-only
// channel, an error is returned.
func NewChannelSender(channel interface{}, unmarshal UnmarshalFunc) (*ChannelSender, error) {
	if channel == nil {
		return nil, errors.New("channel is nil")
	}

	ch := reflect.ValueOf(channel)

	if ch.Kind() != reflect.Chan {
		return nil, errors.New("underlying type is not a channel")
	}

	if ch.Type().ChanDir() == reflect.RecvDir {
		return nil, errors.New("cannot send to channel")
	}

	e := ch.Type().Elem()

	if e.Kind() != reflect.Ptr && e.Kind() != reflect.Struct || e.Kind() == reflect.Ptr && e.Elem().Kind() != reflect.Struct {
		return nil, errors.New("channel type is not a struct or a pointer to a struct")
	}

	if _, ok := reflect.Indirect(reflect.New(e)).Interface().(store.KeyOpSetter); !ok {
		return nil, errors.New("channel type does not implement store.KeyOpSetter interface")
	}

	u := json.Unmarshal
	if unmarshal != nil {
		u = unmarshal
	}

	return &ChannelSender{
		channel:   ch,
		create:    creator(e),
		unmarshal: u,
	}, nil
}

// ChannelSender sends data to a channel. See Write method for more details.
type ChannelSender struct {
	channel   reflect.Value
	create    creatorFunc
	unmarshal UnmarshalFunc
}

// UnmarshalFunc is a function that can unmarshal data to an object v.
type UnmarshalFunc func(data []byte, v interface{}) error

// Send creates a new instance of the channel's type (which must implement store.KeyOpSetter
// interface see NewChannelSender) and applies SetKey and SetOp method. If data is not empty, it
// it is unmarshaled into the newly created instance and sends the result on the channel.
func (w *ChannelSender) Send(key string, op store.Operation, data []byte) error {
	item := w.create().Interface().(store.KeyOpSetter)

	item.SetKey(key)
	item.SetOp(op)

	if len(data) > 0 {
		if err := w.unmarshal(data, item); err != nil {
			return err
		}
	}

	value := reflect.ValueOf(item) // this is the pointer
	if w.channel.Type().Elem().Kind() != reflect.Ptr {
		value = reflect.Indirect(value)
	}

	w.channel.Send(value)

	return nil
}

// creator returns a function similar to reflect.New with the difference,
// that it also initializes the pointer to first embedded struct implementing
// store.KeyOpSetter interface.
func creator(ift reflect.Type) creatorFunc {
	if ift.Kind() == reflect.Ptr {
		ift = ift.Elem()
	}

	keyOpSetter := reflect.TypeOf(new(store.KeyOpSetter)).Elem()

	var (
		index           = -1
		keyOpSetterType reflect.Type
	)

	// no check necessary that ift is a struct. this is performed above.
	for i := 0; i < ift.NumField(); i++ {
		v := ift.Field(i)

		if v.Type.Implements(keyOpSetter) {
			index = i
			keyOpSetterType = v.Type

			break
		}
	}

	// either no store.KeyOpSetter found or corresponding struct is not a pointer.
	if index == -1 || keyOpSetterType.Kind() != reflect.Ptr {
		return func() reflect.Value {
			return reflect.New(ift)
		}
	}

	return func() reflect.Value {
		v := reflect.Indirect(reflect.New(ift))
		v.Field(index).Set(reflect.New(keyOpSetterType.Elem()))

		return v.Addr()
	}
}

type creatorFunc func() reflect.Value
