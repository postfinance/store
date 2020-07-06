// Package store provides the interface for a key-value store with different backends
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"
)

var (
	// ErrKeyNotFound is returned when key was not found.
	ErrKeyNotFound = errors.New("key not found")
	// ErrResponseChannelClosed will be returned if the response channel of the keep-alive is closed
	ErrResponseChannelClosed = fmt.Errorf("keepalive response channel has been closed")
)

// HandlerFunc is a function that is called on (each) returned
// key-value pair during a Get request.
type HandlerFunc func([]byte, []byte) error

// ErrorFunc is a function called on (each) error not returned
type ErrorFunc func(error) error

// NotifyCallback TODO(sauterm): describe
// Name: NotifyFunc?
type NotifyCallback func()

// Backend is the interface required for a key value store.
type Backend interface {
	// Put is used to insert or update an entry.
	//
	// The entry is added if the key exists or not.
	// If value is changed, true is returned, false is returned only if
	// value stays unchanged.
	//
	// If WithInsert option is used and the key already
	// exists, nothing is done and false is returned. If key does not exist
	// the entry is added and true is returned.
	Put(*Entry, ...PutOption) (bool, error)

	// Get is used to fetch an entry. If key is not found and WithPrefix is absent ErrKeyNotFound is
	// returned.
	Get(string, ...GetOption) ([]Entry, error)

	// Delete is used to permanently delte an entry. The number of deleted keys will be returned.
	Del(string, ...DelOption) (int64, error)

	// Watch a key
	Watch(string, Watcher, ...WatchOption) error

	// Close closes the connection.
	Close() error
}

// Watcher interface
type Watcher interface {
	BeforeWatch() error
	BeforeLoop() error
	OnDone() error
	OnPut([]byte, []byte) error
	OnDelete([]byte, []byte) error
}

// BackendKeyer interface extends Backend with key handling
type BackendKeyer interface {
	Backend
	RelKey(k string) string
	AbsKey(k string) string
	JoinKey(args ...string) string
	SplitKey(key string) []string
	KeyLeaf(key string) string
}

// Entry is used to represent data stored by the physical backend
type Entry struct {
	Key   string
	Value []byte
}

// Put is a wrapper around the Backend interface's Put method. This wrapper
// JSON marhals the interface v and uses the generated byte array as value.
func Put(b Backend, key string, v interface{}, opts ...PutOption) (bool, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return false, err
	}

	return b.Put(&Entry{
		Key:   key,
		Value: data,
	}, opts...)
}

// GetOptions represent all possible options for Get requests.
type GetOptions struct {
	Prefix    bool
	Handler   HandlerFunc
	Context   context.Context
	Unmarshal *unmarshal
}

// PutOptions represent all possible options for Put requests.
type PutOptions struct {
	Context context.Context
	TTL     time.Duration
	ErrChan chan<- error
	Insert  bool
}

// DelOptions represent all possible options for Del requests.
type DelOptions struct {
	Prefix  bool
	Context context.Context
}

// WatchOptions represent all possible options for watchers.
type WatchOptions struct {
	Prefix        bool
	Context       context.Context
	NotifyCreated NotifyCallback
	ErrorHandler  ErrorFunc
}

// WithPrefix is an option to perform a request with prefix.
func WithPrefix() interface {
	GetOption
	DelOption
	WatchOption
} {
	return &prefixOption{}
}

// WithContext is an options to set a context for a request.
func WithContext(ctx context.Context) interface {
	GetOption
	PutOption
	DelOption
	WatchOption
} {
	return &contextOption{Context: ctx}
}

// WithHandler is an option to use an HandlerFunc on each
// key-value pair during a Get request.
func WithHandler(h HandlerFunc) interface {
	GetOption
} {
	return &handlerOption{Handler: h}
}

// WithNotifyCreated is an option to use a NotifyCallback
// as soon as the Watch is established and ready to receive events
func WithNotifyCreated(c NotifyCallback) interface {
	WatchOption
} {
	return &notifyCreatedOption{Callback: c}
}

// WithErrorHandler is an option to use an ErrorFunc on each
// error not returned
func WithErrorHandler(h ErrorFunc) interface {
	WatchOption
} {
	return &errorHandlerOption{ErrorHandler: h}
}

// WithTTL is an option to add a time to live.
func WithTTL(ttl time.Duration) interface {
	PutOption
} {
	return &ttlOption{TTL: ttl}
}

// WithKeepAlive is an option to start a keep-alive for a key.
// The keep-alive will only start if errChan != nil
func WithKeepAlive(errChan chan<- error) interface {
	PutOption
} {
	return &keepAliveOption{ErrChan: errChan}
}

// WithUnmarshal unmarshals the byte array in the store
// into v. It panics if v is not a pointer .
//
// In combination with WithPrefix, v should be a pointer to
// a slice.
func WithUnmarshal(v interface{}) interface {
	GetOption
} {
	return &unmarshalOption{Unmarshal: v}
}

// WithInsert is an option to put a non existing key. If the key already
// exists, nothing is done and false is returned. If key does not exist
// key and value are added and true is returned.
func WithInsert() interface {
	PutOption
} {
	return &insertOption{}
}

// GetOption is the option interface for Get requests.
type GetOption interface {
	SetGetOption(*GetOptions)
}

// PutOption is the option interface for Put requests.
type PutOption interface {
	SetPutOption(*PutOptions)
}

// DelOption is the option interface for Del requests.
type DelOption interface {
	SetDelOption(*DelOptions)
}

// WatchOption is the option interface for watchers.
type WatchOption interface {
	SetWatchOption(*WatchOptions)
}

// prefix
type prefixOption struct{}

func (p *prefixOption) SetGetOption(opts *GetOptions) {
	opts.Prefix = true
}

func (p *prefixOption) SetDelOption(opts *DelOptions) {
	opts.Prefix = true
}

func (p *prefixOption) SetWatchOption(opts *WatchOptions) {
	opts.Prefix = true
}

// context
type contextOption struct {
	Context context.Context
}

func (c *contextOption) SetWatchOption(opts *WatchOptions) {
	opts.Context = c.Context
}

func (c *contextOption) SetGetOption(opts *GetOptions) {
	opts.Context = c.Context
}

func (c *contextOption) SetPutOption(opts *PutOptions) {
	opts.Context = c.Context
}

func (c *contextOption) SetDelOption(opts *DelOptions) {
	opts.Context = c.Context
}

// handler
type handlerOption struct {
	Handler HandlerFunc
}

func (h *handlerOption) SetGetOption(opts *GetOptions) {
	opts.Handler = h.Handler
}

// notifyCreated
type notifyCreatedOption struct {
	Callback NotifyCallback
}

func (n *notifyCreatedOption) SetWatchOption(opts *WatchOptions) {
	opts.NotifyCreated = n.Callback
}

// errorHandler
type errorHandlerOption struct {
	ErrorHandler ErrorFunc
}

func (e *errorHandlerOption) SetWatchOption(opts *WatchOptions) {
	opts.ErrorHandler = e.ErrorHandler
}

// ttl
type ttlOption struct {
	TTL time.Duration
}

func (t *ttlOption) SetPutOption(opts *PutOptions) {
	opts.TTL = t.TTL
}

// keepAlive
type keepAliveOption struct {
	ErrChan chan<- error
}

func (k *keepAliveOption) SetPutOption(opts *PutOptions) {
	opts.ErrChan = k.ErrChan
}

// insert
type insertOption struct{}

func (i *insertOption) SetPutOption(opts *PutOptions) {
	opts.Insert = true
}

// unmarshal
type unmarshalOption struct {
	Unmarshal interface{}
}

func (u *unmarshalOption) SetGetOption(opts *GetOptions) {
	opts.Unmarshal = new(unmarshal)
	opts.Unmarshal.Input = u.Unmarshal
	ptr := reflect.ValueOf(u.Unmarshal)
	opts.Unmarshal.Ptr = ptr

	if ptr.Kind() != reflect.Ptr {
		panic("unmarshal value has to be pointer")
	}

	opts.Unmarshal.Type = ptr.Elem().Kind()
	if opts.Unmarshal.Type == reflect.Slice {
		opts.Unmarshal.Slice = ptr.Elem()
		opts.Unmarshal.NewSlice = reflect.MakeSlice(opts.Unmarshal.Slice.Type(), 0, 0)
	}
}

type unmarshal struct {
	Input    interface{}
	Ptr      reflect.Value
	Type     reflect.Kind
	Slice    reflect.Value
	NewSlice reflect.Value
}

func (u *unmarshal) IsSlice() bool {
	if u == nil {
		return false
	}

	return u.Type == reflect.Slice
}

func (u *unmarshal) Element() interface{} {
	if u == nil || !u.IsSlice() {
		return nil
	}

	return reflect.New(u.Slice.Type().Elem()).Interface()
}

func (u *unmarshal) Append(v reflect.Value) {
	u.NewSlice = reflect.Append(u.NewSlice, v)
}
