// Package hash implements the store.Backend interface for a hash map
package hash

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/postfinance/store"
)

// entry in the hash map
type entry struct {
	Data    store.Entry
	Updated time.Time
	TTL     time.Duration
}

// Backend hash backend
type Backend struct {
	sync.RWMutex
	prefix          string
	prefixReady2Use string
	separator       rune
	data            map[string]entry
	ttl             time.Duration
	watchKey        map[string][]notifyCallbackFunc
	watchKeyPrefix  map[string][]notifyCallbackFunc
}

// AbsKey will convert a relativ key in a absolute key
// AbsKey("a/b") with prefix "root" and separator '/' returns "root/a/b"
// AbsKey does not validate the given key.
// Given a faulty relative key returns a faulty absolute key.
func (h *Backend) AbsKey(k string) string {
	if h.prefix == "" {
		return k
	}

	return h.prefixReady2Use + k
}

// RelKey will convert an absolute key in a relativ key
// RelKey("root/a/b") with prefix "root" and separator '/' returns "a/b"
// RelKey does not validate the given key.
// Given a faulty absolute key returns a faulty relativ key.
func (h *Backend) RelKey(k string) string {
	if h.prefix == "" {
		return k
	}

	return strings.TrimPrefix(k, h.prefixReady2Use)
}

// JoinKey returns a formatted key
// it joins the given elements with the correct delimiter
func (h *Backend) JoinKey(args ...string) string {
	return strings.Join(args, string(h.separator))
}

// SplitKey returns the key elements
// it splits the given key in its elements with the correct delimiter
func (h *Backend) SplitKey(key string) []string {
	return strings.Split(strings.Trim(key, string(h.separator)), string(h.separator))
}

// KeyLeaf returns the leave (last) element of a key
func (h *Backend) KeyLeaf(key string) string {
	elems := h.SplitKey(key)
	if len(elems) > 0 {
		return elems[len(elems)-1]
	}

	return ""
}

// TTL returns the configured TTL
func (h *Backend) TTL() time.Duration {
	return h.ttl
}

// register a watch key
func (h *Backend) register(key string, prefix bool, f notifyCallbackFunc) {
	h.Lock()
	if prefix {
		if _, ok := h.watchKeyPrefix[key]; !ok {
			h.watchKeyPrefix[key] = []notifyCallbackFunc{}
		}

		h.watchKeyPrefix[key] = append(h.watchKeyPrefix[key], f)
	} else {
		if _, ok := h.watchKey[key]; !ok {
			h.watchKey[key] = []notifyCallbackFunc{}
		}
		h.watchKey[key] = append(h.watchKey[key], f)
	}
	h.Unlock()
}

// notify all registrants
func (h *Backend) notify(key string, msg changeNotification) {
	for _, f := range h.watchKey[key] {
		f(msg)
	}

	for k, callbacks := range h.watchKeyPrefix {
		if strings.HasPrefix(key, k) {
			for _, f := range callbacks {
				f(msg)
			}
		}
	}
}

// exists checks if an entry is expired
func (h *Backend) exists(e entry) bool {
	// entry has no TTL or an unexpired TTL
	if e.TTL == 0 || e.Updated.Add(e.TTL).After(time.Now()) {
		return true
	}
	// entry has a expired TTL
	h.del(e.Data)

	return false
}

// keys returns all keys with prefix
func (h *Backend) keys(prefix string) []string {
	keys := []string{}

	h.RLock()
	for k := range h.data {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	h.RUnlock()
	sort.Strings(keys)

	return keys
}

// Get returns a list of store entries
// WithPrefix, WithHandler are supported
// WithContext will be ignored
//nolint:gocyclo // review
func (h *Backend) Get(key string, ops ...store.GetOption) ([]store.Entry, error) {
	opts := &store.GetOptions{}

	for _, op := range ops {
		op.SetGetOption(opts)
	}

	if opts.Filter == nil {
		opts.Filter = func([]byte, []byte) bool {
			return true
		}
	}

	if opts.Handler == nil {
		opts.Handler = func([]byte, []byte) error {
			return nil
		}
	}

	absKey := h.AbsKey(key)

	keys := []string{absKey}
	if opts.Prefix {
		keys = h.keys(absKey)
	}

	result := []store.Entry{}

	for _, k := range keys {
		h.RLock()
		v, ok := h.data[k]
		h.RUnlock()

		if ok && h.exists(v) {
			if ok := opts.Filter([]byte(v.Data.Key), v.Data.Value); !ok {
				continue
			}

			if opts.Unmarshal != nil && !opts.Unmarshal.IsSlice() {
				return nil, json.Unmarshal(v.Data.Value, &opts.Unmarshal.Input)
			}

			if opts.Unmarshal.IsSlice() {
				element := opts.Unmarshal.Element()
				if err := json.Unmarshal(v.Data.Value, &element); err != nil {
					return nil, err
				}

				opts.Unmarshal.Append(reflect.ValueOf(element).Elem())

				continue
			}

			e := v.Data
			result = append(result, e)

			if err := opts.Handler([]byte(e.Key), e.Value); err != nil {
				return result, err
			}
		}
	}

	if len(result) == 0 && !opts.Prefix {
		return result, store.ErrKeyNotFound
	}

	if opts.Unmarshal.IsSlice() {
		opts.Unmarshal.Ptr.Elem().Set(opts.Unmarshal.NewSlice)
	}

	return result, nil
}

// Put insert and/or update an entry in the store
// WithTTL, WithInsert are supported
// WithContext will be ignored
func (h *Backend) Put(e *store.Entry, ops ...store.PutOption) (bool, error) {
	opts := &store.PutOptions{}

	for _, op := range ops {
		op.SetPutOption(opts)
	}
	// use the background context if no context is given
	ctx := context.Background()
	if opts.Context != nil {
		ctx = opts.Context
	}

	// Get entry
	entries, err := h.Get(e.Key)

	// Insert mode and key exists
	if opts.Insert && err == nil {
		return false, nil
	}

	v := store.Entry{}
	if len(entries) > 0 {
		v = entries[0]
	}

	// TTL must be set for KeepAlive
	if opts.TTL == 0 && opts.ErrChan != nil {
		opts.TTL = DfltKeepAliveTTL
	}

	// WithTTL will overwrite the default ttl
	ttl := h.ttl
	if opts.TTL > 0 {
		ttl = opts.TTL
	}

	// Insert/Update the entry
	absKey := h.AbsKey(e.Key)

	h.Lock()
	h.data[absKey] = entry{
		Data:    *e,
		Updated: time.Now(),
		TTL:     ttl,
	}
	h.Unlock()

	h.notify(absKey, changeNotification{
		Type: put,
		Data: *e,
	})

	// keep-alive
	if opts.ErrChan != nil {
		go func() {
			ticker := time.NewTicker(opts.TTL / 2)

			for {
				select {
				case <-ctx.Done():
					// get a full TTL before delete
					_ = h.keepAlive(e.Key)
					opts.ErrChan <- context.Canceled

					return
				case <-ticker.C:
					// if the key does no longer exist
					// stop the monitor and act like etcd
					if err := h.keepAlive(e.Key); err != nil {
						opts.ErrChan <- store.ErrResponseChannelClosed

						return
					}
				}
			}
		}()
	}

	return len(e.Value) == 0 || !bytes.Equal(v.Value, e.Value), nil
}

func (h *Backend) keepAlive(key string) error {
	// Insert/Update the entry
	absKey := h.AbsKey(key)

	h.Lock()
	defer h.Unlock()

	e, ok := h.data[absKey]
	if !ok {
		return store.ErrKeyNotFound
	}

	e.Updated = time.Now()
	h.data[absKey] = e

	return nil
}

// Del an entry from the store
// WithPrefix is supported
// WithContext will be ignored
func (h *Backend) Del(key string, ops ...store.DelOption) (int64, error) {
	var count int64

	opts := &store.DelOptions{}

	for _, op := range ops {
		op.SetDelOption(opts)
	}

	getOps := []store.GetOption{}
	if opts.Prefix {
		getOps = append(getOps, store.WithPrefix())
	}

	entries, err := h.Get(key, getOps...)
	if err != nil && err != store.ErrKeyNotFound {
		return count, err
	}

	for _, e := range entries {
		count++

		h.del(e)
	}

	return count, nil
}

func (h *Backend) del(e store.Entry) {
	absKey := h.AbsKey(e.Key)

	h.Lock()
	delete(h.data, absKey)
	h.notify(absKey, changeNotification{
		Type: del,
		Data: e,
	})
	h.Unlock()
}

// Close exists to implement the store interface, there
// is no connection to close.
func (h *Backend) Close() error {
	return nil
}

// ensure the hash.Backend implements the necessary interfaces
var _ store.Backend = &Backend{}

var _ store.BackendKeyer = &Backend{}
