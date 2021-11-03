// Package etcd implements the store.Backend interface for the etcd
package etcd

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/postfinance/store"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// Backend is uses etcd to store the data.
type Backend struct {
	RequestTimeout       time.Duration
	client               *clientv3.Client
	config               *clientv3.Config
	endpoints            []string
	user                 string
	password             string
	prefix               string
	prefixReady2Use      string
	separator            rune
	ssl                  ssl
	autoSyncInterval     time.Duration
	dialKeepAliveTime    time.Duration
	dialKeepAliveTimeout time.Duration
	dialTimeout          time.Duration
	ctx                  context.Context
	errHandler           store.ErrorFunc
	dialOptions          []grpc.DialOption
}

// AbsKey will convert a relativ key in a absolute key
// AbsKey("a/b") with prefix "root" and separator '/' returns "root/a/b"
// AbsKey does not validate the given key.
// Given a faulty relative key returns a faulty absolute key.
func (e *Backend) AbsKey(k string) string {
	if e.prefix == "" {
		return k
	}

	return e.prefixReady2Use + k
}

// RelKey will convert an absolute key in a relativ key
// RelKey("root/a/b") with prefix "root" and separator '/' returns "a/b"
// RelKey does not validate the given key.
// Given a faulty absolute key returns a faulty relativ key.
func (e *Backend) RelKey(k string) string {
	if e.prefix == "" {
		return k
	}

	return strings.TrimPrefix(k, e.prefixReady2Use)
}

// JoinKey returns a formatted key
// it joins the given elements with the correct delimiter
func (e *Backend) JoinKey(args ...string) string {
	return strings.Join(args, string(e.separator))
}

// SplitKey returns the key elements
// it splits the given key in its elements with the correct delimiter
func (e *Backend) SplitKey(key string) []string {
	return strings.Split(strings.Trim(key, string(e.separator)), string(e.separator))
}

// KeyLeaf returns the leave (last) element of a key
func (e *Backend) KeyLeaf(key string) string {
	elems := e.SplitKey(key)
	if len(elems) > 0 {
		return elems[len(elems)-1]
	}

	return ""
}

// Put is used to insert or update an entry
// nolint: gocyclo
func (e *Backend) Put(entry *store.Entry, ops ...store.PutOption) (bool, error) {
	opts := &store.PutOptions{}

	for _, op := range ops {
		op.SetPutOption(opts)
	}

	ctx := context.Background()
	if opts.Context != nil {
		ctx = opts.Context
	}

	rctx, cancel := e.requestContext(ctx)
	defer cancel()

	etcdOpts := []clientv3.OpOption{}

	var lease *clientv3.LeaseGrantResponse

	// TTL must be set for KeepAlive
	if opts.TTL == 0 && opts.ErrChan != nil {
		opts.TTL = DfltKeepAliveTTL
	}

	if opts.TTL > 0 {
		var err error

		lease, err = e.client.Grant(rctx, int64(opts.TTL.Seconds()))
		if e.errHandler(err) != nil {
			return false, errors.New("could not get lease with ttl")
		}

		etcdOpts = append(etcdOpts, clientv3.WithLease(lease.ID))
	}

	absKey := e.AbsKey(entry.Key)
	value := string(entry.Value)

	if opts.Insert {
		resp, err := e.client.Txn(rctx).
			If(clientv3.Compare(clientv3.Version(absKey), "=", 0)).
			Then(clientv3.OpPut(absKey, value, etcdOpts...)).
			Commit()
		if e.errHandler(err) != nil {
			return false, err
		}

		return resp.Succeeded, e.startKeepalive(ctx, lease, opts.ErrChan)
	}

	resp, err := e.client.Txn(rctx).
		If(clientv3.Compare(clientv3.Value(absKey), "=", value)).
		Else(clientv3.OpPut(absKey, value, etcdOpts...)).
		Commit()
	if e.errHandler(err) != nil {
		return false, err
	}

	return !resp.Succeeded, e.startKeepalive(ctx, lease, opts.ErrChan)
}

// Get is used to fetch an one ore many entries.
// nolint: gocyclo
func (e *Backend) Get(key string, ops ...store.GetOption) ([]store.Entry, error) {
	opts := &store.GetOptions{}

	for _, op := range ops {
		op.SetGetOption(opts)
	}

	etcdOpts := []clientv3.OpOption{}
	if opts.Prefix {
		etcdOpts = append(etcdOpts, clientv3.WithPrefix())
	}

	ctx := context.Background()
	if opts.Context != nil {
		ctx = opts.Context
	}

	rctx, cancel := e.requestContext(ctx)
	defer cancel()

	resp, err := e.client.Get(rctx, e.AbsKey(key), etcdOpts...)
	if e.errHandler(err) != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 && !opts.Prefix {
		return []store.Entry{}, store.ErrKeyNotFound
	}

	if opts.Handler == nil {
		opts.Handler = func([]byte, []byte) error {
			return nil
		}
	}

	result := []store.Entry{}

	for _, value := range resp.Kvs {
		if err := opts.Handler([]byte(e.RelKey(string(value.Key))), value.Value); err != nil {
			return result, err
		}

		if opts.Unmarshal != nil && !opts.Unmarshal.IsSlice() {
			return nil, json.Unmarshal(value.Value, &opts.Unmarshal.Input)
		}

		if opts.Unmarshal.IsSlice() {
			element := opts.Unmarshal.Element()
			if err := json.Unmarshal(value.Value, &element); err != nil {
				return nil, err
			}

			opts.Unmarshal.Append(reflect.ValueOf(element).Elem())

			continue
		}

		result = append(result, store.Entry{
			Key:   e.RelKey(string(value.Key)),
			Value: value.Value,
		})
	}

	if opts.Unmarshal.IsSlice() {
		opts.Unmarshal.Ptr.Elem().Set(opts.Unmarshal.NewSlice)
	}

	return result, nil
}

// Del is used to permanently delete an entry
func (e *Backend) Del(key string, ops ...store.DelOption) (int64, error) {
	opts := &store.DelOptions{}

	for _, op := range ops {
		op.SetDelOption(opts)
	}

	ctx := context.Background()
	if opts.Context != nil {
		ctx = opts.Context
	}

	rctx, cancel := e.requestContext(ctx)
	defer cancel()

	absKey := e.AbsKey(key)

	if opts.Prefix {
		resp, err := e.client.Delete(rctx, absKey, clientv3.WithPrefix())
		if e.errHandler(err) != nil {
			return 0, err
		}

		return resp.Deleted, nil
	}

	t, err := e.client.Txn(rctx).
		If(clientv3.Compare(clientv3.Version(absKey), ">", 0)).
		Then(clientv3.OpDelete(absKey)).
		Commit()
	if e.errHandler(err) != nil {
		return 0, err
	}

	if !t.Succeeded {
		return 0, nil
	}

	return 1, err
}

// Close closes the etcd connection.
func (e *Backend) Close() error {
	return e.client.Close()
}

// Client returns the underlying client.Client.
func (e *Backend) Client() *clientv3.Client {
	return e.client
}

func (e *Backend) valid() error {
	if err := e.ssl.valid(); err != nil {
		return err
	}

	if e.client == nil && len(e.endpoints) == 0 {
		return errors.New("configured etcd client and endpoints are empty")
	}

	return nil
}

func (e *Backend) requestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if e.RequestTimeout == 0 {
		return ctx, func() {}
	}

	// add timeout to context if given
	return context.WithTimeout(ctx, e.RequestTimeout)
}

func (e *Backend) startKeepalive(ctx context.Context, lease *clientv3.LeaseGrantResponse, errChan chan<- error) error {
	if errChan == nil {
		return nil
	}
	// start keep-alive
	ch, err := e.client.KeepAlive(ctx, lease.ID)
	if err != nil {
		return err
	}
	// start keep-alive monitor
	go func() {
		for {
			select {
			case <-ctx.Done():
				errChan <- context.Canceled

				return
			case _, ok := <-ch:
				if !ok {
					errChan <- store.ErrResponseChannelClosed

					return
				}
			}
		}
	}()

	return nil
}

// ensure the etcd.Backend implements the necessary interfaces
var _ store.Backend = &Backend{}

var _ store.BackendKeyer = &Backend{}
