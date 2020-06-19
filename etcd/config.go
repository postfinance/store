package etcd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/postfinance/store"
	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// Constants
const (
	DfltSeparator = '/'
)

// New configures a new etcd backend. At least Withclientv3 or WithEndoints option
// has to be used.
func New(opts ...Opt) (store.Backend, error) {
	e := Backend{
		separator:  DfltSeparator,
		errHandler: func(err error) error { return err },
	}

	for _, option := range opts {
		if err := option(&e); err != nil {
			return nil, err
		}
	}

	if err := e.valid(); err != nil {
		return nil, err
	}

	if e.client != nil {
		return &e, nil
	}

	// no etcd client specified
	e.config = &clientv3.Config{
		Endpoints:            e.endpoints,
		Username:             e.user,
		Password:             e.password,
		AutoSyncInterval:     e.autoSyncInterval,
		DialTimeout:          e.dialTimeout,
		DialKeepAliveTime:    e.dialKeepAliveTime,
		DialKeepAliveTimeout: e.dialKeepAliveTimeout,
		Context:              e.ctx,
		DialOptions:          e.dialOptions,
	}

	tlsConfig, err := e.ssl.tlsConfig()
	if err != nil {
		return nil, err
	}

	if tlsConfig != nil {
		e.config.TLS = tlsConfig
	}

	client, err := clientv3.New(*e.config)
	if err != nil {
		return nil, errors.Wrap(err, "could not create etcd client")
	}

	e.client = client

	return &e, nil
}

// Opt is a functional option to configure backend.
type Opt func(*Backend) error

// WithPrefix is an option to set the global prefix used for the backend
// WithPrefix("global") results in keys prefixed "global" + separator
func WithPrefix(p string) Opt {
	return func(e *Backend) error {
		e.prefix = p

		if e.prefix != "" {
			e.prefixReady2Use = fmt.Sprintf("%s%c", e.prefix, e.separator)
		}

		return nil
	}
}

// WithSeparator is an option to overwrite the default separator for keys
func WithSeparator(s rune) Opt {
	return func(e *Backend) error {
		e.separator = s

		if e.prefix != "" {
			e.prefixReady2Use = fmt.Sprintf("%s%c", e.prefix, e.separator)
		}

		return nil
	}
}

// WithRequestTimeout is an option to configure the request timeout.
func WithRequestTimeout(timeout time.Duration) Opt {
	return func(e *Backend) error {
		if timeout <= 0 {
			return errors.New("request timeout cannot be <= 0")
		}

		e.RequestTimeout = timeout

		return nil
	}
}

// WithUsername is an option to configure etcd username.
func WithUsername(username string) Opt {
	return func(e *Backend) error {
		e.user = username
		return nil
	}
}

// WithPassword is an option to configure etcd password.
func WithPassword(password string) Opt {
	return func(e *Backend) error {
		e.password = password
		return nil
	}
}

// WithClient is an option to configure the backend with a etcdv3.Client.
func WithClient(cli *clientv3.Client) Opt {
	return func(e *Backend) error {
		if cli == nil {
			return errors.New("etcd client cannot be nil")
		}

		e.client = cli

		return nil
	}
}

// WithEndpoints is an option to configure etcd endpoints.
func WithEndpoints(endpoints []string) Opt {
	return func(e *Backend) error {
		if len(endpoints) == 0 {
			return errors.New("the endpoints list cannot be empty")
		}

		e.endpoints = endpoints

		return nil
	}
}

// WithContext is an option to set the default client context. It can
// be used to cancel grpc dial out and other operations that do not have an
// explicit context.
func WithContext(ctx context.Context) Opt {
	return func(e *Backend) error {
		e.ctx = ctx
		return nil
	}
}

// WithKey is an option to configure the tls key.
func WithKey(key string) Opt {
	return func(e *Backend) error {
		if key == "" {
			return nil
		}

		if len(e.ssl.key) > 0 {
			return errors.New("got duplicate key data: key and key_file used")
		}

		e.ssl.key = []byte(key)

		return nil
	}
}

// WithKeyFile is an option to configure the tls key with the path to the key file.
func WithKeyFile(path string) Opt {
	return func(e *Backend) error {
		if path == "" {
			return nil
		}

		key, err := ioutil.ReadFile(path) // nolint: gosec // G304: Potential file inclusion via variable (gosec)
		if err != nil {
			return errors.Wrapf(err, "could not read key file %s", path)
		}

		if len(e.ssl.key) > 0 {
			return errors.New("got duplicate key data: key and key_file used")
		}

		e.ssl.key = key

		return nil
	}
}

// WithCert is an option to configure the tls certificate.
func WithCert(cert string) Opt {
	return func(e *Backend) error {
		if cert == "" {
			return nil
		}

		if len(e.ssl.cert) > 0 {
			return errors.New("got duplicate cert data: cert and cert_file used")
		}

		e.ssl.cert = []byte(cert)

		return nil
	}
}

// WithCertFile is an option to configure the tls certificate with the path to the certificate file.
func WithCertFile(path string) Opt {
	return func(e *Backend) error {
		if path == "" {
			return nil
		}

		cert, err := ioutil.ReadFile(path) // nolint: gosec // G304: Potential file inclusion via variable (gosec)
		if err != nil {
			return errors.Wrapf(err, "could not read cert file %s", path)
		}

		if len(e.ssl.cert) > 0 {
			return errors.New("got duplicate cert data: cert and cert_file used")
		}

		e.ssl.cert = cert

		return nil
	}
}

// WithCA is an option to configure the tls ca certificate.
func WithCA(ca string) Opt {
	return func(e *Backend) error {
		if ca == "" {
			return nil
		}

		if len(e.ssl.ca) > 0 {
			return errors.New("got duplicate ca data: ca and ca_file used")
		}

		e.ssl.ca = []byte(ca)

		return nil
	}
}

// WithCAFile is an option to configure the tls ca certificate with the path to the ca file.
func WithCAFile(path string) Opt {
	return func(e *Backend) error {
		if path == "" {
			return nil
		}

		ca, err := ioutil.ReadFile(path) // nolint: gosec // G304: Potential file inclusion via variable (gosec)
		if err != nil {
			return errors.Wrapf(err, "could not read ca file %s", path)
		}

		if len(e.ssl.ca) > 0 {
			return errors.New("got duplicate ca data: ca and ca_file used")
		}

		e.ssl.ca = ca

		return nil
	}
}

// WithAutoSyncInterval is an option to configure the autosync interval.
func WithAutoSyncInterval(interval time.Duration) Opt {
	return func(e *Backend) error {
		if interval < 0 {
			return errors.New("autosync interval cannot be < 0")
		}

		e.autoSyncInterval = interval

		return nil
	}
}

// WithDialTimeout is an option to configure the dial timeout.
func WithDialTimeout(timeout time.Duration) Opt {
	return func(e *Backend) error {
		if timeout < 0 {
			return errors.New("dial timeout cannot be < 0")
		}

		e.dialTimeout = timeout

		return nil
	}
}

// WithDialKeepAliveTimeout is an option to configure the keepalive dial timeout.
func WithDialKeepAliveTimeout(t time.Duration) Opt {
	return func(e *Backend) error {
		if t < 0 {
			return errors.New("dial keepalive timeout cannot be < 0")
		}

		e.dialKeepAliveTimeout = t

		return nil
	}
}

// WithDialKeepAliveTime is an option to configure the keepalive dial time.
func WithDialKeepAliveTime(t time.Duration) Opt {
	return func(e *Backend) error {
		if t < 0 {
			return errors.New("dial keepalive time cannot be < 0")
		}

		e.dialKeepAliveTime = t

		return nil
	}
}

// WithErrorHandler is a function that can be used to handle etcd server
// errors. The default error handler does nothing.
func WithErrorHandler(f store.ErrorFunc) Opt {
	return func(e *Backend) error {
		if f == nil {
			return errors.New("error handler cannot be nil")
		}

		e.errHandler = f

		return nil
	}
}

// WithDialOptions is a function that sets the underlying grpc dial options.
func WithDialOptions(d ...grpc.DialOption) Opt {
	return func(e *Backend) error {
		e.dialOptions = d
		return nil
	}
}

type ssl struct {
	key  []byte
	cert []byte
	ca   []byte
}

func (s ssl) tlsConfig() (*tls.Config, error) {
	var tlsConfig *tls.Config
	// load client cert
	if len(s.cert) > 0 || len(s.key) > 0 {
		certBlock, _ := pem.Decode(s.cert)
		if certBlock == nil {
			return nil, errors.Errorf("invalid client certificate: %v", string(s.cert))
		}

		keyBlock, _ := pem.Decode(s.key)
		if keyBlock == nil {
			return nil, errors.Errorf("invalid client key: %v", string(s.key))
		}

		cert, err := tls.X509KeyPair(s.cert, s.key)
		if err != nil {
			return nil, err
		}

		tlsConfig = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true, // nolint: gosec // G402: TLS InsecureSkipVerify set true. (gosec)
		}
	}
	// load CA cert
	if len(s.ca) > 0 {
		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM(s.ca); !ok {
			return nil, errors.Errorf("invalid ca certificate: %v", string(s.ca))
		}

		if tlsConfig == nil {
			tlsConfig = &tls.Config{}
		}

		tlsConfig.RootCAs = caCertPool
		tlsConfig.InsecureSkipVerify = false
	}

	return tlsConfig, nil
}

func (s ssl) valid() error {
	if len(s.cert) > 0 && len(s.key) == 0 {
		return errors.New("tls certificate configured without a key")
	}

	if len(s.key) > 0 && len(s.cert) == 0 {
		return errors.New("tls key configured without a certificate")
	}

	return nil
}
