package hash

import (
	"fmt"
	"time"

	"git.pnet.ch/golang/pkg/store"

	"github.com/pkg/errors"
)

// Constants
const (
	DfltSeparator = '/'
)

// New returns a new store and the function to stop the cache janitor
func New(opts ...Opt) (store.Backend, error) {
	h := &Hash{
		separator:      DfltSeparator,
		data:           make(map[string]entry),
		watchKey:       make(map[string][]notifyCallbackFunc),
		watchKeyPrefix: make(map[string][]notifyCallbackFunc),
	}

	for _, option := range opts {
		if err := option(h); err != nil {
			return nil, err
		}
	}

	return h, nil
}

// Opt is a functional option to configure backend
type Opt func(*Hash) error

// WithPrefix is an option to set the global prefix used for the backend
// WithPrefix("global") results in keys prefixed "global" + separator
func WithPrefix(p string) Opt {
	return func(h *Hash) error {
		h.prefix = p

		if h.prefix != "" {
			h.prefixReady2Use = fmt.Sprintf("%s%c", h.prefix, h.separator)
		}

		return nil
	}
}

// WithSeparator is an option to overwrite the default separator for keys
func WithSeparator(s rune) Opt {
	return func(h *Hash) error {
		h.separator = s

		if h.prefix != "" {
			h.prefixReady2Use = fmt.Sprintf("%s%c", h.prefix, h.separator)
		}

		return nil
	}
}

// WithTTL is an option to add a time to live for hash entries
// the TTL will be the preset which can be overwritten for each Operation supporting WithTTL
func WithTTL(ttl time.Duration) Opt {
	return func(h *Hash) error {
		if ttl < 0 {
			return errors.New("ttl cannot be < 0")
		}

		h.ttl = ttl

		return nil
	}
}
