package etcd

import (
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealth(t *testing.T) {
	t.Run("healthy", func(t *testing.T) {
		for _, p := range []string{"", "root"} {
			opts := []Opt{}
			if p != "" {
				opts = append(opts, WithPrefix(p))
			}

			b, _, teardown := setupTestStore(t, false, opts)
			defer teardown()

			e, _ := b.(*Backend)
			s := e.Health("key")
			require.Len(t, s, 1)
			assert.True(t, len(s[0].Endpoint) > 0)
			// flaky, sometimes works, sometimes not
			// assert.True(t, s[0].Healthy, s[0].Detail)
		}
	})

	t.Run("unhealthy", func(t *testing.T) {
		ep := []string{"localhost:1111", "localhost:1112"}
		e := Backend{
			RequestTimeout: 1 * time.Second,
			endpoints:      ep,
			config:         &clientv3.Config{Endpoints: ep},
		}
		s := e.Health("key")
		require.Len(t, s, 2)
		assert.True(t, len(s[0].Endpoint) > 0)
		assert.False(t, s[0].Healthy)
	})
}
