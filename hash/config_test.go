package hash

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOptions(t *testing.T) {
	const (
		prefix = "root"
	)

	t.Run("WithPrefix", func(t *testing.T) {
		h := &Backend{}
		assert.NoError(t, WithPrefix(prefix)(h))
		assert.EqualValues(t, h.prefix, prefix)
	})

	t.Run("WithSeparator", func(t *testing.T) {
		s := '!'
		h := &Backend{}
		assert.NoError(t, WithSeparator(s)(h))
		assert.EqualValues(t, h.separator, s)
	})

	t.Run("WithTTL", func(t *testing.T) {
		h := &Backend{}
		assert.NoError(t, WithTTL(1)(h))
		assert.EqualValues(t, h.TTL(), 1)
	})

	t.Run("WithTTL invalid value", func(t *testing.T) {
		h := &Backend{}
		assert.Error(t, WithTTL(-1)(h))
		assert.EqualValues(t, h.TTL(), 0)
	})

	t.Run("New with error", func(t *testing.T) {
		h, err := New(WithTTL(-1))
		assert.Error(t, err)
		assert.Nil(t, h)
	})

	t.Run("WithPrefix and default separator", func(t *testing.T) {
		h, err := New(WithPrefix(prefix))
		assert.NoError(t, err)
		assert.EqualValues(t, h.prefix, prefix)
		assert.EqualValues(t, h.prefixReady2Use, fmt.Sprintf("%s/", prefix))
	})

	t.Run("WithPrefix and WithSeparator", func(t *testing.T) {
		s := '!'
		h, err := New(WithPrefix(prefix), WithSeparator(s))
		assert.NoError(t, err)
		assert.EqualValues(t, h.separator, s)
		assert.EqualValues(t, h.prefixReady2Use, fmt.Sprintf("%s%c", prefix, s))
	})
}
