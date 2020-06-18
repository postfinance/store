package hash

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChangeType(t *testing.T) {
	assert.Equal(t, "PUT", put.String())
	assert.Equal(t, "DELETE", del.String())
}
