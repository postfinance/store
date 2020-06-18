package store

import (
	"bytes"
	"encoding/json"
)

const (
	start = "["
	end   = "]" // nolint: deadcode, varcheck // `end` is unused
	sep   = ","
)

// UnmarshalJSONList is a helper function, that unmarshals a list of json values to
// a list of the corresponding type.
// nolint: godox
// TODO(zbindenren): performance test
func UnmarshalJSONList(v interface{}, key string, b Backend, opts ...GetOption) error {
	var buf bytes.Buffer
	options := append(opts, WithHandler(concat(&buf)))

	_, err := b.Get(key, options...)
	if err != nil {
		return err
	}

	data := append(bytes.TrimRight(buf.Bytes(), ","), []byte("]")...)

	return json.Unmarshal(data, v)
}

func concat(buf *bytes.Buffer) HandlerFunc {
	_, _ = buf.WriteString(start)

	return func(k, v []byte) error {
		_, _ = buf.Write(v)
		_, _ = buf.Write([]byte(sep))

		return nil
	}
}
