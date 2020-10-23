package hash_test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/postfinance/store"
	"github.com/postfinance/store/hash"
	"github.com/stretchr/testify/require"
)

// nolint: gochecknoglobals
var (
	entries []*store.Entry
	data    []*A
)

// nolint: gochecknoinits
func init() {
	entries = []*store.Entry{}
	data = generate(100000)

	for i, a := range data {
		value, err := json.Marshal(a)
		if err != nil {
			panic(err)
		}

		entries = append(entries, &store.Entry{
			Key:   fmt.Sprintf("key%d", i),
			Value: value,
		})
	}
}

/*
go test -bench . -benchtime=10s
goos: linux
goarch: amd64
pkg: github.com/postfinance/store/hash
BenchmarkUnmarshal100-4            30000            583465 ns/op           81328 B/op       1024 allocs/op
BenchmarkIterate100-4              30000            652154 ns/op           81328 B/op       1024 allocs/op
BenchmarkHandler100-4              30000            489527 ns/op           79736 B/op        832 allocs/op
BenchmarkUnmarshal1000-4            3000           5177122 ns/op          757408 B/op      10030 allocs/op
BenchmarkIterate1000-4              3000           6228008 ns/op          757408 B/op      10030 allocs/op
BenchmarkHandler1000-4              2000           5758885 ns/op          748264 B/op       8041 allocs/op
BenchmarkUnmarshal10000-4            200          69585925 ns/op        11000336 B/op     100050 allocs/op
BenchmarkIterate10000-4              200          69515481 ns/op        11000335 B/op     100050 allocs/op
BenchmarkHandler10000-4              200          66994008 ns/op        11600472 B/op      80069 allocs/op
BenchmarkUnmarshal100000-4            20         731059321 ns/op        109470739 B/op   1000070 allocs/op
BenchmarkIterate100000-4              20         736711026 ns/op        109470740 B/op   1000070 allocs/op
BenchmarkHandler100000-4              20         746221662 ns/op        118637666 B/op    800099 allocs/op
PASS
ok      github.com/postfinance/store/hash       227.357s
*/

func BenchmarkUnmarshal100(b *testing.B)    { benchmarkUnmarshal(100, b) }
func BenchmarkIterate100(b *testing.B)      { benchmarkIterate(100, b) }
func BenchmarkHandler100(b *testing.B)      { benchmarkHandler(100, b) }
func BenchmarkUnmarshal1000(b *testing.B)   { benchmarkUnmarshal(1000, b) }
func BenchmarkIterate1000(b *testing.B)     { benchmarkIterate(1000, b) }
func BenchmarkHandler1000(b *testing.B)     { benchmarkHandler(1000, b) }
func BenchmarkUnmarshal10000(b *testing.B)  { benchmarkUnmarshal(10000, b) }
func BenchmarkIterate10000(b *testing.B)    { benchmarkIterate(10000, b) }
func BenchmarkHandler10000(b *testing.B)    { benchmarkHandler(10000, b) }
func BenchmarkUnmarshal100000(b *testing.B) { benchmarkUnmarshal(100000, b) }
func BenchmarkIterate100000(b *testing.B)   { benchmarkIterate(100000, b) }
func BenchmarkHandler100000(b *testing.B)   { benchmarkHandler(100000, b) }

func benchmarkUnmarshal(max int, b *testing.B) {
	b.StopTimer()

	back, err := hash.New()
	require.NoError(b, err)

	for i := 0; i < max; i++ {
		_, err := back.Put(entries[i])
		require.NoError(b, err)
	}
	b.ReportAllocs()
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		result := []A{}

		_, err := back.Get("key", store.WithPrefix(), store.WithUnmarshal(&result))
		if err != nil {
			b.Error(err)
		}

		if len(result) != max {
			b.Errorf("wrong result %d", len(result))
		}
	}
}

func benchmarkIterate(max int, b *testing.B) {
	b.StopTimer()

	back, err := hash.New()
	require.NoError(b, err)

	for i := 0; i < max; i++ {
		_, err := back.Put(entries[i])
		require.NoError(b, err)
	}
	b.ReportAllocs()
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		result := []A{}

		values, err := back.Get("key", store.WithPrefix(), store.WithUnmarshal(&result))
		if err != nil {
			b.Error(err)
		}

		for _, e := range values {
			a := A{}
			if err := json.Unmarshal(e.Value, &a); err != nil {
				b.Error(err)
			}

			result = append(result, a)
		}

		if len(result) != max {
			b.Errorf("wrong result %d", len(result))
		}
	}
}

func benchmarkHandler(max int, b *testing.B) {
	b.StopTimer()

	back, err := hash.New()
	require.NoError(b, err)

	for i := 0; i < max; i++ {
		_, err := back.Put(entries[i])
		require.NoError(b, err)
	}
	b.ReportAllocs()
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		result := []A{}
		a := A{}

		_, err := back.Get("key", store.WithPrefix(), store.WithHandler(func(k, v []byte) error {
			if err := json.Unmarshal(v, &a); err != nil {
				return err
			}
			result = append(result, a)
			return nil
		}))
		if err != nil {
			b.Error(err)
		}

		if len(result) != max {
			b.Errorf("wrong result %d", len(result))
		}
	}
}

func randString(l int) string {
	buf := make([]byte, l)
	for i := 0; i < (l+1)/2; i++ {
		buf[i] = byte(rand.Intn(256)) // nolint: gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand)
	}

	return fmt.Sprintf("%x", buf)[:l]
}

type A struct {
	Name     string
	BirthDay time.Time
	Phone    string
	Siblings int
	Spouse   bool
	Money    float64
}

func generate(max int) []*A {
	a := make([]*A, 0, max)
	for i := 0; i < max; i++ {
		a = append(a, &A{
			Name:     randString(16),
			BirthDay: time.Now(),
			Phone:    randString(10),
			Siblings: rand.Intn(5),      // nolint: gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand)
			Spouse:   rand.Intn(2) == 1, // nolint: gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand)
			Money:    rand.Float64(),    // nolint: gosec // G404: Use of weak random number generator (math/rand instead of crypto/rand)
		})
	}

	return a
}
