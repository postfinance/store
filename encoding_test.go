package store

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnmarshal(t *testing.T) {
	count := 1000
	b := newMockBackend(count)
	l := []testStruct{}
	_, err := b.Get("", WithHandler(func(k, v []byte) error {
		s := testStruct{}
		err := json.Unmarshal(v, &s)
		if err != nil {
			return err
		}
		l = append(l, s)
		return nil
	}))
	require.NoError(t, err)
	assert.Len(t, l, count)
}

func benchmarkUnmarshal(i int, b *testing.B) {
	b.StopTimer()

	back := newMockBackend(i)

	b.ReportAllocs()
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		l := []testStruct{}
		_, err := back.Get("", WithHandler(func(k, v []byte) error {
			s := testStruct{}
			err := json.Unmarshal(v, &s)
			if err != nil {
				return err
			}
			l = append(l, s)
			return nil
		}))
		require.NoError(b, err)
	}
}

/*
BenchmarkUnmarshal10-4                    200000            194390 ns/op
BenchmarkUnmarshal100-4                    20000           2076871 ns/op
BenchmarkUnmarshal1000-4                    2000          21495289 ns/op
BenchmarkUnmarshal10000-4                    200         233705937 ns/op
BenchmarkUnmarshal100000-4                    20        2317840686 ns/op
*/
func BenchmarkUnmarshal10(b *testing.B)     { benchmarkUnmarshal(10, b) }
func BenchmarkUnmarshal100(b *testing.B)    { benchmarkUnmarshal(100, b) }
func BenchmarkUnmarshal1000(b *testing.B)   { benchmarkUnmarshal(1000, b) }
func BenchmarkUnmarshal10000(b *testing.B)  { benchmarkUnmarshal(10000, b) }
func BenchmarkUnmarshal100000(b *testing.B) { benchmarkUnmarshal(100000, b) }

type mockBackend struct {
	entries []Entry
}

func (m mockBackend) Get(key string, ops ...GetOption) ([]Entry, error) {
	opts := &GetOptions{}

	for _, op := range ops {
		op.SetGetOption(opts)
	}

	if opts.Handler == nil {
		opts.Handler = func([]byte, []byte) error {
			return nil
		}
	}

	for _, e := range m.entries {
		if err := opts.Handler([]byte(e.Key), e.Value); err != nil {
			return m.entries, err
		}
	}

	return m.entries, nil
}

func (mockBackend) Del(string, ...DelOption) (int64, error) {
	panic("not implemented")
}

func (mockBackend) Watch(string, Watcher, ...WatchOption) error {
	panic("not implemented")
}

func (m *mockBackend) Put(e *Entry, opst ...PutOption) (bool, error) {
	m.entries = append(m.entries, *e)
	return true, nil
}

func (mockBackend) Close() error {
	return nil
}

func (m *mockBackend) WatchChan(string, any, chan error, ...WatchOption) (WatchStarter, error) {
	return nil, nil
}

func newMockBackend(max int) Backend {
	entries := make([]Entry, 0, max)
	for i := 0; i < max; i++ {
		entries = append(entries, Entry{Key: "", Value: []byte(data)})
	}

	return &mockBackend{
		entries: entries,
	}
}

type testStruct struct {
	ID           string `json:"id"`
	Organization string `json:"organization"`
	Name         string `json:"name"`
	Description  string `json:"description"`
	User         string `json:"user"`
	Group        string `json:"group"`
	Shell        string `json:"shell"`
	Timeout      int    `json:"timeout"`
	Interval     int64  `json:"interval"`
	Constraints  struct {
		Os    []string `json:"os"`
		Host  []string `json:"host"`
		Files []string `json:"files"`
	} `json:"constraints"`
	Discovery struct {
		Script struct {
			Data       string `json:"data"`
			Rediscover bool   `json:"rediscover"`
		} `json:"script"`
	} `json:"discovery"`
	Tags     any `json:"tags"`
	Commands struct {
		Start struct {
			Shell      string `json:"shell"`
			Timeout    string `json:"timeout"`
			Rediscover bool   `json:"rediscover"`
		} `json:"start"`
		Status struct {
			Shell      string `json:"shell"`
			Timeout    string `json:"timeout"`
			Rediscover bool   `json:"rediscover"`
		} `json:"status"`
		Stop struct {
			Shell      string `json:"shell"`
			Timeout    string `json:"timeout"`
			Rediscover bool   `json:"rediscover"`
		} `json:"stop"`
	} `json:"commands"`
}

const data = `{"id":"admin:lslb-pool","organization":"admin","name":"lslb-pool","description":"","user":"","group":"","shell":"bash","timeout":0,"interval":300000000000,"constraints":{"os":["linux"],"host":["p1-linux-mlsu005","p1-linux-mlsu006"],"files":["/usr/bin/lslb"]},"discovery":{"script":{"data":"echo \"fake-pool\"\n# sudo /usr/bin/lslb pools","rediscover":false}},"tags":null,"commands":{"start":{"shell":"/bin/true","timeout":"10s","rediscover":false},"status":{"shell":"/bin/true","timeout":"10s","rediscover":false},"stop":{"shell":"/bin/true","timeout":"10s","rediscover":false}}}`
