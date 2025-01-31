package etcd

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

func TestOptions(t *testing.T) {
	exp := &Backend{
		client: &clientv3.Client{},
	}
	opts := []Opt{
		WithClient(exp.client),
	}
	actual := Backend{}

	for _, opt := range opts {
		_ = opt(&actual)
	}

	assert.EqualValues(t, exp, &actual)
	exp = &Backend{
		RequestTimeout: 1,
		endpoints:      []string{"e1", "e2"},
		user:           "user",
		password:       "password",
		ctx:            context.TODO(),
		ssl: ssl{
			key:  []byte(key),
			cert: []byte(cert),
			ca:   []byte(ca),
		},
		autoSyncInterval:     2,
		dialTimeout:          3,
		dialKeepAliveTimeout: 2 * time.Second,
		dialKeepAliveTime:    3 * time.Second,
	}
	opts = []Opt{
		WithRequestTimeout(exp.RequestTimeout),
		WithEndpoints(exp.endpoints),
		WithUsername(exp.user),
		WithPassword(exp.password),
		WithKey(string(exp.ssl.key)),
		WithCert(string(exp.ssl.cert)),
		WithCA(string(exp.ssl.ca)),
		WithAutoSyncInterval(exp.autoSyncInterval),
		WithDialTimeout(exp.dialTimeout),
		WithDialKeepAliveTime(exp.dialKeepAliveTime),
		WithDialKeepAliveTimeout(exp.dialKeepAliveTimeout),
		WithContext(exp.ctx),
	}

	actual = Backend{}

	for _, opt := range opts {
		_ = opt(&actual)
	}

	assert.EqualValues(t, exp, &actual)
}

func TestErrorhandler(t *testing.T) {
	testValue := ""
	e := Backend{}
	err := WithErrorHandler(func(err error) error {
		testValue = "test"
		return err
	})(&e)
	require.NoError(t, err)

	nerr := errors.New("dummy error")
	err = e.errHandler(nerr)
	require.Equal(t, nerr, err)
	assert.True(t, len(testValue) > 0)
}

func TestDialOptions(t *testing.T) {
	e := Backend{}
	err := WithDialOptions(grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024 * 1024 * 8)))(&e)
	require.NoError(t, err)
	assert.Len(t, e.dialOptions, 1)
}

func TestTLSConfig(t *testing.T) {
	s := ssl{
		key:  []byte(key),
		cert: []byte(cert),
		ca:   []byte(ca),
	}
	tlscfg, err := s.tlsConfig()
	assert.NoError(t, err)
	assert.NotNil(t, tlscfg)
}

func TestDuplicateTLSConfig(t *testing.T) {
	sslData := map[string]string{
		"/tmp/key.pem":  key,
		"/tmp/cert.pem": cert,
		"/tmp/ca.pem":   ca,
	}

	for f, data := range sslData {
		err := os.WriteFile(f, []byte(data), 0o644) //nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)
		require.NoError(t, err)
	}

	e := Backend{}
	e.ssl.key = []byte(key)
	e.ssl.ca = []byte(ca)
	e.ssl.cert = []byte(cert)
	tt := []struct {
		name string
		opt  Opt
	}{
		{
			"duplicate ca file",
			WithCAFile("/tmp/ca.pem"),
		},
		{
			"duplicate cert file",
			WithCertFile("/tmp/cert.pem"),
		},
		{
			"duplicate key file",
			WithKeyFile("/tmp/key.pem"),
		},
		{
			"duplicate ca data",
			WithCA(ca),
		},
		{
			"duplicate cert data",
			WithCert(cert),
		},
		{
			"duplicate key data",
			WithKey(key),
		},
	}

	for _, tc := range tt {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := tc.opt(&e)
			require.Error(t, err)
		})
	}
}

//nolint:gochecknoglobals
var key = `-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEA1iXGN20mStbIoGMf12nOutZzDNZJa2t2B87IC4ZQp/9PLLKt
n5aaBRkaDGV7GGW3WhWKaubjODjC7zwaiKJ75wDeQzE126AXlVNRabbiTk3kzEhe
e8VX63bKj2AYjQ1aaljlMJdEeA/fmIJrSQUEoObpjiAMV6clv5vlqm814aSkJw8L
EMlneapA96IJx/OqkGGgOxRDz77ZU/tJY4466zujY9CmpyEgue2nU8zcouqfuIvI
Qszrop/yCIRchmqQyd6Xj/MV/dqz6GYyR7E3IOKoL4wIUb6Ma3vZ/K9b8j39bFgh
yRU2SnZGT3ykSoGwGIOByy5dtE0NilObZK2aGwIDAQABAoIBABRiGBHig8iP1Rzf
EoLQgWrrSpwoMWjWG6/2kLf4GulCteiNQVV1Ykel8gLIDgVueRtL/ZbbTzlkvblD
wUrMHHsKbB19K6EyBcyi15b2X2gQpfyDzup9CDdTYUhwXyXd5YNF3trXKx86c4/y
qf+TSLOABy8OAd69/CvWzO+CEmDdvF3jQtoqFWFHL6H1u+fzUaED7FTRPminQibW
NIg5oCpLSL25KQO7RLeiUQotATXEQ87munap8jKsuKQguchS/RhosLC++yXCkDF0
PiBwAJdEPZYkpNYDbTlI98NXrapyNBwHf8uCFdJ+rtCeG0o7m4t+YUyjiOB3X2Tn
Y5FOW/ECgYEA8/SUHFzywvQ+HKLT5xel5Xj7KSI1OtSiOEA+zmj9Qa5kEXZDzXpL
xfN5CtGCEMHadyz5gSvApH7bYdm/gGk0s63FKmmYNSdRkEhxLVVlhhlohdQJiKQF
koQ+PsOMZjljmw2zw43MKFFsuiqXicumgRgusPadfIFv3dBcNp8zOWcCgYEA4Lhy
M6vWqU7RTmPZ9c4hOBGCUPuWQ5apmv/b3CQXUY5zJYtOr3FwTHTo+EBhzac4T9AA
iLHpXAtFXvu+IhVMn36E5yPn+VlME0pqFEReMufs1/q7F5PGd9z5HFGJWe8HlhGr
WhYb8HuvIFKoCqQAIKyyhAvFGdUKAatoERsWhS0CgYEAjhlixHqntUNLgqadw7gU
m+uycK1KUDBXJWjWrKifDEkmZL18lQ2tYWqGkPSkTFp+hHigBMuVdLun0gFh+MxP
NHH9rMzBKgTzD5YCxqM9dbULGxj9TKgtzsBU+X4yI8E98a0zjq7jwoBbUr1Ic606
GWwall2wMju7C3s4m2B0F/UCgYAljo3/lsOMjlCq4kNGMooYvcxiznlhmGeGMSbE
lv+SsNULvEmNRs825/8bpl7yKPVfWWsNbXaL3JJeeoJfXeHtcg6yNq0JXmQ5gSFT
e82mgl5yBkSIfzkXRUYY9oOXFTzWVuOlbbaDrZgqNcZB6QDgdRxsty7cz18ZwlTl
LvrHjQKBgAnIfTrZJ9FFV0vi+RNXfbaHHCHmRvTWNQ+r++kCZc/cqXw5kt50PVG2
7p5BReuPsXM0rylghE737eMolagv1UeZ/KH+lqT3QbgzWO6TpNDG9zBCITRkvj01
mt+dewOupOGZliD14TVBaKMjc2dcKtMXOXyzMS5KbdDPmD9rzHGX
-----END RSA PRIVATE KEY-----
`

//nolint:gochecknoglobals
var cert = `-----BEGIN CERTIFICATE-----
MIIDBzCCAe+gAwIBAgIJAIGng0yg24ZTMA0GCSqGSIb3DQEBBQUAMBoxGDAWBgNV
BAMMD3d3dy5leGFtcGxlLmNvbTAeFw0xNzExMTQxMTUzMDJaFw0yNzExMTIxMTUz
MDJaMBoxGDAWBgNVBAMMD3d3dy5leGFtcGxlLmNvbTCCASIwDQYJKoZIhvcNAQEB
BQADggEPADCCAQoCggEBANYlxjdtJkrWyKBjH9dpzrrWcwzWSWtrdgfOyAuGUKf/
TyyyrZ+WmgUZGgxlexhlt1oVimrm4zg4wu88Goiie+cA3kMxNdugF5VTUWm24k5N
5MxIXnvFV+t2yo9gGI0NWmpY5TCXRHgP35iCa0kFBKDm6Y4gDFenJb+b5apvNeGk
pCcPCxDJZ3mqQPeiCcfzqpBhoDsUQ8++2VP7SWOOOus7o2PQpqchILntp1PM3KLq
n7iLyELM66Kf8giEXIZqkMnel4/zFf3as+hmMkexNyDiqC+MCFG+jGt72fyvW/I9
/WxYIckVNkp2Rk98pEqBsBiDgcsuXbRNDYpTm2StmhsCAwEAAaNQME4wHQYDVR0O
BBYEFEZs7PL3kVUtnNhkudTTDCFvN6O8MB8GA1UdIwQYMBaAFEZs7PL3kVUtnNhk
udTTDCFvN6O8MAwGA1UdEwQFMAMBAf8wDQYJKoZIhvcNAQEFBQADggEBAEITtCJ6
yxkhX8Zsx1G2ytNNvs5MDJV1RZc6HhFk69i+JAs2toUX0zmo68Ro75myxHcXoRBI
+Y4VcjYsRscDkNbilFJWRrREbuRtnVHvw2aq2Y37te4Ov6Yw+slu4DYNe3rwf0hU
9vP+edFrtWqkCAgaUFE2Oeka/XKsD732XxlUZvDYNywUlPUt70PKMGnmkrG1e1+p
7R1X2saqBhcJqtfbzxlM1s8K386YNLTNGl3LCq6WU1bNfXtjZSPiwJGi9U4J75JE
mUN2TfeN/X+LK3LRCe4h7hJ8MfH8Gv8lcgnQvwjfR5c7lvD9vb5zQP733hhcBAKE
0s/mQN3KLohh5uE=
-----END CERTIFICATE-----
`

//nolint:gochecknoglobals
var ca = `-----BEGIN CERTIFICATE-----
MIICPDCCAaUCEDyRMcsf9tAbDpq40ES/Er4wDQYJKoZIhvcNAQEFBQAwXzELMAkG
A1UEBhMCVVMxFzAVBgNVBAoTDlZlcmlTaWduLCBJbmMuMTcwNQYDVQQLEy5DbGFz
cyAzIFB1YmxpYyBQcmltYXJ5IENlcnRpZmljYXRpb24gQXV0aG9yaXR5MB4XDTk2
MDEyOTAwMDAwMFoXDTI4MDgwMjIzNTk1OVowXzELMAkGA1UEBhMCVVMxFzAVBgNV
BAoTDlZlcmlTaWduLCBJbmMuMTcwNQYDVQQLEy5DbGFzcyAzIFB1YmxpYyBQcmlt
YXJ5IENlcnRpZmljYXRpb24gQXV0aG9yaXR5MIGfMA0GCSqGSIb3DQEBAQUAA4GN
ADCBiQKBgQDJXFme8huKARS0EN8EQNvjV69qRUCPhAwL0TPZ2RHP7gJYHyX3KqhE
BarsAx94f56TuZoAqiN91qyFomNFx3InzPRMxnVx0jnvT0Lwdd8KkMaOIG+YD/is
I19wKTakyYbnsZogy1Olhec9vn2a/iRFM9x2Fe0PonFkTGUugWhFpwIDAQABMA0G
CSqGSIb3DQEBBQUAA4GBABByUqkFFBkyCEHwxWsKzH4PIRnN5GfcX6kb5sroc50i
2JhucwNhkcV8sEVAbkSdjbCxlnRhLQ2pRdKkkirWmnWXbj9T/UWZYB2oK0z5XqcJ
2HUw19JlYD1n1khVdWk/kfVIC0dpImmClr7JyDiGSnoscxlIaU5rfGW/D/xwzoiQ
-----END CERTIFICATE-----
`
