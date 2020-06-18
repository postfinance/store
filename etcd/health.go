package etcd

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
)

const dfltTimeout = 10 * time.Second

// Status represents the status of the store
type Status struct {
	Endpoint string `json:"endpoint"`
	Healthy  bool   `json:"healthy"`
	Detail   string `json:"detail"`
	cli      *clientv3.Client
}

// Health returns the status of the store
// etcd is missing some meaningful documentation
func (e *Backend) Health(key string) []Status {
	endpoints, err := e.endpointsFromCluster()
	if err != nil {
		return []Status{{
			Healthy: false,
			Detail:  "could not get members",
		}}
	}

	status := []Status{}

	for _, ep := range endpoints {
		stat := Status{
			Endpoint: ep,
			Healthy:  false,
		}

		if e.config != nil {
			cfg := *e.config

			cli, err := clientv3.New(cfg)
			if err != nil {
				stat.Detail = fmt.Sprintf("%s is unhealthy: failed to connect: %v", stat.Endpoint, err)
				continue
			}

			stat.cli = cli
		} else {
			cli := e.client
			cli.SetEndpoints(ep)
			stat.cli = cli
		}

		status = append(status, stat)
	}

	var wg sync.WaitGroup

	for i := range status {
		if status[i].cli == nil {
			continue
		}

		wg.Add(1)

		go func(s *Status) {
			defer wg.Done()

			start := time.Now()
			// get a random key. As long as we can get the response without an error, the
			// endpoint is health.
			timeout := dfltTimeout
			if e.RequestTimeout > 0 {
				timeout = e.RequestTimeout
			}

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			_, nerr := s.cli.Get(ctx, key)
			if nerr != nil {
				s.Detail = fmt.Sprintf("%s is unhealthy: failed commit proposal: %v", s.Endpoint, nerr)
			} else {
				s.Healthy = true
				s.Detail = fmt.Sprintf("%s is healthy: successfully committed proposal: took = %v", s.Endpoint, time.Since(start))
			}
		}(&status[i])
	}

	wg.Wait()

	return status
}

func (e *Backend) endpointsFromCluster() ([]string, error) {
	if len(e.endpoints) > 0 {
		return e.endpoints, nil
	}

	timeout := dfltTimeout
	if e.RequestTimeout > 0 {
		timeout = e.RequestTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	membs, err := e.client.MemberList(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch endpoints from etcd cluster member list: %v", err)
	}

	members := []string{}
	for _, m := range membs.Members {
		members = append(members, m.ClientURLs...)
	}

	return members, nil
}
