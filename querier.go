package gorange

import (
	"fmt"
	"net/http"
	"time"

	gogetter "gopkg.in/karrick/gogetter.v1"
)

const DefaultQueryTimeout = 3 * time.Second

// Querier interface
type Querier interface {
	Query(string) ([]string, error)
}

// Configurator provides a way to list the range server addresses, and a way to override defaults
// when creating new http.Client instances.
type Configurator struct {
	Addr2Getter   func(string) gogetter.Getter // Addr2Getter converts a range server address to a Getter, ideally a customized http.Client object with a Timeout set. Leave nil to create default gogetter.Getter with DefaultQueryTimeout.
	RetryCallback func(error) bool             // RetryCallback is predicate function that tests whether query should be retried for a given error. Leave nil to retry all errors.
	RetryCount    int                          // RetryCount is number of query retries to be issued if query returns error. Leave 0 to never retry query errors.
	Servers       []string                     // Servers is slice of range server address strings. Must contain at least one string.
	TTL           time.Duration                // TTL is duration of time to cache query responses. Leave 0 to not cache responses.
}

// NewQuerier returns a new instance that sends queries to one or more range servers. The provided
// Configurator not only provides a way of listing one or more range servers, but also allows
// specification of optional retry-on-failure feature and optional TTL cache that memoizes range
// query responses.
func NewQuerier(config *Configurator) (Querier, error) {
	if len(config.Servers) == 0 {
		return nil, fmt.Errorf("cannot create Querier without at least one range server address")
	}

	addr2getter := defaultAddr2Getter
	if config.Addr2Getter != nil {
		addr2getter = config.Addr2Getter
	}

	var hg gogetter.Getter

	if len(config.Servers) == 1 {
		hg = addr2getter(config.Servers[0])
	} else {
		rr := &gogetter.RoundRobin{}
		for _, hostname := range config.Servers {
			rr.Getters = append(rr.Getters, addr2getter(hostname))
		}
		hg = rr
	}

	if config.RetryCount > 0 {
		hg = &gogetter.Retrier{
			Getter:        hg,
			RetryCallback: config.RetryCallback,
			RetryCount:    config.RetryCount,
		}
	}

	q := &Client{hg}

	if config.TTL > time.Duration(0) {
		return NewCachingClient(q, config.TTL)
	}

	return q, nil
}

func defaultAddr2Getter(addr string) gogetter.Getter {
	return &gogetter.Prefixer{
		Prefix: fmt.Sprintf("http://%s/range/list?", addr),
		Getter: &http.Client{
			// WARNING: Not having timeout will cause resource leakage if library connects to buggy range server, or a range server over a poor network connection.
			Timeout: time.Duration(DefaultQueryTimeout),

			// Transport: &http.Transport{
			// 	Dial: (&net.Dialer{
			// 		Timeout:   dialTimeout,
			// 		KeepAlive: keepAliveDuration,
			// 	}).Dial,
			// 	MaxIdleConnsPerHost: int(maxConns),
			// },
		},
	}
}