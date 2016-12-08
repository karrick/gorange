package gorange

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/karrick/gogetter"
)

// DefaultQueryTimeout is used when no Addr2Getter function is provided to control the duration a
// query will remain in flight prior to automatic cancellation.
const DefaultQueryTimeout = 30 * time.Second

// DefaultDialTimeout is used when no Addr2Getter function is provided to control the timeout for
// establishing a new connection.
const DefaultDialTimeout = 5 * time.Second

// DefaultDialKeepAlive is used when no Addr2Getter function is provided to control the keep-alive
// duration for an active connection.
const DefaultDialKeepAlive = 1 * time.Minute

// DefaultMaxIdleConnsPerHost is used when no Addr2Getter function is provided to control how many
// idle connections to keep alive per host.
const DefaultMaxIdleConnsPerHost = 1

// Querier is the interface implemented by an object that allows key-value lookups, where keys are
// strings and values are slices of strings.
type Querier interface {
	Query(string) ([]string, error)
}

// Configurator provides a way to list the range server addresses, and a way to override defaults
// when creating new http.Client instances.
type Configurator struct {
	// Addr2Getter converts a range server address to a Getter, ideally a customized http.Client
	// object with a Timeout set. Leave nil to create default gogetter.Getter with
	// DefaultQueryTimeout.
	Addr2Getter func(string) gogetter.Getter

	// RetryCallback is predicate function that tests whether query should be retried for a
	// given error. Leave nil to retry all errors.
	RetryCallback func(error) bool

	// RetryCount is number of query retries to be issued if query returns error. Leave 0 to
	// never retry query errors.
	RetryCount int

	// RetryPause is the amount of time to wait before retrying the query with the underlying
	// Getter.
	RetryPause time.Duration

	// Servers is slice of range server address strings. Must contain at least one string.
	Servers []string

	// TTL is duration of time to cache query responses. Leave 0 to not cache responses.
	TTL time.Duration
}

// NewQuerier returns a new instance that sends queries to one or more range servers. The provided
// Configurator not only provides a way of listing one or more range servers, but also allows
// specification of optional retry-on-failure feature and optional TTL cache that memoizes range
// query responses.
//
//    func main() {
//		servers := []string{"range1.example.com", "range2.example.com", "range3.example.com"}
//
//		config := &gorange.Configurator{
//			RetryCount:    len(servers),
//			RetryPause:    5 * time.Second,
//			Servers:       servers,
//			TTL:           5 * time.Minute,
//		}
//
//		// create a range querier; could list additional servers or include other options as well
//		querier, err := gorange.NewQuerier(config)
//		if err != nil {
//			fmt.Fprintf(os.Stderr, "%s", err)
//			os.Exit(1)
//		}
//    }
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
		var hostGetters []gogetter.Getter
		for _, hostname := range config.Servers {
			hostGetters = append(hostGetters, addr2getter(hostname))
		}
		hg = gogetter.NewRoundRobin(hostGetters)
	}

	if config.RetryCount > 0 {
		hg = &gogetter.Retrier{
			Getter:        hg,
			RetryCallback: config.RetryCallback,
			RetryCount:    config.RetryCount,
			RetryPause:    config.RetryPause,
		}
	}

	q := &Client{hg}

	if config.TTL > 0 {
		return newCachingClient(q, config.TTL)
	}

	return q, nil
}

func defaultAddr2Getter(addr string) gogetter.Getter {
	return &gogetter.Prefixer{
		Prefix: fmt.Sprintf("http://%s/range/list?", addr),
		Getter: &http.Client{
			// WARNING: Using http.Client instance without a Timeout will cause resource
			// leaks and may render your program inoperative if the client connects to a
			// buggy range server, or over a poor network connection.
			Timeout: time.Duration(DefaultQueryTimeout),

			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout:   DefaultDialTimeout,
					KeepAlive: DefaultDialKeepAlive,
				}).Dial,
				MaxIdleConnsPerHost: int(DefaultMaxIdleConnsPerHost),
			},
		},
	}
}
