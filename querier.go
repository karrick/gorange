package gorange

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
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
	Close() error
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

	// TTL is duration of time to cache query responses. Leave 0 to not cache responses.  When a
	// value is older than its TTL, it becomes stale.  When a key is queried for a value that is
	// stale, an asynchronous routine attempts to lookup the new value, while the existing value
	// is immediately returned to the user.  TTL, TTE, and CheckVersionPeriodicity work together
	// to prevent frequently needlessly asking servers for information that is still current
	// while preventing heap build-up on clients.
	TTL time.Duration

	// TTE is duration of time before cached response is no longer able to be served, even if
	// attempts to fetch new value repeatedly fail.  This value should be large if your
	// application needs to still operate even when range servers are down.  A zero-value for
	// this implies that values never expire and can continue to be served.  TTL, TTE, and
	// CheckVersionPeriodicity work together to prevent frequently needlessly asking servers for
	// information that is still current while preventing heap build-up on clients.
	TTE time.Duration

	// CheckVersionPeriodicity is the amount of time between checking the range `%version`
	// key. If your range server returns the epoch seconds of the time the data set became
	// active when given the `%version` query, using this option is much better than using just
	// TTL and TTE.  After the specified period of time the CachingClient will query the range
	// server's `%version` key, and if greater than the value discovered during the previous
	// check, schedules an asynchronous refresh of all keys last requested by the client less
	// than the amount of time specified by the TTL from the new version epoch. In other words,
	// say key A was last requested at time 300, and key B was last requested at time 360. If
	// the version comes back as 400, and the TTL is 60, then key A will be deleted and B will
	// be refreshed.  It makes no sense for CheckVersionPeriodicity to be a non-zero value when
	// TTL and TTE are both zero-values.
	CheckVersionPeriodicity time.Duration
}

// NewQuerier returns a new instance that sends queries to one or more range servers. The provided
// Configurator not only provides a way of listing one or more range servers, but also allows
// specification of optional retry-on-failure feature and optional TTL cache that memoizes range
// query responses.
//
//	func main() {
//		servers := []string{"range1.example.com", "range2.example.com", "range3.example.com"}
//
//		config := &gorange.Configurator{
//			RetryCount:              len(servers),
//			RetryPause:              5 * time.Second,
//			Servers:                 servers,
//			CheckVersionPeriodicity: 15 * time.Second,
//			TTL:                     30 * time.Second,
//			TTE:                     15 * time.Minute,
//		}
//
//		// create a range querier; could list additional servers or include other options as well
//		querier, err := gorange.NewQuerier(config)
//		if err != nil {
//			fmt.Fprintf(os.Stderr, "%s", err)
//			os.Exit(1)
//		}
//		// must invoke Close method when finished using to prevent resource leakage
//		defer func() { _ = querier.Close() }()
//	}
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
		if config.RetryCallback == nil {
			config.RetryCallback = makeRetryCallback(len(config.Servers))
		}
		hg = &gogetter.Retrier{
			Getter:        hg,
			RetryCallback: config.RetryCallback,
			RetryCount:    config.RetryCount,
			RetryPause:    config.RetryPause,
		}
	}

	q := &Client{hg}

	if config.TTL > 0 || config.TTE > 0 || config.CheckVersionPeriodicity > 0 {
		// There is no point in having the underlying cache run its GC if results never go
		// stale.
		var gcPeriodicity time.Duration
		if config.TTE > 0 {
			gcPeriodicity = time.Hour
		}

		return newCachingClient(&cachingClientConfig{
			querier:                 q,
			stale:                   config.TTL, // 5 * time.Minute,
			expiry:                  config.TTE, // 24 * time.Hour,
			checkVersionPeriodicity: config.CheckVersionPeriodicity,
			gcPeriodicity:           gcPeriodicity,
		})
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

func makeRetryCallback(count int) func(error) bool {
	return func(err error) bool {
		switch err1 := err.(type) {
		case net.Error:
			if err1.Temporary() || err1.Timeout() {
				return true
			}
			switch err2 := err.(type) {
			case *url.Error:
				switch err3 := err2.Err.(type) {
				case *net.OpError:
					switch err3.Err.(type) {
					case *net.DNSError:
						return count > 1
					}
				}
			}
			return false
		}
		return false
	}
}
