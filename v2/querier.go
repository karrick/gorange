package gorange

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	gogetter "github.com/karrick/gogetter/v2"
)

// DefaultQueryTimeout is used when no Addr2Getter function is provided to control the duration a
// query will remain in flight prior to automatic cancellation.
const DefaultQueryTimeout = 30 * time.Second

// DefaultDialTimeout is used when no Addr2Getter function is provided to control the timeout for
// establishing a new connection.
const DefaultDialTimeout = 5 * time.Second

// DefaultDialKeepAlive is used when no Addr2Getter function is provided to control the keep-alive
// duration for an active connection.
const DefaultDialKeepAlive = 30 * time.Second

// DefaultMaxIdleConnsPerHost is used when no Addr2Getter function is provided to control how many
// idle connections to keep alive per host.
const DefaultMaxIdleConnsPerHost = 1

// Querier is the interface implemented by an object that allows key-value lookups, where keys are
// strings and values are slices of strings.
type Querier interface {
	Close() error
	Expand(string) (string, error)
	List(string) ([]string, error)
	Query(string) ([]string, error)
	Raw(string) (io.ReadCloser, error)
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

	if config.CheckVersionPeriodicity == 0 && config.TTE == 0 && config.TTL == 0 {
		return q, nil
	}

	expiry := config.TTE
	stale := config.TTL

	// If using %version, we don't need to worry about having goswarm refreshing stale
	// tuples, because values never actually go stale until the version is updated.
	if config.CheckVersionPeriodicity > 0 {
		stale = 0

		// Unless we have expiry on the data values, they will persist until %version
		// changes, so make sure one exists to prevent heap gluttony.
		if expiry == 0 {
			expiry = 4 * time.Hour
		}
	}

	// There is no point in having the underlying cache run its GC if results never
	// expire.
	var gcPeriodicity time.Duration
	if expiry > 0 {
		gcPeriodicity = expiry
	}

	return newCachingClient(&cachingClientConfig{
		querier:                 q,
		stale:                   stale,
		expiry:                  expiry,
		checkVersionPeriodicity: config.CheckVersionPeriodicity,
		gcPeriodicity:           gcPeriodicity,
	})
}

func defaultAddr2Getter(addr string) gogetter.Getter {
	return &gogetter.Prefixer{
		Prefix: fmt.Sprintf("http://%s", addr),
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

//
// Some utility functions for the default method of whether or not a query with an error result
// ought to be retried.
//

type temporary interface {
	Temporary() bool
}

type timeout interface {
	Timeout() bool
}

func isTemporary(err error) bool {
	t, ok := err.(temporary)
	return ok && t.Temporary()
}

func isTimeout(err error) bool {
	t, ok := err.(timeout)
	return ok && t.Timeout()
}

func makeRetryCallback(count int) func(error) bool {
	return func(err error) bool {
		// Because some DNSError errors can be temporary or timeout, most efficient to check
		// whether those conditions are true first.
		if isTemporary(err) || isTimeout(err) {
			return true
		}
		// And if error is neither temporary nor a timeout, then it might still be retryable
		// if it's a DNSError and there are more than one servers configured to proxy for.
		if urlError, ok := err.(*url.Error); ok {
			if netOpError, ok := urlError.Err.(*net.OpError); ok {
				if _, ok = netOpError.Err.(*net.DNSError); ok {
					// "no such host": This query may be retried either if there
					// are more servers in the list of servers, or if the DNS
					// lookup resulted in a timeout.
					return count > 1
				}
			}
		}
		return false
	}
}
