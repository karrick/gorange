package gorange

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/karrick/goswarm"
)

type cachingClientConfig struct {
	client                  *Client
	stale                   time.Duration // prune periodicity
	expiry                  time.Duration // drop keys older than
	checkVersionPeriodicity time.Duration
}

// Client attempts to resolve range queries to a list of strings or an error,
// and stores them in an in-memory cache for quick repeated lookups.
type CachingClient struct {
	config cachingClientConfig

	cache            *goswarm.Simple
	lastRequestTimes *goswarm.Simple

	version int64

	// handle safe shutdowns
	closeError chan error
	halt       chan struct{}
}

func newCachingClient(ccc cachingClientConfig) (*CachingClient, error) {
	// NOTE: When creating a goswarm, a nil config implies treat like a
	// conventional map used for concurrent access: values never go stale, never
	// expire.
	lastRequestTimes, err := goswarm.NewSimple(nil)
	if err != nil {
		return nil, err // should not happen for nil config
	}

	if ccc.checkVersionPeriodicity > 0 {
		// When using %version, we don't need to worry about having goswarm
		// refreshing stale tuples, because values never actually go stale until
		// the version is updated.
		ccc.stale = 0
		if ccc.expiry == 0 {
			// When we do not have expiry on the data values, they will persist
			// until %version changes, so make sure one exists to prevent heap
			// gluttony.
			ccc.expiry = 4 * time.Hour
		}
	}

	// There is no point in having the underlying cache run its GC if results never
	// expire.
	var gcPeriodicity time.Duration
	if ccc.expiry > 0 {
		gcPeriodicity = ccc.expiry
	}

	// When good config, go ahead and create instance.
	badStaleDuration := 1 * time.Minute
	badExpiryDuration := 5 * time.Minute

	expandCache, err := goswarm.NewSimple(&goswarm.Config{
		GoodStaleDuration:  ccc.stale,
		GoodExpiryDuration: ccc.expiry,
		BadStaleDuration:   badStaleDuration,
		BadExpiryDuration:  badExpiryDuration,
		GCPeriodicity:      gcPeriodicity,
		Lookup: func(expression string) (interface{}, error) {
			someStrings, err := ccc.client.Query(expression)
			if err == nil {
				return someStrings, nil
			}
			if _, ok := err.(ErrRangeException); !ok {
				// Return all non-RangeException events, including http.Get
				// errors and ErrStatusNotOK errors, so will continue looking
				// for a non-error value.
				return nil, err
			}
			// ErrRangeException events are cached as bad values, so library
			// does not send the same request to other range servers.
			now := time.Now()
			tv := goswarm.TimedValue{
				Value:  nil,
				Err:    err,
				Stale:  now.Add(badStaleDuration),
				Expiry: now.Add(badExpiryDuration),
			}
			return tv, nil
		},
	})
	if err != nil {
		return nil, err
	}

	cc := &CachingClient{
		cache:            expandCache,
		closeError:       make(chan error),
		config:           ccc,
		halt:             make(chan struct{}),
		lastRequestTimes: lastRequestTimes,
	}

	go cc.run()
	return cc, nil
}

// Close releases all memory and go-routines used by the Simple swarm. If during
// instantiation, checkVersionPeriodicty was greater than the zero-value for
// time.Duration, this method may block while completing any in progress updates
// due to `%version` changes.
func (cc *CachingClient) Close() error {
	close(cc.halt)

	// Wait for run() loop to acknowledge signal that it's complete
	err := <-cc.closeError

	if cerr := cc.cache.Close(); err == nil {
		err = cerr
	}

	return err
}

// Query returns the response of the query, first checking in the TTL cache,
// then by actually sending a query to one or more of the configured range
// servers.
//
// If the response includes a RangeException header, it returns
// ErrRangeException.  If the status code is not okay, it returns
// ErrStatusNotOK.  Finally, if it cannot parse the lines in the response body,
// it returns ErrParseException.
//
//     lines, err := querier.Query("%someQuery")
//     if err != nil {
//         fmt.Fprintf(os.Stderr, "ERROR: %s", err)
//         os.Exit(1)
//     }
//     for _, line := range lines {
//         fmt.Println(line)
//     }
func (cc *CachingClient) Query(expression string) ([]string, error) {
	cc.lastRequestTimes.Store(expression, time.Now())
	someValue, err := cc.cache.Query(expression)
	if err != nil {
		return nil, err
	}
	someStrings, ok := someValue.([]string)
	if !ok {
		panic(fmt.Errorf("SHOULD NEVER FIND ANYTHING BUG []string in cache: %T", someValue))
	}
	return someStrings, nil
}

func (cc *CachingClient) lastRequestTime(key string) time.Time {
	lrt, ok := cc.lastRequestTimes.Load(key)
	if !ok {
		panic(fmt.Errorf("SHOULD NEVER FIND KEY IN cache BUT NOT IN lastRequestTimes: %q", key))
	}
	return lrt.(time.Time)
}

func (cc *CachingClient) refreshBasedOnVersion() error {
	someStrings, err := cc.config.client.Query("%version")
	if err != nil {
		return err
	}
	if len(someStrings) != 1 {
		return fmt.Errorf("%%version returned %d output lines; expected 1 line", len(someStrings))
	}
	// version is an epoch timestamp
	version, err := strconv.ParseInt(someStrings[0], 10, 64)
	if err != nil {
		return err
	}
	if version > cc.version {
		cutoff := time.Unix(version, 0).Add(-cc.config.stale)
		cc.refreshBefore(cutoff)
		cc.version = version
	}
	return nil
}

func (cc *CachingClient) refreshBefore(cutoff time.Time) {
	// To prevent overloading the range server with refresh requests for lots of
	// keys at once, trickle them in one-by-one.
	toRefresh := make(chan string, 64) // WARNING: must be at least 1 to prevent Range callback from dead locking

	var refresher sync.WaitGroup
	refresher.Add(1)
	go func() {
		defer refresher.Done()
		for key := range toRefresh {
			cc.cache.Update(key)
		}
	}()

	// Go maps and goswarm.Simple's Range method allows deleting keys while iterating over the
	// map's key-value pairs.  We'll use that to our advantage below.
	cc.cache.Range(func(key string, tv *goswarm.TimedValue) {
		if tv.Err != nil {
			// log.Printf("deleting result that is an error: %q", key)
			cc.cache.Delete(key)
		} else if cc.lastRequestTime(key).Before(cutoff) {
			// log.Printf("dropping because last requested quite a while ago: %q", key)
			cc.cache.Delete(key)
		} else {
			// log.Printf("enqueue request to update: %q", key)
			toRefresh <- key
		}
	})
	close(toRefresh)
	refresher.Wait()
}

func (cc *CachingClient) run() {
	// If param is 0, client does not want to use the feature, so make it a very
	// long periodicity, and when the select case is chosen, skip calling the
	// feature.
	checkVersionPeriodicity := cc.config.checkVersionPeriodicity
	if checkVersionPeriodicity == 0 {
		// long enough that we do not care about cycles that do a no-op
		checkVersionPeriodicity = 24 * time.Hour
	}
	stale := cc.config.stale
	if stale == 0 {
		// long enough that we do not care about cycles that do a no-op
		stale = 24 * time.Hour
	}

	for {
		select {
		case <-time.After(checkVersionPeriodicity):
			if cc.config.checkVersionPeriodicity > 0 {
				_ = cc.refreshBasedOnVersion() // ignoring error return value
			}
		case <-time.After(stale):
			if cc.config.stale > 0 {
				cutoff := time.Now().Add(-cc.config.expiry)
				cc.refreshBefore(cutoff)
			}
		case <-cc.halt:
			cc.closeError <- nil
			// there is no cleanup required, so we just return
			return
		}
	}
}
