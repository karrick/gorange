package gorange

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/karrick/gorill"
	"github.com/karrick/goswarm"
)

// CachingClient memoizes responses from a Querier.
type CachingClient struct {
	config cachingClientConfig

	expandCache            *goswarm.Simple
	expandLastRequestTimes *goswarm.Simple

	listCache            *goswarm.Simple
	listLastRequestTimes *goswarm.Simple

	rawCache            *goswarm.Simple
	rawLastRequestTimes *goswarm.Simple

	version int64

	// handle safe shutdowns
	closeError chan error
	halt       chan struct{}
}

type cachingClientConfig struct {
	querier Querier
	stale   time.Duration // prune periodicity
	expiry  time.Duration // drop keys older than

	// when non-zero, check %version and drop expired items or refresh stale items
	checkVersionPeriodicity time.Duration

	// when non-zero, periodically garbage collect expired items
	gcPeriodicity time.Duration
}

// NewCachingClient returns CachingClient that attempts to respond to Query methods by consulting
// its TTL cache, then directing the call to the underlying Querier if a valid response is not
// stored.
func newCachingClient(config *cachingClientConfig) (*CachingClient, error) {
	if config == nil {
		config = &cachingClientConfig{}
	}
	if config.querier == nil {
		return nil, fmt.Errorf("cannot create CachingClient without querier")
	}
	if config.stale < 0 {
		return nil, fmt.Errorf("cannot create CachingClient with negative stale duration: %v", config.stale)
	}
	if config.expiry < 0 {
		return nil, fmt.Errorf("cannot create CachingClient with negative expiry duration: %v", config.expiry)
	}
	if config.checkVersionPeriodicity < 0 {
		return nil, fmt.Errorf("cannot create CachingClient with negative checkVersionPeriodicity duration: %v", config.checkVersionPeriodicity)
	}
	if config.gcPeriodicity < 0 {
		return nil, fmt.Errorf("cannot create CachingClient with negative gcPeriodicity duration: %v", config.gcPeriodicity)
	}
	// when good config, go ahead and create instance
	badStaleDuration := 1 * time.Minute
	badExpiryDuration := 5 * time.Minute

	// nil config implies treat like a conventional map used for concurrent access: values never go stale, never expire
	expandLastRequestTimes, err := goswarm.NewSimple(nil)
	if err != nil {
		return nil, err
	}
	listLastRequestTimes, err := goswarm.NewSimple(nil)
	if err != nil {
		return nil, err
	}
	rawLastRequestTimes, err := goswarm.NewSimple(nil)
	if err != nil {
		return nil, err
	}

	expandConfig := goswarm.Config{
		GoodStaleDuration:  config.stale,
		GoodExpiryDuration: config.expiry,
		BadStaleDuration:   badStaleDuration,
		BadExpiryDuration:  badExpiryDuration,
		GCPeriodicity:      config.gcPeriodicity,
		Lookup: func(url string) (interface{}, error) {
			results, err := config.querier.Expand(url)
			// Check for nil before type check because it's faster, and it's the common case.
			if err == nil {
				return results, nil
			}
			if _, ok := err.(ErrRangeException); ok {
				// ErrRangeException events are cached as bad values, so library
				// does not send the same request to other range servers.
				now := time.Now()
				return goswarm.TimedValue{
					Value:  nil,
					Err:    err,
					Stale:  now.Add(badStaleDuration),
					Expiry: now.Add(badExpiryDuration),
				}, nil
			}
			// Return all non-RangeException events, including http.Get errors
			// and ErrStatusNotOK errors, so RoundRobin will continue looking
			// for a non-error value.
			return nil, err
		},
	}

	expandCache, err := goswarm.NewSimple(&expandConfig)
	if err != nil {
		return nil, err
	}

	rawConfig := expandConfig
	rawConfig.Lookup = func(url string) (interface{}, error) {
		iorc, err := config.querier.Raw(url)
		// Check for nil before type check because it's faster, and it's the common case.
		if err == nil {
			// We have been given an io.ReadCloser that contains the data to be
			// read and stored in the cache.
			buf, err := ioutil.ReadAll(iorc)
			if err2 := iorc.Close(); err == nil {
				err = err2
			}
			return buf, err
		}
		if _, ok := err.(ErrRangeException); ok {
			now := time.Now()
			// RangeException events are cached as bad values, so library
			// does not send the same request to other range servers.
			return goswarm.TimedValue{
				Value:  nil,
				Err:    err,
				Stale:  now.Add(badStaleDuration),
				Expiry: now.Add(badExpiryDuration),
			}, nil
		}
		// Return all non-RangeException events, including http.Get errors
		// and ErrStatusNotOK errors, so RoundRobin will continue looking
		// for a non-error value.
		return nil, err
	}

	rawCache, err := goswarm.NewSimple(&rawConfig)
	if err != nil {
		return nil, err
	}

	c := &CachingClient{
		config:                 *config,
		expandCache:            expandCache,
		expandLastRequestTimes: expandLastRequestTimes,
		listLastRequestTimes:   listLastRequestTimes,
		rawCache:               rawCache,
		rawLastRequestTimes:    rawLastRequestTimes,
	}

	listConfig := expandConfig
	listConfig.Lookup = func(url string) (interface{}, error) {
		iorc, err := c.Raw(url)
		// Check for nil before type check because it's faster, and it's the common case.
		if err == nil {
			// NOTE: The CachingClient.Raw method returns a bytes buffer with a
			// NOP closer, so we do not need to read and close it.
			var lines []string
			scanner := bufio.NewScanner(iorc)
			for scanner.Scan() {
				lines = append(lines, strings.TrimSpace(scanner.Text()))
			}
			if err = scanner.Err(); err != nil {
				return nil, ErrParseException{err}
			}
			return lines, nil
		}
		if _, ok := err.(ErrRangeException); ok {
			now := time.Now()
			// RangeException events are cached as bad values, so library
			// does not send the same request to other range servers.
			return goswarm.TimedValue{
				Value:  nil,
				Err:    err,
				Stale:  now.Add(badStaleDuration),
				Expiry: now.Add(badExpiryDuration),
			}, nil
		}
		// Return all non-RangeException events, including http.Get errors
		// and ErrStatusNotOK errors, so RoundRobin will continue looking
		// for a non-error value.
		return nil, err
	}

	listCache, err := goswarm.NewSimple(&listConfig)
	if err != nil {
		return nil, err
	}
	c.listCache = listCache

	c.halt = make(chan struct{})
	c.closeError = make(chan error)
	go c.run()
	return c, nil
}

// Close releases all memory and go-routines used by the Simple swarm. If during instantiation,
// checkVersionPeriodicty was greater than the zero-value for time.Duration, this method may block
// while completing any in progress updates due to `%version` changes.
func (c *CachingClient) Close() error {
	close(c.halt)

	// Wait for run() loop to acknowledge signal that it's complete
	err := <-c.closeError

	if cerr := c.expandCache.Close(); err == nil {
		err = cerr
	}
	if cerr := c.listCache.Close(); err == nil {
		err = cerr
	}
	if cerr := c.rawCache.Close(); err == nil {
		err = cerr
	}

	// TODO: It would be nice to return all errors rather than first one.
	return err
}

// Expand returns the response of the query, first checking in the TTL cache, then by actually
// invoking the Expand method on the underlying Querier.
func (c *CachingClient) Expand(query string) (string, error) {
	c.expandLastRequestTimes.Store(query, time.Now())
	raw, err := c.expandCache.Query(query)
	if err != nil {
		return "", err
	}
	result, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("cannot convert %T to string", raw)
	}
	return result, nil
}

// List returns the response of the query, first checking in the TTL cache, then by actually
// invoking the List method on the underlying Querier.
func (c *CachingClient) List(query string) ([]string, error) {
	c.listLastRequestTimes.Store(query, time.Now())
	raw, err := c.listCache.Query(query)
	if err != nil {
		return nil, err
	}
	results, ok := raw.([]string)
	if !ok {
		return nil, fmt.Errorf("cannot convert %T to []string", raw)
	}
	return results, nil
}

// Query returns the response of the query, first checking in the TTL cache, then by actually
// invoking the Query method on the underlying Querier.
func (c *CachingClient) Query(query string) ([]string, error) {
	return c.List(query)
}

// Raw sends the range request and checks for invalid responses from
// downstream. If the response is valid, this returns the response body as an
// io.ReadCloser for the client to use. It is the client's responsibility to
// invoke the Close method on the returned io.ReadCloser.
func (c *CachingClient) Raw(query string) (io.ReadCloser, error) {
	c.rawLastRequestTimes.Store(query, time.Now())
	raw, err := c.rawCache.Query(query)
	if err != nil {
		return nil, err
	}
	results, ok := raw.([]byte)
	if !ok {
		return nil, fmt.Errorf("cannot convert %T to []byte", raw)
	}
	return gorill.NopCloseReader(bytes.NewBuffer(results)), nil
}

func (c CachingClient) getListLastRequestTime(key string) time.Time {
	lrt, ok := c.listLastRequestTimes.Load(key)
	if !ok {
		panic(fmt.Errorf("SHOULD NEVER FIND KEY IN cache BUT NOT IN listLastResponseTimes: %q", key))
	}
	return lrt.(time.Time)
}

func (c CachingClient) getExpandLastRequestTime(key string) time.Time {
	lrt, ok := c.expandLastRequestTimes.Load(key)
	if !ok {
		panic(fmt.Errorf("SHOULD NEVER FIND KEY IN cache BUT NOT IN expandLastResponseTimes: %q", key))
	}
	return lrt.(time.Time)
}

func (c *CachingClient) refreshBasedOnVersion() error {
	results, err := c.config.querier.List("%version")
	if err != nil {
		return err
	}
	if len(results) != 1 {
		return fmt.Errorf("%%version returned %d output lines; expected 1 line", len(results))
	}
	// version is an epoch timestamp
	version, err := strconv.ParseInt(results[0], 10, 64)
	if err != nil {
		return err
	}
	if version > c.version {
		cutoff := time.Unix(version, 0).Add(-c.config.stale)
		c.expandRefreshBefore(cutoff)
		c.listRefreshBefore(cutoff)
		c.version = version
	}
	return nil
}

func (c *CachingClient) expandRefreshBefore(cutoff time.Time) {
	// log.Printf("refreshBefore(%d)", cutoff.Unix())

	// To prevent overloading the range server with refresh requests for lots of keys at once,
	// trickle them in one-by-one.
	toRefresh := make(chan string, 64) // WARNING: must be at least 1 to prevent Range callback from dead locking
	var refresher sync.WaitGroup
	refresher.Add(1)
	go func() {
		for key := range toRefresh {
			c.expandCache.Update(key)
		}
		refresher.Done()
	}()

	// Go maps and goswarm.Simple's Range method allows deleting keys while iterating over the
	// map's key-value pairs. We'll use that to our advantage below.
	c.expandCache.Range(func(key string, tv *goswarm.TimedValue) {
		if tv.Err != nil {
			// log.Printf("deleting result that is an error: %q", key)
			c.expandCache.Delete(key)
		} else if c.getExpandLastRequestTime(key).Before(cutoff) {
			// log.Printf("dropping because last requested quite a while ago: %q", key)
			c.expandCache.Delete(key)
		} else {
			// log.Printf("enqueue request to update: %q", key)
			toRefresh <- key
		}
	})
	close(toRefresh)
	refresher.Wait()
}

func (c *CachingClient) listRefreshBefore(cutoff time.Time) {
	// log.Printf("refreshBefore(%d)", cutoff.Unix())

	// To prevent overloading the range server with refresh requests for lots of keys at once,
	// trickle them in one-by-one.
	toRefresh := make(chan string, 64) // WARNING: must be at least 1 to prevent Range callback from dead locking
	var refresher sync.WaitGroup
	refresher.Add(1)
	go func() {
		for key := range toRefresh {
			c.listCache.Update(key)
		}
		refresher.Done()
	}()

	// Go maps and goswarm.Simple's Range method allows deleting keys while iterating over the
	// map's key-value pairs. We'll use that to our advantage below.
	c.listCache.Range(func(key string, tv *goswarm.TimedValue) {
		if tv.Err != nil {
			// log.Printf("deleting result that is an error: %q", key)
			c.listCache.Delete(key)
		} else if c.getListLastRequestTime(key).Before(cutoff) {
			// log.Printf("dropping because last requested quite a while ago: %q", key)
			c.listCache.Delete(key)
		} else {
			// log.Printf("enqueue request to update: %q", key)
			toRefresh <- key
		}
	})
	close(toRefresh)
	refresher.Wait()
}

func (c *CachingClient) run() {
	// If param is 0, client does not want to use the feature, so make it a very long
	// periodicity, and when the select case is chosen, skip calling the feature.
	checkVersionPeriodicity := c.config.checkVersionPeriodicity
	if checkVersionPeriodicity == 0 {
		checkVersionPeriodicity = 24 * time.Hour
	}
	stale := c.config.stale
	if stale == 0 {
		stale = 24 * time.Hour
	}

	for {
		select {
		case <-time.After(checkVersionPeriodicity):
			if c.config.checkVersionPeriodicity > 0 {
				_ = c.refreshBasedOnVersion() // ignoring error return value
			}
		case <-time.After(stale):
			if c.config.stale > 0 {
				cutoff := time.Now().Add(-c.config.expiry)
				c.expandRefreshBefore(cutoff)
				c.listRefreshBefore(cutoff)
			}
		case <-c.halt:
			c.closeError <- nil
			// there is no cleanup required, so we just return
			return
		}
	}
}
