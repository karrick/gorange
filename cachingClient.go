package gorange

import (
	"fmt"
	"time"

	"github.com/karrick/goswarm"
)

// CachingClient memoizes responses from a Querier.
type CachingClient struct {
	cache *goswarm.Simple
}

type queryResult struct {
	value        []string
	lastResponse time.Time
}

// NewCachingClient returns CachingClient that attempts to respond to Query methods by consulting
// its TTL cache, then directing the call to the underlying Querier if a valid response is not
// stored.
func newCachingClient(querier Querier, ttl, tte time.Duration) (*CachingClient, error) {
	if ttl < 0 {
		return nil, fmt.Errorf("cannot create CachingClient with negative TTL: %v", ttl)
	}
	cache, err := goswarm.NewSimple(&goswarm.Config{
		GoodStaleDuration:  ttl,
		GoodExpiryDuration: tte,
		BadStaleDuration:   1 * time.Minute,
		BadExpiryDuration:  5 * time.Minute,
		GCPeriodicity:      time.Hour,
		Lookup: func(url string) (interface{}, error) {
			raw, err := querier.Query(url)
			if err != nil {
				// don't return structs with error value
				return nil, err
			}
			return queryResult{
				value:        raw,
				lastResponse: time.Now(),
			}, nil
		},
	})
	if err != nil {
		return nil, err
	}
	return &CachingClient{cache}, nil
}

// Query returns the response of the query, first checking in the TTL cache, then by actually
// invoking the Query method on the underlying Querier.
func (c *CachingClient) Query(query string) ([]string, error) {
	raw, err := c.cache.Query(query)
	if err != nil {
		return nil, err
	}
	qr, ok := raw.(queryResult)
	if !ok {
		return nil, fmt.Errorf("expected queryResult; actual: %T", raw)
	}
	return qr.value, nil
}
