package gorange

import (
	"fmt"
	"time"

	"github.com/karrick/congomap"
)

// CachingClient memoizes responses from a Querier.
type CachingClient struct {
	cache congomap.Congomap
}

// NewCachingClient returns CachingClient that attempts to respond to Query methods by consulting
// its TTL cache, then directing the call to the underlying Querier if a valid response is not
// stored.
func newCachingClient(querier Querier, ttl time.Duration) (*CachingClient, error) {
	if ttl < 0 {
		return nil, fmt.Errorf("cannot create CachingClient with negative TTL: %v", ttl)
	}
	cache, err := congomap.NewTwoLevelMap(congomap.TTL(ttl), congomap.Lookup(func(url string) (interface{}, error) {
		// NOTE: send query to querier when cache does not contain response for this URL yet
		return querier.Query(url)
	}))
	if err != nil {
		return nil, err
	}
	return &CachingClient{cache}, nil
}

// Query returns the response of the query, first checking in the TTL cache, then by actually
// invoking the Query method on the underlying Querier.
func (c *CachingClient) Query(query string) ([]string, error) {
	raw, err := c.cache.LoadStore(query)
	if err != nil {
		return nil, err
	}
	// NOTE: convert (interface{}, error) to ([]string, error), because Congomap always returns
	// empty interface as value
	response, ok := raw.([]string)
	if !ok {
		return nil, fmt.Errorf("expected []string; actual: %T", raw)
	}
	return response, nil
}
