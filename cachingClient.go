package gorange

import (
	"fmt"
	"time"

	congomap "gopkg.in/karrick/congomap.v2"
)

// CachingClient memoizes responses from a Querier.
type CachingClient struct {
	ttl     time.Duration
	cache   congomap.Congomap
	querier Querier
}

// NewCachingClient returns CachingClient that attempts to respond to Query methods by consulting
// its TTL cache, then directing the call to the underlying Querier if a valid response is not
// stored.
func NewCachingClient(querier Querier, ttl time.Duration) (*CachingClient, error) {
	if ttl < 0 {
		return nil, fmt.Errorf("cannot create CachingClient with negative TTL: %v", ttl)
	}
	client := &CachingClient{querier: querier, ttl: ttl}
	var err error
	client.cache, err = congomap.NewTwoLevelMap(congomap.TTL(ttl), congomap.Lookup(func(url string) (interface{}, error) {
		// NOTE: send query to underlying querier when cache does not contain response for this URL yet
		return client.querier.Query(url)
	}))
	// NOTE: don't want to send partially instantiated client structure back if there was an error creating the cache
	if err != nil {
		return nil, err
	}
	return client, nil
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
