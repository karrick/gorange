package gorange

import (
	"container/ring"
	"errors"
	"sync"
)

// roundRobinStrings returns a structure that on each invocation of its Next()
// method, returns the next string value from the list of values when it was
// initialized. On rollover, it returns the first value from the list.
type roundRobinStrings struct {
	r *ring.Ring
	l sync.Mutex
}

func newRoundRobinStrings(someStrings []string) (*roundRobinStrings, error) {
	l := len(someStrings)
	if l == 0 {
		return nil, errors.New("cannot create a round robin strings structure without at least one string")
	}

	// Create the data structure
	r := ring.New(l)

	// Populate data structure with values.
	for _, value := range someStrings {
		r = r.Next()
		r.Value = value
	}

	return &roundRobinStrings{r: r}, nil
}

// Len returns the number of strings in the roundRobinStrings structure.
func (rr *roundRobinStrings) Len() int { return rr.r.Len() }

// Next returns the next string in the roundRobinStrings structure.
func (rr *roundRobinStrings) Next() string {
	rr.l.Lock()
	next := rr.r.Next()
	rr.r = next
	rr.l.Unlock()
	return next.Value.(string)
}
