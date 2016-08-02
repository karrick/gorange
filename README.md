# gorange

gorange is a small Go library for interacting with range servers.

### Usage

Documentation is available via
[![GoDoc](https://godoc.org/github.com/karrick/gorange?status.svg)](https://godoc.org/github.com/karrick/gorange).

### Description

gorange provides a `Querier` interface, and a few data structures that implement this interface and
allows querying range services on remote hosts.

Querying range is a simple HTTP GET call, and Go already provides a steller http library. So why
wrap it? Well, either you write your own wrapper or use one someone else has written, it's all the
same to me. But I had to write the wrapper, so I figured I'd at least provide my implementation as a
reference piece for others doing the same.

In any eveny, this library

1. guarantees HTTP connections can be re-used by always reading all body bytes if the Get succeeded.
1. detects and parses the RangeException header, so you don't have to.
1. converts response body to slice of strings.

There are four possible error types this library returns:

1. Raw error that the underlying Get method returned.
1. ErrStatusNotOK is returned when the response status code is not OK.
1. ErrRangeException is returned when the response headers includes 'RangeException' header.
1. ErrParseException is returned by Client.Query when an error occurs while parsing the GET
response.

### Supported Use Cases

Both the `Client` and `CachingClient` data types implement the `Querier` interface. In fact,
`CachingClient` is implemented as a simple `Client` with a TTL cache, and the `NewCachingClient`
function merely wraps the provided `Client`. For a majority of use-cases, you don't need to worry
about any of this. I recommend ignoring `Client` and `CachingClient` and just think about calling
the `NewQuerier` function and getting back some opaque data structure instance that exposes the
`Query` method. See the _Simple_ code example below.

##### Simple

The easiest way to use gorange is to use a `Configurator` instance to create an object that
implements the `Querier` interface, and use that to query range.

```Go
    package main
    
    import (
        gorange "gopkg.in/karrick/gorange.v2"
    )

    var querier gorange.Querier

    func init() {
    	// create a range querier; could list additional servers or include other options as well
    	var err error
        querier, err = gorange.NewQuerier(&gorange.Configurator{Servers: []string{"range.example.com"}})
    	if err != nil {
    		fmt.Fprintf(os.Stderr, "%s\n", err)
    		os.Exit(1)
    	}
    }
    
    func main() {
    	// use the range querier
    	lines, err := querier.Query("%someQuery")
    	if err != nil {
    		fmt.Fprintf(os.Stderr, "%s", err)
    		os.Exit(1)
    	}
    	for _, line := range lines {
    		fmt.Println(line)
    	}
    }
```

##### Customized

As described above, the `NewQuerier` function allows creating objects that implement `Querier` and
supports most use-cases: optional round-robin query of multiple servers, optional retry of specific
errors, and optional TTL memoization of query responses. The only requirement is to specify one or
more servers to query. Leaving any other config option at its zero value creates a viable Querier
without those optional features.

See the `examples/customized/main.go` for complete example of this code, including constants and
functions not shown here.

```Go
    package main
    
    import (
        gorange "gopkg.in/karrick/gorange.v2"
    )

    func main() {
    	servers := []string{"range1.example.com", "range2.example.com", "range3.example.com"}
    
    	config := &gorange.Configurator{
    		Addr2Getter:   addr2Getter,
    		RetryCallback: retryCallback,
    		RetryCount:    len(servers),
    		Servers:       servers,
    		TTL:           time.Minute,
    	}
    
    	// create a range querier; could list additional servers or include other options as well
    	querier, err := gorange.NewQuerier(config)
    	if err != nil {
    		fmt.Fprintf(os.Stderr, "%s", err)
    		os.Exit(1)
    	}
    
    	// use the range querier
    	lines, err := querier.Query("%someQuery")
    	if err != nil {
    		fmt.Fprintf(os.Stderr, "%s", err)
    		os.Exit(1)
    	}
    	for _, line := range lines {
    		fmt.Println(line)
    	}
    }
```

##### Bit-Twiddling Tweaking Capabilities

While using the `NewQuerier` method is fine for most use-cases, there are times when you might need
to create your own querying pipeline. If you have a `Get` method that matches the `http.Client`
`Get` method's signature that you'd like to inject in the pipeline, you can build your own Querier
by composing the functionality you need for your application.

How to do this is illustrated by the `NewQuerier` function in this library, but a simple example is
shown below. Note that the example code simply wraps `gogetter.Getter` instances around other
`gogetter.Getter` instances.

NOTE: A `gogetter.Prefixer` is needed to prepend the server name and URL route to each URL before the
URL is sent to the underlying `http.Client`'s `Get` method.

WARNING: Using `http.Client` instance without a `Timeout` will cause resource leaks and may render
your program inoperative if the client connects to a buggy range server, or over a poor network
connection.

```Go
    func main() {
        // create a range client
    	server := "range.example.com"
    	querier := &gorange.Client{
    		&gogetter.Prefixer{
    			Prefix: fmt.Sprintf("http://%s/range/list?", server),

    			Getter: &http.Client{Timeout: 5 * time.Second}, // don't forget the Timeout...
    		},
    	}
    
        // use the range querier
    	lines, err := querier.Query("%someQuery")
    	if err != nil {
    		fmt.Fprintf(os.Stderr, "%s", err)
    		os.Exit(1)
    	}
    	for _, line := range lines {
    		fmt.Println(line)
    	}
    }
```
