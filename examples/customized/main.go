package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	gogetter "gopkg.in/karrick/gogetter.v1"
	gorange "gopkg.in/karrick/gorange.v1"
)

const (
	dialTimeout        = 1 * time.Second
	keepAliveDuration  = 10 * time.Minute
	maxIdleConnections = 5
	queryTimeout       = 5 * time.Second
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

func addr2Getter(addr string) gogetter.Getter {
	return &gogetter.Prefixer{
		Prefix: fmt.Sprintf("http://%s/range/list?", addr),
		Getter: &http.Client{
			// WARNING: Using http.Client instance without a Timeout will cause resource
			// leaks and may render your program inoperative if the client connects to a
			// buggy range server, or over a poor network connection.
			Timeout: time.Duration(queryTimeout),

			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout:   dialTimeout,
					KeepAlive: keepAliveDuration,
				}).Dial,
				MaxIdleConnsPerHost: int(maxIdleConnections),
			},
		},
	}
}

func retryCallback(err error) bool {
	if nerr, ok := err.(net.Error); ok {
		if nerr.Temporary() {
			return true
		}
	}
	return false
}
