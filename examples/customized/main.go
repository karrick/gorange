package main

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/karrick/gogetter"
	"github.com/karrick/gorange"
)

const (
	dialTimeout        = 1 * time.Second
	keepAliveDuration  = 10 * time.Minute
	maxIdleConnections = 5
	queryTimeout       = 5 * time.Second
	responseTTL        = 5 * time.Minute
)

func main() {
	servers := []string{"range1.example.com", "range.corp.linkedin.com", "range3.example.com"}

	config := &gorange.Configurator{
		Addr2Getter:             addr2Getter,
		RetryCount:              len(servers),
		Servers:                 servers,
		CheckVersionPeriodicity: 15 * time.Second,
	}

	// create a range querier
	querier, err := gorange.NewQuerier(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s", err)
		os.Exit(1)
	}

	// main loop
	fmt.Printf("> ")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Text()
		hosts, err := querier.Query(text)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			fmt.Printf("> ")
			continue
		}
		fmt.Printf("%s\n> ", hosts)
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
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
