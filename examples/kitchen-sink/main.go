package main

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/karrick/gogetter"
	"github.com/karrick/gorange"
)

func addrs2querier(hostnames []string) (gorange.Querier, error) {
	if len(hostnames) == 0 {
		return nil, fmt.Errorf("cannot create without at least one server address")
	}
	var hg gogetter.Getter

	if len(hostnames) == 1 {
		hg = addr2getter(hostnames[0])
	} else {
		rr := &gogetter.RoundRobin{}
		for _, hostname := range hostnames {
			rr.Getters = append(rr.Getters, addr2getter(hostname))
		}
		hg = rr
	}

	// set 'retryCount' to a positive number if you'd like to retry on errors; set it to 0 to
	// send queries only once
	retryCount := 100
	if retryCount > 0 {
		hg = &gogetter.Retrier{
			Getter:     hg,
			RetryCount: retryCount,
		}
	}

	q := &gorange.Client{hg}

	// set 'ttl' to a positive duration if you'd like to memoize results
	ttl := time.Duration(10 * time.Second)
	if ttl > 0 {
		return gorange.NewCachingClient(q, ttl)
	}

	// return to caller
	return q, nil
}

func addr2getter(addr string) gogetter.Getter {
	return &gogetter.Failer{
		Frequency: 0.1,
		Getter: &gogetter.Prefixer{
			Prefix: fmt.Sprintf("http://%s/range/list?", addr),
			// NOTE: customize http.Client as desired:
			Getter: &http.Client{
			// Transport: &http.Transport{
			// 	MaxIdleConnsPerHost: int(maxConns),
			// },
			// Timeout: time.Duration(timeout),
			},
		},
	}
}

func main() {
	client, err := addrs2querier([]string{"range.example.com"})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("> ")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := scanner.Text()
		hosts, err := client.Query(text)
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
