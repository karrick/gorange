package main

import (
	"bufio"
	"fmt"
	"os"
	"time"

	gorange "github.com/karrick/gorange/v3"
)

const (
	dialTimeout        = 1 * time.Second
	keepAliveDuration  = 10 * time.Minute
	maxIdleConnections = 5
	queryTimeout       = 5 * time.Second
	responseTTL        = 5 * time.Minute
)

func main() {
	servers := []string{"range1.example.com", "range2.example.com", "range3.example.com"}

	config := &gorange.Configurator{
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
