package main

import (
	"bufio"
	"fmt"
	"os"
	"time"

	"github.com/karrick/gorange"
)

func main() {
	// create a range querier; could list additional servers or include other options as well
	querier, err := gorange.NewQuerier(&gorange.Configurator{
		CheckVersionPeriodicity: 15 * time.Second,
		TTL:     30 * time.Second,
		TTE:     2 * time.Hour,
		Servers: []string{"range.example.com"},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
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
