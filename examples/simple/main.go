package main

import (
	"fmt"
	"os"

	gorange "gopkg.in/karrick/gorange.v1"
)

func main() {
	// create a range querier; could list additional servers or include other options as well
	querier, err := gorange.NewQuerier(&gorange.Configurator{Servers: []string{"range.example.com"}})
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
