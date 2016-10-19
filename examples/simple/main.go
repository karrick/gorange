package main

import (
	"fmt"
	"os"

	"github.com/karrick/gorange"
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
