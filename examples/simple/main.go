package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/karrick/gogetter"
	"github.com/karrick/gorange"
)

func main() {

	// create a range client
	server := "range.example.com"
	prefix := fmt.Sprintf("http://%s/range/list?", server)
	client := &gorange.Client{
		&gogetter.Prefixer{
			Prefix: prefix,
			Getter: &http.Client{
			// customize if desired
			},
		},
	}

	// use the range client
	text := "%%someQuery"
	lines, err := client.Query(text)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s", err)
		os.Exit(1)
	}
	for _, line := range lines {
		fmt.Println(line)
	}
}
