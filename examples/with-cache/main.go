package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/karrick/gogetter"
	"github.com/karrick/gorange"
)

func main() {

	// create a range client as demonstrated above:
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

	// cachingClient simply wraps the original client
	cachingClient, err := gorange.NewCachingClient(client, time.Minute)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s", err)
		os.Exit(1)
	}

	text := "%%someQuery"
	lines, err := cachingClient.Query(text) // <--- NOTE: using cachingClient here
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s", err)
		os.Exit(1)
	}
	for _, line := range lines {
		fmt.Println(line)
	}
}
