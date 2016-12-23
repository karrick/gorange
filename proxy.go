package gorange

import (
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// ProxyConfig specifies the configuration for a gorange proxy HTTP server.
type ProxyConfig struct {
	CheckVersionPeriodicity time.Duration
	Port                    uint
	Servers                 []string
}

// Proxy creates a proxy http server on the port that proxies range queries to the specified range
// servers.
func Proxy(config ProxyConfig) error {
	defaultQuerier, err := NewQuerier(&Configurator{
		CheckVersionPeriodicity: config.CheckVersionPeriodicity,
		RetryCount:              len(config.Servers),
		Servers:                 config.Servers,
	})
	if err != nil {
		return err
	}
	http.HandleFunc("/range/list", func(w http.ResponseWriter, r *http.Request) {
		query, err := url.QueryUnescape(r.URL.RawQuery)
		if err != nil {
			fmt.Fprintf(w, "Invalid query: %q\r\n", query)
			return
		}
		response, err := defaultQuerier.Query(query)
		if err != nil {
			fmt.Fprintf(w, "Cannot resolve query: %q; error: %s\r\n", query, err)
			return
		}
		for _, item := range response {
			fmt.Fprintf(w, "%s\r\n", item)
		}
	})
	return http.ListenAndServe(fmt.Sprintf(":%d", config.Port), nil)
}
