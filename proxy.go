package gorange

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
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
	querier, err := NewQuerier(&Configurator{
		CheckVersionPeriodicity: config.CheckVersionPeriodicity,
		RetryCount:              len(config.Servers),
		Servers:                 config.Servers,
	})
	if err != nil {
		return err
	}
	http.HandleFunc("/range/list", makeGzipHandler(makeRangeHandler(querier)))
	return http.ListenAndServe(fmt.Sprintf(":%d", config.Port), nil)
}

func makeRangeHandler(querier Querier) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			logHttpError(&w, r, errors.New(r.Method), http.StatusMethodNotAllowed)
			return
		}
		query, err := url.QueryUnescape(r.URL.RawQuery)
		if err != nil {
			logHttpError(&w, r, fmt.Errorf("cannot decode query: %s", err), http.StatusBadRequest)
		}
		response, err := querier.Query(query)
		if err != nil {
			logHttpError(&w, r, fmt.Errorf("cannot resolve query: %s", err), http.StatusBadGateway)
			return
		}
		for _, item := range response {
			fmt.Fprintf(w, "%s\r\n", item)
		}
	}
}

func logHttpError(w *http.ResponseWriter, r *http.Request, err error, status int) {
	http.Error(*w, err.Error(), status)
}

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func makeGzipHandler(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			fn(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		fn(gzipResponseWriter{Writer: gz, ResponseWriter: w}, r)
		err := gz.Close()
		if err != nil {
			logHttpError(&w, r, fmt.Errorf("cannot compress response: %s", err), http.StatusInternalServerError)
		}
	}
}
