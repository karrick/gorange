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
	http.HandleFunc("/range/expand", makeGzipHandler(makeExpandHandler(querier)))
	http.HandleFunc("/range/list", makeGzipHandler(makeListHandler(querier)))
	return http.ListenAndServe(fmt.Sprintf(":%d", config.Port), nil)
}

func makeExpandHandler(querier Querier) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			logHTTPError(w, r, errors.New(r.Method), http.StatusMethodNotAllowed)
			return
		}
		query, err := url.QueryUnescape(r.URL.RawQuery)
		if err != nil {
			logHTTPError(w, r, fmt.Errorf("cannot decode query: %s", err), http.StatusBadRequest)
		}
		response, err := querier.Expand(query)
		if err != nil {
			logHTTPError(w, r, fmt.Errorf("cannot resolve query: %s", err), http.StatusBadGateway)
			return
		}
		if _, err = io.WriteString(w, response); err != nil {
			logHTTPError(w, r, fmt.Errorf("cannot write response: %s", err), http.StatusInternalServerError)
			return
		}
	}
}

func makeListHandler(querier Querier) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			logHTTPError(w, r, errors.New(r.Method), http.StatusMethodNotAllowed)
			return
		}
		query, err := url.QueryUnescape(r.URL.RawQuery)
		if err != nil {
			logHTTPError(w, r, fmt.Errorf("cannot decode query: %s", err), http.StatusBadRequest)
		}
		responses, err := querier.List(query)
		if err != nil {
			logHTTPError(w, r, fmt.Errorf("cannot resolve query: %s", err), http.StatusBadGateway)
			return
		}
		for _, response := range responses {
			if _, err = fmt.Fprintf(w, "%s\r\n", response); err != nil {
				logHTTPError(w, r, fmt.Errorf("cannot write response: %s", err), http.StatusInternalServerError)
				return
			}
		}
	}
}

func logHTTPError(w http.ResponseWriter, r *http.Request, err error, status int) {
	http.Error(w, err.Error(), status)
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
			logHTTPError(w, r, fmt.Errorf("cannot compress response: %s", err), http.StatusInternalServerError)
		}
	}
}
