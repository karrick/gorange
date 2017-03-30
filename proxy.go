package gorange

import (
	"context"
	"expvar"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/karrick/gohm"
)

var errorCount = expvar.NewInt("errorCount")

// ProxyConfig specifies the configuration for a gorange proxy HTTP server.
type ProxyConfig struct {
	// CheckVersionPeriodicity directs the range proxy to periodically send the '%version' query
	// the proxied range servers, and if the value is greater than the previous value, to
	// asynchronously update all the values for recently used range keys.
	CheckVersionPeriodicity time.Duration

	// Log directs the proxy to emit common log formatted log lines to the specified io.Writer.
	Log io.Writer

	// LogFormat specifies the log format to use when Log is not nil.  See `gohm` package for
	// LogFormat specifications.  If left blank, uses `gohm.DefaultLogFormat`.
	LogFormat string

	// Port specifies which network port the proxy should bind to.
	Port uint

	// Servers specifies which addresses ought to be consulted as the source of truth for range
	// queries.
	Servers []string

	// Timeout specifies how long to wait for the source of truth to respond. If the zero-value,
	// no timeout will be used. Not having a timeout value may cause resource exhaustion where
	// any of the proxied servers take too long to return a response.
	Timeout time.Duration
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

	mux := http.NewServeMux()
	mux.Handle("/range/expand", onlyGet(decodeURI(expand(querier))))
	mux.Handle("/range/list", onlyGet(decodeURI(list(querier))))
	mux.Handle("/", notFound())

	var h http.Handler = gohm.ConvertPanicsToErrors(mux)
	if config.Timeout > 0 {
		h = gohm.WithTimeout(config.Timeout, h)
	}
	if config.Log != nil {
		if config.LogFormat == "" {
			config.LogFormat = gohm.DefaultLogFormat
		}
		logBitmask := gohm.LogStatusAll
		h = gohm.LogStatusBitmaskWithFormat(config.LogFormat, &logBitmask, config.Log, h)
	}

	server := http.Server{
		Addr:         fmt.Sprintf(":%d", config.Port),
		Handler:      h,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 15 * time.Second,
	}
	return server.ListenAndServe()
}

func notFound() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gohm.Error(w, r.URL.String(), http.StatusNotFound)
	})
}

func onlyGet(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			gohm.Error(w, r.Method, http.StatusMethodNotAllowed)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type key int

const requestIDKey key = 0

func contextFromQuery(ctx context.Context, r *http.Request) (context.Context, error) {
	query, err := url.QueryUnescape(r.URL.RawQuery)
	if err != nil {
		return nil, fmt.Errorf("cannot decode query: %s", err)
	}
	return context.WithValue(ctx, requestIDKey, query), nil
}

func queryFromContext(ctx context.Context) string {
	return ctx.Value(requestIDKey).(string)
}

func decodeURI(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, err := contextFromQuery(r.Context(), r)
		if err != nil {
			gohm.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func expand(querier Querier) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := queryFromContext(r.Context())
		response, err := querier.Expand(query)
		if err != nil {
			gohm.Error(w, "cannot resolve query: "+err.Error(), http.StatusBadGateway)
			return
		}
		if _, err = io.WriteString(w, response); err != nil {
			gohm.Error(w, "cannot write response: "+err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func list(querier Querier) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := queryFromContext(r.Context())
		responses, err := querier.List(query)
		if err != nil {
			gohm.Error(w, "cannot resolve query: "+err.Error(), http.StatusBadGateway)
			return
		}
		for _, response := range responses {
			if _, err = fmt.Fprintf(w, "%s\r\n", response); err != nil {
				gohm.Error(w, "cannot write response: "+err.Error(), http.StatusInternalServerError)
				return
			}
		}
	})
}
