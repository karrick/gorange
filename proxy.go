package gorange

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ProxyConfig specifies the configuration for a gorange proxy HTTP server.
type ProxyConfig struct {
	// CheckVersionPeriodicity directs the range proxy to periodically send the '%version' query
	// the proxied range servers, and if the value is greater than the previous value, to
	// asynchronously update all the values for recently used range keys.
	CheckVersionPeriodicity time.Duration

	// Log directs the proxy to emit common log formatted log lines to the specified io.Writer.
	Log io.Writer

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

	h := PanicHandler(mux)
	if config.Timeout > 0 {
		h = http.TimeoutHandler(h, config.Timeout, "request took too long")
	}
	if config.Log != nil {
		h = ErrorLogHandler(h, config.Log)
		// h = LogHandler(h, config.Log)
	}

	server := http.Server{
		Addr:         fmt.Sprintf(":%d", config.Port),
		Handler:      h,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 15 * time.Second,
	}
	return server.ListenAndServe()
}

func httpError(w http.ResponseWriter, text string, code int) {
	fullText := strconv.Itoa(code) + " " + http.StatusText(code)
	if text != "" {
		fullText += ": " + text
	}
	http.Error(w, fullText, code)
}

func notFound() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpError(w, fmt.Sprintf("%v", r.URL), http.StatusNotFound)
	})
}

func onlyGet(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			httpError(w, r.Method, http.StatusMethodNotAllowed)
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
		return nil, fmt.Errorf("cannot decode query: " + err.Error())
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
			httpError(w, err.Error(), http.StatusBadRequest)
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
			httpError(w, "cannot resolve query: "+err.Error(), http.StatusBadGateway)
			return
		}
		if _, err = io.WriteString(w, response); err != nil {
			httpError(w, "cannot write response: "+err.Error(), http.StatusInternalServerError)
			return
		}
	})
}

func list(querier Querier) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := queryFromContext(r.Context())
		responses, err := querier.List(query)
		if err != nil {
			httpError(w, "cannot resolve query: "+err.Error(), http.StatusBadGateway)
			return
		}
		for _, response := range responses {
			if _, err = fmt.Fprintf(w, "%s\r\n", response); err != nil {
				httpError(w, "cannot write response: "+err.Error(), http.StatusInternalServerError)
				return
			}
		}
	})
}

// ErrorLogHandler returns a new http.Handler that logs HTTP requests that result in response
// errors. The handler will output lines in common log format to the specified io.Writer.
func ErrorLogHandler(next http.Handler, out io.Writer) http.Handler {
	const apacheLogFormat = "%s [%s] \"%s\" %d %d %f\n"
	const timeFormat = "02/Jan/2006:15:04:05 MST"

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lrw := &loggedResponseWriter{
			ResponseWriter: w,
			status:         http.StatusOK,
		}

		begin := time.Now()
		next.ServeHTTP(lrw, r)

		if lrw.status != http.StatusOK {
			end := time.Now()
			clientIP := r.RemoteAddr
			if colon := strings.LastIndex(clientIP, ":"); colon != -1 {
				clientIP = clientIP[:colon]
			}
			request := fmt.Sprintf("%s %s %s", r.Method, r.RequestURI, r.Proto)
			duration := end.Sub(begin).Seconds()
			formattedTime := end.UTC().Format(timeFormat)
			fmt.Fprintf(out, apacheLogFormat, clientIP, formattedTime, request, lrw.status, lrw.responseBytes, duration)
		}
	})
}

// LogHandler returns a new http.Handler that logs HTTP requests and responses in common log format
// to the specified io.Writer.
func LogHandler(next http.Handler, out io.Writer) http.Handler {
	const apacheLogFormat = "%s [%s] \"%s\" %d %d %f\n"
	const timeFormat = "02/Jan/2006:15:04:05 MST"

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lrw := &loggedResponseWriter{
			ResponseWriter: w,
			status:         http.StatusOK,
		}

		begin := time.Now()
		next.ServeHTTP(lrw, r)
		end := time.Now()

		clientIP := r.RemoteAddr
		if colon := strings.LastIndex(clientIP, ":"); colon != -1 {
			clientIP = clientIP[:colon]
		}
		request := fmt.Sprintf("%s %s %s", r.Method, r.RequestURI, r.Proto)

		duration := end.Sub(begin).Seconds()
		formattedTime := end.UTC().Format(timeFormat)
		fmt.Fprintf(out, apacheLogFormat, clientIP, formattedTime, request, lrw.status, lrw.responseBytes, duration)
	})
}

type loggedResponseWriter struct {
	http.ResponseWriter
	responseBytes int64
	status        int
}

func (r *loggedResponseWriter) Write(p []byte) (int, error) {
	written, err := r.ResponseWriter.Write(p)
	r.responseBytes += int64(written)
	return written, err
}

func (r *loggedResponseWriter) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// PanicHandler returns a new http.Handler that catches a panic caused by the specified
// http.Handler, and responds with an appropriate http status code and message.
func PanicHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if r := recover(); r != nil {
				var text string
				switch t := r.(type) {
				case error:
					text = t.Error()
				case string:

				default:
					text = fmt.Sprintf("%v", r)
				}
				httpError(w, text, http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
