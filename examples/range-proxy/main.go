package main

import (
	"context"
	"expvar"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	gohm "github.com/karrick/gohm/v2"
	"github.com/karrick/golf"
	gorange "github.com/karrick/gorange/v3"
)

var (
	optCheckVersion = golf.DurationP('c', "check-version", 15*time.Second, "periodicity to check %version for updates")
	optHelp         = golf.BoolP('h', "help", false, "display program help then exit")
	optPort         = golf.UintP('p', "port", 8081, "port to bind to")
	optPprof        = golf.Uint("pprof", 0, "pprof port to bind to")
	optServers      = golf.StringP('s', "servers", "range", "specify comma delimited list of range servers")
	optTTE          = golf.DurationP('e', "tte", 12*time.Hour, "max duration prior to cache eviction")
)

func main() {
	golf.Parse()

	if *optHelp {
		fmt.Fprintf(os.Stderr, "%s\n", filepath.Base(os.Args[0]))
		if *optHelp {
			fmt.Fprintf(os.Stderr, "\trun a reverse proxy against one or more range servers\n\n")
			fmt.Fprintf(os.Stderr, "For bug reports or feature requests:\n")
			fmt.Fprintf(os.Stderr, "\t* ask about `range-proxy` in #golang\n")
			fmt.Fprintf(os.Stderr, "\t* send email to govt@linkedin.com\n\n")
			golf.Usage()
		}
		os.Exit(0)
	}

	servers := strings.Split(*optServers, ",")
	if servers[0] == "" {
		fmt.Fprintf(os.Stderr, "ERROR: cannot proxy to unspecified servers\n")
		os.Exit(2)
	}

	if *optPprof > 0 {
		go func() {
			bind := fmt.Sprintf("localhost:%d", *optPprof)
			for {
				log.Println(http.ListenAndServe(bind, nil))
				time.Sleep(time.Second) // wait a moment before restarting
			}
		}()
	}

	log.Fatal(Proxy(ProxyConfig{
		CheckVersionPeriodicity: *optCheckVersion,
		Log:                     os.Stderr,
		Port:                    *optPort,
		Servers:                 servers,
		Timeout:                 1 * time.Minute, // how long to wait for downstream to respond
		TTE:                     *optTTE,
	}))
}

var errorCount = expvar.NewInt("errorCount")

// ProxyConfig specifies the configuration for a gorange proxy HTTP server.
type ProxyConfig struct {
	// CheckVersionPeriodicity directs the range proxy to periodically send the
	// '%version' query the proxied range servers, and if the value is greater
	// than the previous value, to asynchronously update all the values for
	// recently used range keys.
	CheckVersionPeriodicity time.Duration

	// Log directs the proxy to emit common log formatted log lines to the
	// specified io.Writer.
	Log io.Writer

	// LogFormat specifies the log format to use when Log is not nil.  See
	// `gohm` package for LogFormat specifications.  If left blank, uses
	// `gohm.DefaultLogFormat`.
	LogFormat string

	// Port specifies which network port the proxy should bind to.
	Port uint

	// Servers specifies which addresses ought to be consulted as the source of
	// truth for range queries.
	Servers []string

	// Timeout specifies how long to wait for the source of truth to respond. If
	// the zero-value, no timeout will be used. Not having a timeout value may
	// cause resource exhaustion where any of the proxied servers take too long
	// to return a response.
	Timeout time.Duration

	// TTE is duration of time before cached response is no longer able to be
	// served, even if attempts to fetch new value repeatedly fail.  This value
	// should be large if your application needs to still operate even when
	// range servers are down.  A zero-value for this implies that values never
	// expire and can continue to be served.  TTE and CheckVersionPeriodicity
	// work together to prevent frequently needlessly asking servers for
	// information that is still current while preventing heap build-up on
	// clients.
	TTE time.Duration
}

// Proxy creates a proxy http server on the port that proxies range queries to
// the specified range servers.
func Proxy(config ProxyConfig) error {
	querier, err := gorange.NewQuerier(&gorange.Configurator{
		CheckVersionPeriodicity: config.CheckVersionPeriodicity,
		RetryCount:              len(config.Servers),
		Servers:                 config.Servers,
		TTE:                     config.TTE,
	})
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("/range/expand", onlyGet(decodeURI(expand(querier, ","))))
	mux.Handle("/range/list", onlyGet(decodeURI(expand(querier, "\n"))))
	mux.Handle("/", notFound()) // while not required, this makes for a nicer log output and client response

	logBitmask := gohm.LogStatusErrors
	var h http.Handler = gohm.New(mux, gohm.Config{
		LogBitmask: &logBitmask,
		LogWriter:  config.Log,
		Timeout:    config.Timeout,
	})

	server := http.Server{
		Addr:         fmt.Sprintf(":%d", config.Port),
		Handler:      h,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
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

func expand(querier gorange.Querier, delim string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := queryFromContext(r.Context())
		results, err := querier.Query(query)
		if err != nil {
			gohm.Error(w, "cannot resolve query: "+err.Error(), http.StatusBadGateway)
			return
		}
		l := len(results)
		for i, result := range results {
			if i < l {
				result += delim
			} else {
				result += "\n"
			}
			if _, err = io.WriteString(w, result); err != nil {
				gohm.Error(w, "cannot write response: "+err.Error(), http.StatusInternalServerError)
				return
			}
		}
	})
}
