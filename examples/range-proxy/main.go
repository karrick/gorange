package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/karrick/golf"
	"github.com/karrick/gorange"
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

	log.Fatal(gorange.Proxy(gorange.ProxyConfig{
		CheckVersionPeriodicity: *optCheckVersion,
		Log:     os.Stderr,
		Port:    *optPort,
		Servers: servers,
		Timeout: 1 * time.Minute, // how long to wait for downstream to respond
		TTE:     *optTTE,
	}))
}
