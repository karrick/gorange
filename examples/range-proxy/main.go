package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/karrick/golf"
	"github.com/karrick/gorange"
)

var (
	optCheckVersion = golf.DurationP('c', "check-version", 15*time.Second, "periodicity to check %version for updates")
	optPort         = golf.UintP('p', "port", 8081, "port to bind to")
	optPprof        = golf.Uint("pprof", 0, "pprof port to bind to")
	optServers      = golf.StringP('s', "servers", "", "specify comma delimited list of range servers")
	optTTE          = golf.DurationP('e', "tte", 12*time.Hour, "max duration prior to cache eviction")
)

func main() {
	golf.Parse()

	servers := strings.Split(*optServers, ",")
	if servers[0] == "" {
		servers = []string{"range"} // TODO: put one or more actual range server addresses here
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
		Timeout: 1 * time.Minute, // how long to wait for upstream to respond
		TTE:     *optTTE,
	}))
}
