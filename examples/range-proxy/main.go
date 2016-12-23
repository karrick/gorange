package main

import (
	"flag"
	"log"
	"strings"
	"time"

	"github.com/karrick/gorange"
)

var (
	defaultQuerier          gorange.Querier
	checkVersionPeriodicity = flag.Duration("checkVersion", 15*time.Second, "periodicity to check %version for updates")
	port                    = flag.Uint("port", 8081, "port to bind to")
	servers                 = flag.String("servers", "", "specify comma delimited list of range servers")
)

func main() {
	flag.Parse()

	servers := strings.Split(*servers, ",")
	if len(servers) == 0 || servers[0] == "" {
		servers = []string{"range.corp.linkedin.com"} // TODO
	}

	log.Fatal(gorange.Proxy(gorange.ProxyConfig{
		CheckVersionPeriodicity: *checkVersionPeriodicity,
		Port:    *port,
		Servers: servers,
	}))
}
