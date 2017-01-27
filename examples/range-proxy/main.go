package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/karrick/gorange"
)

var (
	argCheckVersionPeriodicity = flag.Duration("checkVersion", 15*time.Second, "periodicity to check %version for updates")
	argPort                    = flag.Uint("port", 8081, "port to bind to")
	argPprof                   = flag.Uint("pprof", 0, "pprof port to bind to")
	argServers                 = flag.String("servers", "", "specify comma delimited list of range servers")
)

func main() {
	flag.Parse()

	servers := strings.Split(*argServers, ",")
	if servers[0] == "" {
		servers = []string{"range.example.com"} // TODO: put one or more actual range server addresses here
	}

	if *argPprof > 0 {
		go func() {
			bind := fmt.Sprintf("localhost:%d", *argPprof)
			for {
				log.Println(http.ListenAndServe(bind, nil))
				time.Sleep(time.Second)
			}
		}()
	}

	log.Fatal(gorange.Proxy(gorange.ProxyConfig{
		CheckVersionPeriodicity: *argCheckVersionPeriodicity,
		Log:     os.Stderr,
		Port:    *argPort,
		Servers: servers,
		Timeout: 1 * time.Minute,
	}))
}
