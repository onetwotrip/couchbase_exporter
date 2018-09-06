package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/dshmelev/couchbase_exporter/couchbase"
	"github.com/dshmelev/couchbase_exporter/exporttools"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	listenAddress = flag.String("web.listen-address", ":9131", "Address to listen on for web interface and telemetry.")
	metricPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
	nodeName      = flag.String("node.name", "", "Hostname to filter node metrics.")
	nodeURL       = flag.String("node.url", "http://localhost:8091", "DB Url")
)

func main() {

	flag.Parse()

	exporter := couchbase.NewExporter(*nodeURL, *nodeName)
	err := exporttools.Export(exporter)
	if err != nil {
		log.Fatal(err)
	}

	http.Handle(*metricPath, prometheus.Handler())
	http.HandleFunc("/", exporttools.DefaultMetricsHandler("Couchbase exporter", *metricPath))
	err = http.ListenAndServe(*listenAddress, nil)
	if err != nil {
		log.Fatal(err)
	}

}
