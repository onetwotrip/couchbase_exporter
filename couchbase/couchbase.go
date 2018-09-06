package couchbase

import (
	"github.com/dshmelev/couchbase_exporter/couchbase/stats"
	"github.com/dshmelev/couchbase_exporter/exporttools"
	_ "github.com/lib/pq"

	"github.com/prometheus/client_golang/prometheus"
)

type couchbaseExporter struct {
	*exporttools.BaseExporter
	nodeURL  string
	nodeName string
}

func NewExporter(nodeURL string, nodeName string) *couchbaseExporter {
	e := &couchbaseExporter{
		BaseExporter: exporttools.NewBaseExporter("couchbase"),
		nodeURL:      nodeURL,
		nodeName:     nodeName,
	}
	return e
}

func (e *couchbaseExporter) Setup() error {
	e.AddGroup(stats.NewNodeCollector(e.nodeURL, e.nodeName))
	return nil
}

func (e *couchbaseExporter) Close() error {
	return nil
}

func (e *couchbaseExporter) Describe(ch chan<- *prometheus.Desc) {
	exporttools.GenericDescribe(e.BaseExporter, ch)
}

func (e *couchbaseExporter) Collect(ch chan<- prometheus.Metric) {
	exporttools.GenericCollect(e.BaseExporter, ch)
}
