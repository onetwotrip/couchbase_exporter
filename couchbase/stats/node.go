package stats

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/dshmelev/couchbase_exporter/exporttools"
)

type NodeCollector struct {
	nodeURL  string
	nodeName string
}

func NewNodeCollector(nodeURL string, nodeName string) *NodeCollector {
	return &NodeCollector{nodeURL, nodeName}
}

func (s *NodeCollector) Collect() ([]*exporttools.Metric, error) {
	stat := new(NodeStat)
	err := collectNodeStats(s.nodeURL, stat)
	if err != nil {
		return make([]*exporttools.Metric, 0), err
	}
	return formatNodeStats(s.nodeURL, s.nodeName, stat), nil
}

type NodeStat struct {
	Nodes         []Node `json:"nodes"`
	StorageTotals `json:"storageTotals"`
}

type Node struct {
	Uptime           int64            `json:"uptime,string,omitempty"`
	Status           string           `json:"status"`
	Hostname         string           `json:"hostname"`
	InterestingStats InterestingStats `json:"interestingStats"`
	SystemStats      SystemStats      `json:"SystemStats"`
	StorageTotals    StorageTotals    `json:"StorageTotals"`
}

type SystemStats struct {
	CpuUtilizationRate float64 `json:"cpu_utilization_rate,omitempty"`
	SwapTotal          int64   `json:"swap_total"`
	SwapUsed           int64   `json:"swap_used"`
	MemTotal           int64   `json:"mem_total"`
	MemFree            int64   `json:"mem_free"`
}

type InterestingStats struct {
	CmdGet                   float64 `json:"cmd_get"`
	CouchDocsActualDiskSize  int64   `json:"couch_docs_actual_disk_size"`
	CouchDocsDataSize        int64   `json:"couch_docs_data_size"`
	CouchViewsActualDiskSize int64   `json:"couch_views_actual_disk_size"`
	CouchViewsDataSize       int64   `json:"couch_views_data_size"`
	CurrItems                int64   `json:"curr_items"`
	CurrItemsTot             int64   `json:"curr_items_tot"`
	EpBgFetched              int64   `json:"ep_bg_fetched"`
	GetHits                  float64 `json:"get_hits"`
	MemUsed                  int64   `json:"mem_used"`
	Ops                      float64 `json:"ops"`
	VbReplicaCurrItems       int64   `json:"vb_replica_curr_items"`
}

type StorageTotals struct {
	Hdd struct {
		// QuotaTotal int64 `json:"quotaTotal"`
		Total      int64 `json:"total"`
		Free       int64 `json:"free"`
		Used       int64 `json:"used"`
		UsedByData int64 `json:"usedByData"`
	} `json:"hdd"`
	RAM struct {
		// QuotaTotal        int64 `json:"quotaTotal"`
		// QuotaTotalPerNode int64 `json:"quotaTotalPerNode"`
		// QuotaUsed         int64 `json:"quotaUsed"`
		// QuotaUsedPerNode  int64 `json:"quotaUsedPerNode"`
		Total      int64 `json:"total"`
		Used       int64 `json:"used"`
		UsedByData int64 `json:"usedByData"`
	} `json:"ram"`
}

var GlobalClient = http.Client{
	Timeout: time.Duration(10 * time.Second),
}

func collectNodeStats(nodeURL string, s *NodeStat) error {

	req, err := http.NewRequest("GET", nodeURL+"/pools/default", nil)
	if err != nil {
		return err
	}

	resp, err := GlobalClient.Do(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("couchbase node stats non-200 http status code received. body: %v", string(body))
	}

	return json.Unmarshal(body, s)

}

func formatNodeStats(nodeURL string, nodeName string, s *NodeStat) []*exporttools.Metric {

	// add storage totals
	storageMetrics := []*exporttools.Metric{
		{
			Name:        "storage_hdd_total",
			Type:        exporttools.Gauge,
			Value:       s.StorageTotals.Hdd.Total,
			Description: "storagetotals_hdd_total",
		},
		{
			Name:        "storage_hdd_free",
			Type:        exporttools.Gauge,
			Value:       s.StorageTotals.Hdd.Free,
			Description: "storagetotals_hdd_free",
		},
		{
			Name:        "storage_hdd_used",
			Type:        exporttools.Gauge,
			Value:       s.StorageTotals.Hdd.Used,
			Description: "storage_hdd_used",
		},
		{
			Name:        "storage_hdd_usedbydata",
			Type:        exporttools.Gauge,
			Value:       s.StorageTotals.Hdd.UsedByData,
			Description: "storagetotals_hdd_usedbydata",
		},
		{
			Name:        "storage_ram_total",
			Type:        exporttools.Gauge,
			Value:       s.StorageTotals.RAM.Total,
			Description: "storage_ram_total",
		},
		{
			Name:        "storage_ram_used",
			Type:        exporttools.Gauge,
			Value:       s.StorageTotals.RAM.Used,
			Description: "storage_ram_used",
		},
		{
			Name:        "storage_ram_usedbydata",
			Type:        exporttools.Gauge,
			Value:       s.StorageTotals.RAM.UsedByData,
			Description: "storage_ram_usedbydata",
		},
	}

	nodeMetrics := []*exporttools.Metric{}
	for nodeIdx := range s.Nodes {
		if !strings.HasPrefix(s.Nodes[nodeIdx].Hostname, nodeName) {
			continue
		}
		var nodeStatus int64
		if s.Nodes[nodeIdx].Status == "healthy" {
			nodeStatus = 1
		} else {
			nodeStatus = 0
		}
		currentNodeMetrics := []*exporttools.Metric{
			{
				Name:        "cmd_get",
				Type:        exporttools.Gauge,
				Value:       int64(s.Nodes[nodeIdx].InterestingStats.CmdGet),
				Description: "cmd_get",
			},
			{
				Name:        "couch_docs_actual_disk_size",
				Type:        exporttools.Gauge,
				Value:       s.Nodes[nodeIdx].InterestingStats.CouchDocsActualDiskSize,
				Description: "couch_docs_actual_disk_size",
			},
			{
				Name:        "couch_docs_data_size",
				Type:        exporttools.Gauge,
				Value:       s.Nodes[nodeIdx].InterestingStats.CouchDocsDataSize,
				Description: "couch_docs_data_size",
			},
			{
				Name:        "couch_views_actual_disk_size",
				Type:        exporttools.Gauge,
				Value:       s.Nodes[nodeIdx].InterestingStats.CouchViewsActualDiskSize,
				Description: "couch_views_actual_disk_size",
			},
			{
				Name:        "couch_views_data_size",
				Type:        exporttools.Gauge,
				Value:       s.Nodes[nodeIdx].InterestingStats.CouchViewsDataSize,
				Description: "couch_views_data_size",
			},
			{
				Name:        "curr_items",
				Type:        exporttools.Gauge,
				Value:       s.Nodes[nodeIdx].InterestingStats.CurrItems,
				Description: "curr_items",
			},
			{
				Name:        "curr_items_tot",
				Type:        exporttools.Gauge,
				Value:       s.Nodes[nodeIdx].InterestingStats.CurrItemsTot,
				Description: "curr_items_tot",
			},
			{
				Name:        "ep_bg_fetched",
				Type:        exporttools.Gauge,
				Value:       s.Nodes[nodeIdx].InterestingStats.EpBgFetched,
				Description: "ep_bg_fetched",
			},
			{
				Name:        "get_hits",
				Type:        exporttools.Gauge,
				Value:       int64(s.Nodes[nodeIdx].InterestingStats.GetHits),
				Description: "get_hits",
			},
			{
				Name:        "mem_used",
				Type:        exporttools.Gauge,
				Value:       s.Nodes[nodeIdx].InterestingStats.MemUsed,
				Description: "mem_used",
			},
			{
				Name:        "ops",
				Type:        exporttools.Gauge,
				Value:       int64(s.Nodes[nodeIdx].InterestingStats.Ops),
				Description: "ops",
			},
			{
				Name:        "vb_replica_curr_items",
				Type:        exporttools.Gauge,
				Value:       s.Nodes[nodeIdx].InterestingStats.VbReplicaCurrItems,
				Description: "vb_replica_curr_items",
			},
			{
				Name:        "cpu_utilization_rate",
				Type:        exporttools.Gauge,
				Value:       int64(s.Nodes[nodeIdx].SystemStats.CpuUtilizationRate),
				Description: "cpu_utilization_rate",
			},
			{
				Name:        "swap_total",
				Type:        exporttools.Gauge,
				Value:       s.Nodes[nodeIdx].SystemStats.SwapTotal,
				Description: "swap_total",
			},
			{
				Name:        "swap_used",
				Type:        exporttools.Gauge,
				Value:       s.Nodes[nodeIdx].SystemStats.SwapUsed,
				Description: "swap_used",
			},
			{
				Name:        "mem_total",
				Type:        exporttools.Gauge,
				Value:       s.Nodes[nodeIdx].SystemStats.MemTotal,
				Description: "mem_total",
			},
			{
				Name:        "mem_free",
				Type:        exporttools.Gauge,
				Value:       s.Nodes[nodeIdx].SystemStats.MemFree,
				Description: "mem_free",
			},
			{
				Name:        "uptime",
				Type:        exporttools.Gauge,
				Value:       s.Nodes[nodeIdx].Uptime,
				Description: "uptime",
			},
			{
				Name:        "status",
				Type:        exporttools.Gauge,
				Value:       nodeStatus,
				Description: "status",
			},
		}
		for cnmIdx := range currentNodeMetrics {
			currentNodeMetrics[cnmIdx].LabelKeys = []string{"hostname"}
			currentNodeMetrics[cnmIdx].LabelVals = []string{s.Nodes[nodeIdx].Hostname}
		}
		nodeMetrics = append(nodeMetrics, currentNodeMetrics...)
	}

	metrics := []*exporttools.Metric{}
	metrics = append(metrics, storageMetrics...)
	metrics = append(metrics, nodeMetrics...)

	for idx := range metrics {
		if metrics[idx].LabelKeys == nil {
			metrics[idx].LabelKeys = []string{"source"}
		} else {
			metrics[idx].LabelKeys = append(metrics[idx].LabelKeys, "source")
		}
		if metrics[idx].LabelVals == nil {
			metrics[idx].LabelVals = []string{nodeURL}
		} else {
			metrics[idx].LabelVals = append(metrics[idx].LabelVals, nodeURL)
		}
	}

	return metrics

}
