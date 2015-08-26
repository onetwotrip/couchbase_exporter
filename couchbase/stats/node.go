package stats

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/Zumata/exporttools"
)

type NodeCollector struct {
	nodeURL string
}

func NewNodeCollector(nodeURL string) *NodeCollector {
	return &NodeCollector{nodeURL}
}

func (s *NodeCollector) Collect() ([]*exporttools.Metric, error) {
	stat := new(NodeStat)
	err := collectNodeStats(s.nodeURL, stat)
	if err != nil {
		return make([]*exporttools.Metric, 0), err
	}
	return formatNodeStats(s.nodeURL, stat), nil
}

type NodeStat struct {
	Nodes         []Node `json:"nodes"`
	StorageTotals `json:"storageTotals"`
}

type Node struct {
	Hostname         string           `json:"hostname"`
	InterestingStats InterestingStats `json:"interestingStats"`
}

type InterestingStats struct {
	CmdGet                   int64 `json:"cmd_get"`
	CouchDocsActualDiskSize  int64 `json:"couch_docs_actual_disk_size"`
	CouchDocsDataSize        int64 `json:"couch_docs_data_size"`
	CouchViewsActualDiskSize int64 `json:"couch_views_actual_disk_size"`
	CouchViewsDataSize       int64 `json:"couch_views_data_size"`
	CurrItems                int64 `json:"curr_items"`
	CurrItemsTot             int64 `json:"curr_items_tot"`
	EpBgFetched              int64 `json:"ep_bg_fetched"`
	GetHits                  int64 `json:"get_hits"`
	MemUsed                  int64 `json:"mem_used"`
	Ops                      int64 `json:"ops"`
	VbReplicaCurrItems       int64 `json:"vb_replica_curr_items"`
}

type StorageTotals struct {
	Hdd struct {
		// Free       int64 `json:"free"`
		// QuotaTotal int64 `json:"quotaTotal"`
		// Total      int64 `json:"total"`
		Used int64 `json:"used"`
		// UsedByData int64 `json:"usedByData"`
	} `json:"hdd"`
	RAM struct {
		// QuotaTotal        int64 `json:"quotaTotal"`
		// QuotaTotalPerNode int64 `json:"quotaTotalPerNode"`
		// QuotaUsed         int64 `json:"quotaUsed"`
		// QuotaUsedPerNode  int64 `json:"quotaUsedPerNode"`
		// Total             int64 `json:"total"`
		Used int64 `json:"used"`
		// UsedByData        int64 `json:"usedByData"`
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

func formatNodeStats(nodeURL string, s *NodeStat) []*exporttools.Metric {

	// add storage totals
	storageMetrics := []*exporttools.Metric{
		{
			Name:        "storage_hdd_used",
			Type:        exporttools.Gauge,
			Value:       s.StorageTotals.Hdd.Used,
			Description: "storage_hdd_used",
		},
		{
			Name:        "storage_ram_used",
			Type:        exporttools.Gauge,
			Value:       s.StorageTotals.RAM.Used,
			Description: "storage_ram_used",
		},
	}

	nodeMetrics := []*exporttools.Metric{}
	for nodeIdx := range s.Nodes {
		currentNodeMetrics := []*exporttools.Metric{
			{
				Name:        "cmd_get",
				Type:        exporttools.Gauge,
				Value:       s.Nodes[nodeIdx].InterestingStats.CmdGet,
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
				Value:       s.Nodes[nodeIdx].InterestingStats.GetHits,
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
				Value:       s.Nodes[nodeIdx].InterestingStats.Ops,
				Description: "ops",
			},
			{
				Name:        "vb_replica_curr_items",
				Type:        exporttools.Gauge,
				Value:       s.Nodes[nodeIdx].InterestingStats.VbReplicaCurrItems,
				Description: "vb_replica_curr_items",
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
