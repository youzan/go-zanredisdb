package zanredisdb

import (
	"errors"
	"strings"
	"time"
)

var (
	FailedOnClusterChanged           = "ERR_CLUSTER_CHANGED"
	FailedOnNotLeader                = "E_FAILED_ON_NOT_LEADER"
	FailedOnNotWritable              = "E_FAILED_ON_NOT_WRITABLE"
	FailedOnNodeStopped              = "the node stopped"
	errNoNodeForPartition            = errors.New("no partition node")
	errNoConnForHost                 = errors.New("no any connection for host")
	defaultGetConnTimeoutForLargeKey = time.Millisecond * 500
)

const (
	defaultMaxValueSize            = 1024 * 1024
	defaultLargeKeyConnPoolMinSize = 1
)

func IsConnectRefused(err error) bool {
	if err != nil {
		return strings.Contains(strings.ToLower(err.Error()), "connection refused")
	}
	return false
}

func IsFailedOnClusterChanged(err error) bool {
	if err != nil {
		return strings.HasPrefix(err.Error(), FailedOnClusterChanged) ||
			err == errNoNodeForPartition ||
			strings.Contains(err.Error(), FailedOnNodeStopped)
	}
	return false
}

func IsFailedOnNotWritable(err error) bool {
	if err != nil {
		return strings.HasPrefix(err.Error(), FailedOnNotWritable)
	}
	return false
}

type RemoteClusterConf struct {
	LookupList []string
	IsPrimary  bool
	ClusterDC  string
}

type MultiClusterConf []RemoteClusterConf

func (mcc MultiClusterConf) CheckValid() error {
	primaryCnt := 0
	for _, c := range mcc {
		if c.IsPrimary {
			primaryCnt++
		}
		if len(c.LookupList) == 0 {
			return errors.New("cluster lookup list should not be empty")
		}
		if c.ClusterDC == "" {
			return errors.New("multi clusters conf should have cluster dc info")
		}
	}
	if primaryCnt > 1 {
		return errors.New("primary cluster should be unique")
	}
	if primaryCnt != 1 {
		return errors.New("missing primary cluster")
	}
	return nil
}

type LargeKeyConf struct {
	MinPoolSize               int
	GetConnTimeoutForLargeKey time.Duration
	MaxAllowedValueSize       int
}

func NewLargeKeyConf() *LargeKeyConf {
	return &LargeKeyConf{
		MaxAllowedValueSize:       defaultMaxValueSize,
		GetConnTimeoutForLargeKey: defaultGetConnTimeoutForLargeKey,
		MinPoolSize:               defaultLargeKeyConnPoolMinSize,
	}
}

type Conf struct {
	LookupList []string
	// multi conf and lookuplist should not be used both
	MultiConf        MultiClusterConf
	DialTimeout      time.Duration
	ReadTimeout      time.Duration
	RangeReadTimeout time.Duration
	WriteTimeout     time.Duration
	IdleTimeout      time.Duration
	MaxActiveConn    int
	// idle num that will be kept for all idle connections
	MaxIdleConn int
	// default 0.4
	RangeConnRatio float64
	TendInterval   int64
	Namespace      string
	Password       string
	// the datacenter info for client
	// will be used for a single cluster acrossing datacenter
	DC string
}

func (conf *Conf) CheckValid() error {
	if len(conf.LookupList) > 0 && len(conf.MultiConf) > 0 {
		return errors.New("configure invalid: should not use both LookupList and MultiConf")
	}
	if len(conf.MultiConf) > 0 {
		return conf.MultiConf.CheckValid()
	}
	return nil
}

// api data response type
type node struct {
	BroadcastAddress string `json:"broadcast_address"`
	Hostname         string `json:"hostname"`
	RedisPort        string `json:"redis_port"`
	HTTPPort         string `json:"http_port"`
	GrpcPort         string `json:"grpc_port"`
	Version          string `json:"version"`
	DCInfo           string `json:"dc_info"`
}

type PartitionNodeInfo struct {
	Leader   node   `json:"leader"`
	Replicas []node `json:"replicas"`
}

type queryNamespaceResp struct {
	Epoch        int64                     `json:"epoch"`
	EngType      string                    `json:"eng_type"`
	Partitions   map[int]PartitionNodeInfo `json:"partitions"`
	PartitionNum int                       `json:"partition_num"`
}

type NodeInfo struct {
	RegID             uint64
	ID                string
	NodeIP            string
	Hostname          string
	RedisPort         string
	HttpPort          string
	RpcPort           string
	RaftTransportAddr string
	Version           string
	Tags              map[string]bool
	DataRoot          string
	RsyncModule       string
	Epoch             int64
}

type listPDResp struct {
	PDNodes  []NodeInfo `json:"pdnodes"`
	PDLeader NodeInfo   `json:"pdleader"`
}
