package zanredisdb

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/redigo/redis"
	"github.com/spaolacci/murmur3"
)

const (
	DefaultConnPoolMaxActive = 400
	DefaultConnPoolMaxIdle   = 3
)

var (
	RetryFailedInterval     = time.Second * 5
	MaxRetryInterval        = time.Minute
	NextRetryFailedInterval = time.Minute * 2
	ErrCntForStopRW         = 3
	LargeKeyPoolNum         = 4
	defaultWaitConnTimeout  = time.Millisecond * 150
)

var (
	errNoAnyPartitions  = errors.New("no any partitions")
	errInvalidPartition = errors.New("partition invalid")
)

func GetHashedPartitionID(pk []byte, pnum int) int {
	return int(murmur3.Sum32(pk)) % pnum
}

type PoolType int

const (
	DefPool PoolType = iota
	RangePool
	LargeKeyPool
)

func getPoolType(isRange bool) PoolType {
	poolType := DefPool
	if isRange {
		poolType = RangePool
	}
	return poolType
}

type HostStats struct {
	PoolCnt          int   `json:"pool_cnt,omitempty"`
	PoolWaitCnt      int   `json:"pool_wait_cnt,omitempty"`
	RangePoolCnt     int   `json:"range_pool_cnt,omitempty"`
	RangePoolWaitCnt int   `json:"range_pool_wait_cnt,omitempty"`
	LargeKeyPoolCnt  []int `json:"large_key_pool_cnt,omitempty"`
	FailedCnt        int64 `json:"failed_cnt,omitempty"`
	FailedTs         int64 `json:"failed_cnt,omitempty"`
}

type RedisHost struct {
	addr string
	// datacenter
	dcInfo string
	nInfo  node
	// failed count since last success
	lastFailedCnt int64
	lastFailedTs  int64
	connPool      *redis.QueuePool
	// connection for range query such as scan/lrange/smembers/zrange
	rangeConnPool   *redis.QueuePool
	largeKVPool     []*redis.QueuePool
	waitConnTimeout time.Duration
}

func newConnPool(
	maxIdle int, maxActive int,
	newFn func() (redis.Conn, error),
	testBorrow func(redis.Conn, time.Time) error,
	conf *Conf,
) *redis.QueuePool {
	connPool := redis.NewQueuePool(newFn, maxIdle, maxActive)
	connPool.IdleTimeout = 120 * time.Second
	connPool.TestOnBorrow = testBorrow
	if conf.IdleTimeout > time.Second {
		connPool.IdleTimeout = conf.IdleTimeout
	}
	tmpConn, _ := connPool.Get(0, 0)
	if tmpConn != nil {
		tmpConn.Close()
	}
	return connPool
}

func getRangeCnt(cnt int, ratio float64) int {
	if ratio < 0.001 || ratio > 1 {
		ratio = 0.2
	}
	ncnt := int(float64(cnt) * ratio)
	if ncnt < 1 {
		ncnt = 1
	}
	return ncnt
}

func (rh *RedisHost) InitConnPool(
	newFn func() (redis.Conn, error),
	newRangeFn func() (redis.Conn, error),
	testBorrow func(redis.Conn, time.Time) error,
	conf *Conf, largeConf *LargeKeyConf) {
	maxActive := DefaultConnPoolMaxActive
	maxIdle := DefaultConnPoolMaxIdle
	if conf.MaxActiveConn > 0 {
		maxActive = conf.MaxActiveConn
	}
	if conf.MaxIdleConn > 0 {
		maxIdle = conf.MaxIdleConn
	}
	rh.waitConnTimeout = conf.MaxConnWait
	if rh.waitConnTimeout == 0 {
		rh.waitConnTimeout = defaultWaitConnTimeout
	}
	rh.connPool = newConnPool(maxIdle, maxActive, newFn, testBorrow, conf)

	ratio := conf.RangeConnRatio
	if ratio < 0.01 {
		ratio = 0.4
	}
	maxIdle = getRangeCnt(maxIdle, ratio)
	maxActive = getRangeCnt(maxActive, ratio)
	rh.rangeConnPool = newConnPool(
		maxIdle,
		maxActive,
		newRangeFn,
		testBorrow,
		conf,
	)

	if largeConf != nil {
		lconf := *conf
		lconf.MaxActiveConn = largeConf.MinPoolSize
		lconf.WriteTimeout *= 2
		lconf.ReadTimeout *= 2
		lconf.RangeReadTimeout *= 2
		lconf.MaxIdleConn = largeConf.MinPoolSize
		maxActive := lconf.MaxActiveConn
		for i := 0; i < LargeKeyPoolNum; i++ {
			llconf := lconf
			llconf.MaxActiveConn = maxActive
			rh.largeKVPool = append(rh.largeKVPool, newConnPool(
				llconf.MaxIdleConn,
				llconf.MaxActiveConn,
				newRangeFn,
				testBorrow,
				&llconf,
			))
			maxActive *= 2
		}
	}
}

func (rh *RedisHost) IsActive() bool {
	return rh.connPool.IsActive()
}

func (rh *RedisHost) CloseConn() {
	rh.connPool.Close()
	rh.rangeConnPool.Close()
	for _, c := range rh.largeKVPool {
		c.Close()
	}
}

func (rh *RedisHost) Refresh() {
	rh.connPool.Refresh()
	rh.rangeConnPool.Refresh()
	for _, c := range rh.largeKVPool {
		c.Refresh()
	}
}

func (rh *RedisHost) Conn(poolType PoolType, hint int, retry int) (redis.Conn, error) {
	return rh.ConnPool(poolType).GetRetry(rh.waitConnTimeout, hint, retry)
}

func (rh *RedisHost) ConnPool(poolType PoolType) *redis.QueuePool {
	if poolType == DefPool {
		return rh.connPool
	}
	if poolType == RangePool {
		return rh.rangeConnPool
	}
	if poolType == LargeKeyPool && len(rh.largeKVPool) > 0 {
		return rh.largeKVPool[0]
	}
	return rh.connPool
}

func (rh *RedisHost) getConnPoolForLargeKey(vsize int, maxAllowed int) *redis.QueuePool {
	if len(rh.largeKVPool) == 0 || vsize == 0 {
		return rh.connPool
	}

	ratio := int(math.Log2(float64(maxAllowed / vsize)))
	if ratio >= len(rh.largeKVPool) || ratio < 0 {
		return rh.connPool
	}
	if levelLog.Level() > LOG_INFO {
		levelLog.Debugf("host %v get large pool %v for size: %v", rh.addr, ratio, vsize)
	}
	return rh.largeKVPool[ratio]
}

func (rh *RedisHost) Addr() string {
	return rh.addr
}

func (rh *RedisHost) GrpcAddr() string {
	h, _, err := net.SplitHostPort(rh.addr)
	if err != nil {
		return ""
	}
	return net.JoinHostPort(h, rh.nInfo.GrpcPort)
}

func (rh *RedisHost) MaybeIncFailed(err error) {
	if err == nil {
		return
	}
	if IsFailedOnClusterChanged(err) {
		return
	}
	if _, ok := err.(redis.Error); ok {
		return
	}
	if err == redis.ErrPoolExhausted {
		return
	}
	cnt := atomic.AddInt64(&rh.lastFailedCnt, 1)
	atomic.StoreInt64(&rh.lastFailedTs, time.Now().UnixNano())
	levelLog.Debugf("host %v inc failed count to %v for err: %v", rh.addr, cnt, err)
}

func (rh *RedisHost) IncSuccess() {
	fcnt := atomic.LoadInt64(&rh.lastFailedCnt)
	if fcnt == 0 {
		return
	}
	if fcnt > 0 {
		fcnt = atomic.AddInt64(&rh.lastFailedCnt, -1)
	}
	if fcnt < 0 {
		atomic.StoreInt64(&rh.lastFailedCnt, 0)
	}
}

func (rh *RedisHost) ChangeMaxActive(maxActive int, rangeRatio float64) {
	if maxActive <= 0 {
		return
	}
	maxRangeActive := getRangeCnt(maxActive, rangeRatio)

	rh.connPool.SetMaxActive(int32(maxActive))
	rh.rangeConnPool.SetMaxActive(int32(maxRangeActive))
}

func (rh *RedisHost) Stats() HostStats {
	var hs HostStats
	hs.FailedCnt = atomic.LoadInt64(&rh.lastFailedCnt)
	hs.FailedTs = atomic.LoadInt64(&rh.lastFailedTs)
	hs.PoolCnt = rh.connPool.Count()
	hs.PoolWaitCnt = rh.connPool.WaitingCount()
	hs.RangePoolCnt = rh.rangeConnPool.Count()
	hs.RangePoolWaitCnt = rh.rangeConnPool.WaitingCount()
	for _, p := range rh.largeKVPool {
		hs.LargeKeyPoolCnt = append(hs.LargeKeyPoolCnt, p.Count())
	}
	return hs
}

type PartitionInfo struct {
	Leader      *RedisHost
	Replicas    []*RedisHost
	chosenIndex uint32
}

type PartitionAddrInfo struct {
	Leader         string
	Replicas       []string
	ReplicasDCInfo []string
	ReplicaInfos   []node
	chosenIndex    uint32
}

func (pi *PartitionInfo) clone() PartitionInfo {
	var cloned PartitionInfo
	cloned.Leader = pi.Leader
	cloned.Replicas = make([]*RedisHost, 0, len(pi.Replicas))
	cloned.chosenIndex = pi.chosenIndex
	for _, v := range pi.Replicas {
		cloned.Replicas = append(cloned.Replicas, v)
	}
	return cloned
}

type Partitions struct {
	PNum  int
	Epoch int64
	PList []PartitionInfo
}

type PartitionAddrs struct {
	PNum  int
	PList []PartitionAddrInfo
}

func FindString(src []string, f string) int {
	for i, v := range src {
		if f == v {
			return i
		}
	}
	return -1
}

type Cluster struct {
	sync.RWMutex
	conf           *Conf
	largeKeyConf   *LargeKeyConf
	lookupMtx      sync.RWMutex
	LookupList     []string
	lookupIndex    int
	confLookupList []string

	namespace string
	//parts        Partitions
	parts atomic.Value
	//nodes        map[string]*RedisHost
	tendInterval int64
	wg           sync.WaitGroup
	quitC        chan struct{}
	stopping     int32
	tendTrigger  chan int

	dialF            func(string) (redis.Conn, error)
	dialRangeF       func(string) (redis.Conn, error)
	choseSameDCFirst int32
	localCluster     *Cluster
	isLocalForRead   bool
}

func NewCluster(conf *Conf, largeConf *LargeKeyConf) *Cluster {
	var primaryLookups []string
	var primaryCluster string
	if err := conf.CheckValid(); err != nil {
		panic(err)
	}
	if len(conf.LookupList) > 0 {
		primaryLookups = conf.LookupList
	} else {
		for _, c := range conf.MultiConf {
			if c.IsPrimary {
				primaryLookups = c.LookupList
				primaryCluster = c.ClusterDC
				break
			}
		}
	}
	cluster := &Cluster{
		quitC:        make(chan struct{}),
		tendTrigger:  make(chan int, 1),
		tendInterval: conf.TendInterval,
		LookupList:   make([]string, len(primaryLookups)),
		//nodes:        make(map[string]*RedisHost),
		namespace:      conf.Namespace,
		conf:           conf,
		largeKeyConf:   largeConf,
		confLookupList: primaryLookups,
	}
	if cluster.tendInterval <= 0 {
		panic("tend interval should be great than zero")
	}

	copy(cluster.LookupList, primaryLookups)

	cluster.dialF = func(addr string) (redis.Conn, error) {
		levelLog.Debugf("new conn dial to : %v", addr)
		return redis.Dial("tcp", addr, redis.DialConnectTimeout(conf.DialTimeout),
			redis.DialReadTimeout(conf.ReadTimeout),
			redis.DialWriteTimeout(conf.WriteTimeout),
			redis.DialPassword(conf.Password),
		)
	}
	cluster.dialRangeF = func(addr string) (redis.Conn, error) {
		levelLog.Debugf("new conn for range dial to : %v", addr)
		rt := conf.RangeReadTimeout
		if rt < time.Second {
			rt = conf.ReadTimeout*5 + time.Second
		}
		return redis.Dial("tcp", addr, redis.DialConnectTimeout(conf.DialTimeout),
			redis.DialReadTimeout(rt),
			redis.DialWriteTimeout(conf.WriteTimeout),
			redis.DialPassword(conf.Password),
		)
	}

	cluster.tend()

	cluster.wg.Add(1)

	// for client which is not in the primary cluster dc
	// all write will be written to primary
	// the read can be selected read local or primary
	// for client which is in the primary cluster dc, we just read and write in the same primary dc
	if conf.DC != primaryCluster && len(conf.MultiConf) > 0 {
		for _, c := range conf.MultiConf {
			if c.IsPrimary {
				continue
			}
			if c.ClusterDC != conf.DC {
				continue
			}
			localConf := *conf
			localConf.LookupList = c.LookupList
			localConf.MultiConf = nil
			cluster.localCluster = NewCluster(&localConf, largeConf)
			cluster.localCluster.isLocalForRead = true
			break
		}
	}

	go cluster.tendNodes()
	return cluster
}

func (cluster *Cluster) MaybeTriggerCheckForError(err error, delay time.Duration) bool {
	if err == nil {
		return false
	}
	if IsConnectClosed(err) {
		if delay > 0 {
			time.Sleep(delay)
		}
		// need retry for use of closed connection
		return true
	}
	if IsConnectRefused(err) || IsFailedOnClusterChanged(err) {
		if delay > 0 {
			time.Sleep(delay)
		}
		select {
		case cluster.tendTrigger <- 1:
			levelLog.Infof("trigger tend for err: %v", err)
		default:
		}
		return true
	}
	return false
}

func (cluster *Cluster) GetHostStats() map[string]HostStats {
	parts := cluster.getPartitions()
	nodes := getNodesFromParts(parts)
	hss := make(map[string]HostStats, len(nodes))
	for k, n := range nodes {
		hss[k] = n.Stats()
	}
	return hss
}

func (cluster *Cluster) ChangeMaxActive(active int) {
	parts := cluster.getPartitions()
	nodes := getNodesFromParts(parts)
	for _, n := range nodes {
		n.ChangeMaxActive(active, cluster.conf.RangeConnRatio)
	}
}

func (cluster *Cluster) getPartitions() *Partitions {
	p := cluster.parts.Load()
	if p == nil {
		return &Partitions{}
	}
	return p.(*Partitions)
}

func (cluster *Cluster) setPartitions(p *Partitions) {
	cluster.parts.Store(p)
}

func getNodesFromParts(parts *Partitions) map[string]*RedisHost {
	nodes := make(map[string]*RedisHost, parts.PNum)
	for _, v := range parts.PList {
		if v.Leader != nil {
			nodes[v.Leader.addr] = v.Leader
		}
		for _, r := range v.Replicas {
			nodes[r.addr] = r
		}
	}
	return nodes
}

func getGoodNodeInTheSameDC(partInfo *PartitionInfo, dc string) *RedisHost {
	idx := atomic.AddUint32(&partInfo.chosenIndex, 1)
	chosed := partInfo.Replicas[int(idx)%len(partInfo.Replicas)]
	retry := 0
	leastFailed := atomic.LoadInt64(&chosed.lastFailedCnt)
	leastFailedNode := chosed
	for retry < len(partInfo.Replicas) {
		retry++
		n := partInfo.Replicas[int(idx)%len(partInfo.Replicas)]
		if dc != "" && n.dcInfo != dc {
			continue
		}
		fc := atomic.LoadInt64(&n.lastFailedCnt)
		if fc <= int64(ErrCntForStopRW) {
			chosed = n
			leastFailedNode = nil
			break
		}
		// we try the failed again if last failed long ago
		if time.Now().UnixNano()-atomic.LoadInt64(&n.lastFailedTs) > NextRetryFailedInterval.Nanoseconds() {
			chosed = n
			leastFailedNode = nil
			levelLog.Debugf("retry failed node %v , failed at:%v, %v", n.addr, fc, atomic.LoadInt64(&n.lastFailedTs))
			break
		}

		idx = atomic.AddUint32(&partInfo.chosenIndex, 1)
		if fc < leastFailed {
			leastFailedNode = n
			leastFailed = fc
		}
	}
	if leastFailedNode != nil {
		chosed = leastFailedNode
	}
	return chosed
}

func (cluster *Cluster) GetPartitionNum() int {
	return cluster.getPartitions().PNum
}

func getHostByPart(part PartitionInfo, leader bool, sameDCFirst bool, localDC string) (*RedisHost, error) {
	var picked *RedisHost
	if leader {
		picked = part.Leader
	} else {
		if len(part.Replicas) == 0 {
			return nil, errNoNodeForPartition
		}
		dc := ""
		if sameDCFirst {
			dc = localDC
		}
		picked = getGoodNodeInTheSameDC(&part, dc)
	}

	if picked == nil {
		return nil, errNoNodeForPartition
	}
	return picked, nil
}

func (cluster *Cluster) getPartitionsWithError() (*Partitions, error) {
	parts := cluster.getPartitions()
	if parts == nil {
		return nil, errNoAnyPartitions
	}
	if parts.PNum == 0 || len(parts.PList) == 0 {
		return nil, errNoAnyPartitions
	}
	return parts, nil
}

func (cluster *Cluster) GetHostByPart(pid int, leader bool) (*RedisHost, error) {
	parts, err := cluster.getPartitionsWithError()
	if err != nil {
		return nil, err
	}
	if pid >= len(parts.PList) {
		return nil, errInvalidPartition
	}
	part := parts.PList[pid]
	return getHostByPart(part, leader, cluster.IsSameDCFirst(), cluster.conf.DC)
}

func (cluster *Cluster) GetAllHostsByPart(pid int) ([]*RedisHost, error) {
	parts, err := cluster.getPartitionsWithError()
	if err != nil {
		return nil, err
	}
	if pid >= len(parts.PList) {
		return nil, errInvalidPartition
	}
	part := parts.PList[pid]
	return part.clone().Replicas, nil
}

func (cluster *Cluster) IsSameDCFirst() bool {
	return atomic.LoadInt32(&cluster.choseSameDCFirst) == 1
}

func (cluster *Cluster) GetNodeHost(pk []byte, leader bool, tryLocalForRead bool) (*RedisHost, error) {
	if cluster.localCluster != nil && tryLocalForRead {
		if levelLog.Level() > 2 {
			levelLog.Detailf("try local dc read for pk: %s", string(pk))
		}
		return cluster.localCluster.GetNodeHost(pk, leader, false)
	}
	parts, err := cluster.getPartitionsWithError()
	if err != nil {
		return nil, err
	}
	pid := GetHashedPartitionID(pk, parts.PNum)
	part := parts.PList[pid]
	picked, err := getHostByPart(part, leader, cluster.IsSameDCFirst(), cluster.conf.DC)
	if err != nil {
		return nil, err
	}
	if levelLog.Level() > 2 {
		levelLog.Detailf("node %v @ %v (last failed: %v) chosen for partition id: %v, pk: %s", picked.addr,
			picked.dcInfo, atomic.LoadInt64(&picked.lastFailedCnt), pid, string(pk))
	}
	return picked, nil
}

func (cluster *Cluster) GetConnForLarge(pk []byte, leader bool, tryLocalForRead bool, vsize int) (redis.Conn, error) {
	_, conn, err := cluster.GetHostAndConnForLarge(pk, leader, tryLocalForRead, vsize)
	return conn, err
}

func (cluster *Cluster) GetConn(pk []byte, leader bool, tryLocalForRead bool, isSlowQuery bool) (redis.Conn, error) {
	_, conn, err := cluster.GetHostAndConn(pk, leader, tryLocalForRead, isSlowQuery)
	return conn, err
}

func (cluster *Cluster) GetHostAndConn(pk []byte, leader bool, tryLocalForRead bool, isSlowQuery bool) (*RedisHost, redis.Conn, error) {
	picked, err := cluster.GetNodeHost(pk, leader, tryLocalForRead)
	if err != nil {
		return nil, nil, err
	}
	conn, err := picked.Conn(getPoolType(isSlowQuery), int(pk[0]), cluster.conf.MaxRetryGetConn)
	return picked, conn, err
}

func (cluster *Cluster) GetHostAndConnForLarge(pk []byte, leader bool, tryLocalForRead bool, vsize int) (*RedisHost, redis.Conn, error) {
	picked, err := cluster.GetNodeHost(pk, leader, tryLocalForRead)
	if err != nil {
		return nil, nil, err
	}
	if cluster.largeKeyConf != nil {
		rh := picked.getConnPoolForLargeKey(vsize, cluster.largeKeyConf.MaxAllowedValueSize)
		conn, err := rh.GetRetry(cluster.largeKeyConf.GetConnTimeoutForLargeKey, int(pk[0]), 1)
		return picked, conn, err
	}

	conn, err := picked.Conn(getPoolType(vsize >= defaultMaxValueSize/4), int(pk[0]), cluster.conf.MaxRetryGetConn)
	return picked, conn, err
}

func (cluster *Cluster) getConnsByHosts(hosts []string, isSlowQuery bool) ([]redis.Conn, error) {
	parts := cluster.getPartitions()
	if parts == nil {
		return nil, errNoAnyPartitions
	}

	nodes := getNodesFromParts(parts)
	var conns []redis.Conn
	for _, h := range hosts {
		if v, ok := nodes[h]; ok {
			conn, err := v.Conn(getPoolType(isSlowQuery), 0, cluster.conf.MaxRetryGetConn)
			if err != nil {
				return nil, err
			}
			conns = append(conns, conn)
		} else {
			return nil, fmt.Errorf("node %v not found while get connection", h)
		}
	}
	return conns, nil
}

func (cluster *Cluster) GetConnsForAllParts(isSlowQuery bool) ([]redis.Conn, error) {
	parts := cluster.getPartitions()
	if parts == nil {
		return nil, errNoAnyPartitions
	}
	if parts.PNum == 0 || len(parts.PList) == 0 {
		return nil, errNoNodeForPartition
	}
	var conns []redis.Conn
	for _, p := range parts.PList {
		if p.Leader == nil {
			return nil, errors.New("no leader for partition")
		}
		conn, err := p.Leader.Conn(getPoolType(isSlowQuery), 0, cluster.conf.MaxRetryGetConn)
		if err != nil {
			return nil, err
		}
		conns = append(conns, conn)
	}
	return conns, nil
}

func (cluster *Cluster) GetConnsByHosts(hosts []string, isSlowQuery bool) ([]redis.Conn, error) {
	return cluster.getConnsByHosts(hosts, isSlowQuery)
}

func (cluster *Cluster) nextLookupEndpoint(epoch int64) (string, string, string) {
	cluster.lookupMtx.RLock()
	if cluster.lookupIndex >= len(cluster.LookupList) {
		cluster.lookupIndex = 0
	}
	addr := cluster.LookupList[cluster.lookupIndex]
	num := len(cluster.LookupList)
	cluster.lookupIndex = (cluster.lookupIndex + 1) % num
	cluster.lookupMtx.RUnlock()

	urlString := addr
	if !strings.Contains(urlString, "://") {
		urlString = "http://" + addr
	}

	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	listUrl := *u
	if u.Path == "/" || u.Path == "" {
		u.Path = "/query/" + cluster.namespace
	}
	listUrl.Path = "/listpd"

	tmpUrl := *u
	v, _ := url.ParseQuery(tmpUrl.RawQuery)
	v.Add("epoch", strconv.FormatInt(epoch, 10))
	tmpUrl.RawQuery = v.Encode()
	return addr, tmpUrl.String(), listUrl.String()
}

func (cluster *Cluster) tend() {
	oldPartitions := cluster.getPartitions()
	oldEpoch := oldPartitions.Epoch

	addr, queryStr, discoveryUrl := cluster.nextLookupEndpoint(oldEpoch)
	// discovery other lookupd nodes from current lookupd or from etcd
	levelLog.Debugf("discovery lookupd %s", discoveryUrl)
	var listResp listPDResp
	httpRespCode, err := apiRequest("GET", discoveryUrl, nil, &listResp)
	if err != nil {
		levelLog.Warningf("error discovery lookup (%s) - %s, code: %v", discoveryUrl, err, httpRespCode)
		if httpRespCode < 0 {
			cluster.lookupMtx.Lock()
			// remove failed if not seed nodes
			if FindString(cluster.confLookupList, addr) == -1 && IsConnectRefused(err) {
				levelLog.Infof("removing failed lookup : %v", addr)
				newLookupList := make([]string, 0)
				for _, v := range cluster.LookupList {
					if v == addr {
						continue
					} else {
						newLookupList = append(newLookupList, v)
					}
				}
				cluster.LookupList = newLookupList
			}
			cluster.lookupMtx.Unlock()
			select {
			case cluster.tendTrigger <- 1:
				levelLog.Infof("trigger tend for err: %v", err)
			default:
			}
			return
		}
	} else {
		for _, node := range listResp.PDNodes {
			addr := net.JoinHostPort(node.NodeIP, node.HttpPort)
			cluster.lookupMtx.Lock()
			found := false
			for _, x := range cluster.LookupList {
				if x == addr {
					found = true
					break
				}
			}
			if !found {
				cluster.LookupList = append(cluster.LookupList, addr)
				levelLog.Infof("new lookup added %v", addr)
			}
			cluster.lookupMtx.Unlock()
		}
	}

	levelLog.Debugf("querying for namespace %s", queryStr)
	var data queryNamespaceResp
	statusCode, err := apiRequest("GET", queryStr, nil, &data)
	if err != nil {
		if statusCode != http.StatusNotModified {
			levelLog.Warningf("error querying (%s) - %s", queryStr, err)
		} else {
			levelLog.Debugf("server return unchanged, local %v", oldEpoch)
		}
		return
	}

	if len(data.Partitions) != data.PartitionNum {
		levelLog.Warningf("response on partitions mismatch: %v", data)
		return
	}
	newPartitions := PartitionAddrs{PNum: data.PartitionNum, PList: make([]PartitionAddrInfo, data.PartitionNum)}
	if data.Epoch == oldEpoch {
		levelLog.Debugf("namespace info keep unchanged: %v", data)
		return
	}
	if data.Epoch < oldEpoch {
		levelLog.Infof("namespace info is older: %v vs %v", data.Epoch, oldEpoch)
		return
	}

	levelLog.Infof("namespace info from server is: %v, old %v", data.Epoch, oldEpoch)
	for partID, partNodeInfo := range data.Partitions {
		if partID >= newPartitions.PNum || partID < 0 {
			levelLog.Errorf("got invalid partition : %v", partID)
			return
		}
		var leaderAddr string
		var oldPartInfo PartitionInfo
		var oldLeader string
		if partID < len(oldPartitions.PList) {
			oldPartInfo = oldPartitions.PList[partID]
			if oldPartInfo.Leader != nil {
				oldLeader = oldPartInfo.Leader.addr
			}
		}

		replicas := make([]string, 0)
		dcInfos := make([]string, 0)
		ninfos := make([]node, 0)
		for _, n := range partNodeInfo.Replicas {
			if n.BroadcastAddress != "" {
				addr := net.JoinHostPort(n.BroadcastAddress, n.RedisPort)
				replicas = append(replicas, addr)
				dcInfos = append(dcInfos, n.DCInfo)
				ninfos = append(ninfos, n)
			}
		}
		node := partNodeInfo.Leader
		if node.BroadcastAddress != "" {
			leaderAddr = net.JoinHostPort(node.BroadcastAddress, node.RedisPort)
		} else {
			levelLog.Infof("partition %v missing leader node, use old instead %v", partID, oldPartInfo.Leader)
			for _, n := range replicas {
				// only use old leader when the old leader is in new replicas
				if n == oldLeader {
					leaderAddr = oldLeader
					break
				}
			}
		}
		if oldLeader != leaderAddr {
			levelLog.Infof("partition %v leader changed from %v to %v", partID, oldLeader, leaderAddr)
		}
		if len(replicas) == 0 {
			levelLog.Infof("no any replicas for partition : %v, %v", partID, partNodeInfo)
			return
		}
		pinfo := PartitionAddrInfo{Leader: leaderAddr,
			Replicas:       replicas,
			ReplicasDCInfo: dcInfos,
			ReplicaInfos:   ninfos,
		}
		pinfo.chosenIndex = atomic.LoadUint32(&oldPartInfo.chosenIndex)
		newPartitions.PList[partID] = pinfo
		levelLog.Infof("namespace %v partition %v replicas changed to : %v", cluster.namespace, partID, pinfo)
	}
	cleanHosts := make(map[string]*RedisHost)
	nodes := getNodesFromParts(oldPartitions)
	if len(newPartitions.PList) > 0 {
		for k, p := range nodes {
			found := false
			for _, partInfo := range newPartitions.PList {
				if p.addr == partInfo.Leader {
					found = true
					break
				}
				for _, replica := range partInfo.Replicas {
					if p.addr == replica {
						found = true
						break
					}
				}
				if found {
					break
				}
			}
			if !found {
				levelLog.Infof("node %v for namespace %v removing since not in lookup", p.addr, cluster.namespace)
				cleanHosts[k] = p
				delete(nodes, k)
			}
		}
	}

	testF := func(c redis.Conn, t time.Time) (err error) {
		if time.Since(t) > 60*time.Second {
			_, err = c.Do("PING")
		}
		return
	}

	for _, partInfo := range newPartitions.PList {
		for idx, replica := range partInfo.Replicas {
			rh, ok := nodes[replica]
			if ok && rh != nil && rh.IsActive() {
				continue
			}
			newNode := &RedisHost{addr: replica,
				dcInfo: partInfo.ReplicasDCInfo[idx],
				nInfo:  partInfo.ReplicaInfos[idx],
			}

			newNode.InitConnPool(func() (redis.Conn, error) {
				return cluster.dialF(newNode.addr)
			}, func() (redis.Conn, error) {
				return cluster.dialRangeF(newNode.addr)
			}, testF, cluster.conf, cluster.largeKeyConf)
			levelLog.Infof("host:%v is available and come into service: %v",
				newNode.addr+"@"+newNode.dcInfo, newNode.nInfo)
			nodes[replica] = newNode
		}
	}
	newHostPartitions := &Partitions{PNum: newPartitions.PNum, Epoch: data.Epoch,
		PList: make([]PartitionInfo, 0, len(newPartitions.PList))}

	for _, partInfo := range newPartitions.PList {
		var pi PartitionInfo
		pi.chosenIndex = partInfo.chosenIndex
		var ok bool
		pi.Leader, ok = nodes[partInfo.Leader]
		if !ok {
			levelLog.Infof("host:%v not found ", partInfo.Leader)
		}
		for _, r := range partInfo.Replicas {
			n, ok := nodes[r]
			if !ok || n == nil {
				levelLog.Infof("host:%v not found ", r)
				continue
			}
			pi.Replicas = append(pi.Replicas, n)
		}
		newHostPartitions.PList = append(newHostPartitions.PList, pi)
	}
	cluster.setPartitions(newHostPartitions)
	levelLog.Infof("namespace info update to epoch: %v", data.Epoch)
	for _, p := range cleanHosts {
		p.CloseConn()
	}
}

func (cluster *Cluster) tendNodes() {
	tendTicker := time.NewTicker(time.Duration(cluster.tendInterval) * time.Second)
	defer func() {
		tendTicker.Stop()
		cluster.wg.Done()
	}()

	trigCnt := 0
	for {
		select {
		case <-tendTicker.C:
			cluster.tend()
			trigCnt = 0

			nodes := getNodesFromParts(cluster.getPartitions())
			for _, n := range nodes {
				n.Refresh()
			}

		case <-cluster.tendTrigger:
			levelLog.Infof("trigger tend")
			cluster.tend()
			trigCnt++
			d := time.Duration(trigCnt) * MinRetrySleep
			if d > time.Second {
				d = time.Second
			}
			time.Sleep(d)
		case <-cluster.quitC:
			nodes := getNodesFromParts(cluster.getPartitions())
			for _, node := range nodes {
				node.CloseConn()
			}
			levelLog.Debugf("go routine for tend cluster exit")
			return
		}
	}
}

func (cluster *Cluster) IsStopped() bool {
	return atomic.LoadInt32(&cluster.stopping) == 1
}

func (cluster *Cluster) Close() {
	if !atomic.CompareAndSwapInt32(&cluster.stopping, 0, 1) {
		return
	}
	close(cluster.quitC)
	cluster.wg.Wait()
	cluster.setPartitions(&Partitions{})
	if cluster.localCluster != nil {
		cluster.localCluster.Close()
	}
	levelLog.Debugf("cluster exit")
}
