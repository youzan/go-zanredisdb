package zanredisdb

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/redigo/redis"
)

var ErrSizeExceedLimit = errors.New("key value size exceeded the limit")

const (
	MinRetrySleep = time.Millisecond * 5
	anyCmd        = 0
	readCmd       = 1
	writeCmd      = 2
)

const (
	defCmd int = iota
	batchCmd
	exceptionCmd
)

func filterDuplicateConn(conns []redis.Conn) []redis.Conn {
	newConns := make([]redis.Conn, 0, len(conns))
	scanned := make(map[string]bool)
	for _, c := range conns {
		_, ok := scanned[c.RemoteAddrStr()]
		if ok {
			continue
		}
		scanned[c.RemoteAddrStr()] = true
		newConns = append(newConns, c)
	}

	return newConns
}

type PipelineCmd struct {
	CmdName     string
	ShardingKey []byte
	ToLeader    bool
	Args        []interface{}
}
type PipelineCmdList []PipelineCmd

func (pl *PipelineCmdList) Add(cmd string, shardingKey []byte, toLeader bool,
	args ...interface{}) {
	*pl = append(*pl, PipelineCmd{CmdName: cmd,
		ShardingKey: shardingKey,
		ToLeader:    toLeader,
		Args:        args})
}

type ZanRedisClient struct {
	conf         *Conf
	largeKeyConf *LargeKeyConf
	cluster      *Cluster
}

func NewZanRedisClient(conf *Conf) (*ZanRedisClient, error) {
	err := conf.CheckValid()
	if err != nil {
		return nil, err
	}
	return &ZanRedisClient{
		conf: conf,
	}, nil
}

func (self *ZanRedisClient) SetLargeKeyConf(conf *LargeKeyConf) {
	self.largeKeyConf = conf
}

func (self *ZanRedisClient) Start() {
	self.cluster = NewCluster(self.conf, self.largeKeyConf)
}

// while deploy across two datacenters, to improve read latency we can
// enable this, and set toLeader=false while calling DoRedis
// if there is no node in the same data center, we will fallback to the random node in other dc.
func (self *ZanRedisClient) SwitchSameDC(useSameDC bool) {
	if useSameDC {
		atomic.StoreInt32(&self.cluster.choseSameDCFirst, 1)
	} else {
		atomic.StoreInt32(&self.cluster.choseSameDCFirst, 0)
	}
}

func (self *ZanRedisClient) Stop() {
	if self.cluster != nil {
		self.cluster.Close()
	}
}

func DoRedisCmd(conn redis.Conn, cmdName string, args ...interface{}) (reply interface{}, err error) {
	defer conn.Close()
	rsp, err := conn.Do(cmdName, args...)
	return rsp, err
}

func (self *ZanRedisClient) doPipelineCmd(cmds PipelineCmdList,
	rsps []interface{}, errs []error) ([]interface{}, []error) {
	reqs := make([]redis.Conn, len(cmds))
	reusedConn := make(map[string]redis.Conn)
	redisHosts := make(map[string]*RedisHost)
	retryStart := time.Now()
	for i, cmd := range cmds {
		// avoid retry the success cmd while retrying the failed
		if rsps[i] != nil {
			continue
		}
		if errs[i] != nil {
			if _, ok := errs[i].(redis.Error); ok {
				continue
			}
		}
		errs[i] = nil
		node, err := self.cluster.GetNodeHost(cmd.ShardingKey, cmd.ToLeader, false)
		if err != nil {
			levelLog.Infof("command err while get conn: %v, %v", cmd, err)
			errs[i] = err
			continue
		}
		conn, ok := reusedConn[node.addr]
		if !ok {
			conn, err = node.connPool.Get(0, int(cmd.ShardingKey[0]))
			if err != nil {
				levelLog.Infof("command err while get conn: %v, %v", cmd, err)
				errs[i] = err
				node.MaybeIncFailed(err)
				continue
			}
			reusedConn[node.addr] = conn
			redisHosts[node.addr] = node
		}
		reqs[i] = conn
		cost := time.Since(retryStart)
		if cost > time.Millisecond*500 {
			levelLog.Infof("pipeline command get conn %v slow, cost: %v", conn.RemoteAddrStr(), cost)
		}
		if levelLog.Level() > LOG_INFO {
			levelLog.Debugf("command %v send to conn: %v", cmd, conn.RemoteAddrStr())
		}
		errs[i] = conn.Send(cmd.CmdName, cmd.Args...)
	}
	for addr, c := range reusedConn {
		if c != nil {
			err := c.Flush()
			if err != nil {
				for i, reqConn := range reqs {
					if reqConn != nil && reqConn.RemoteAddrStr() == addr {
						errs[i] = err
					}
				}
				redisHost, ok := redisHosts[addr]
				if ok {
					redisHost.MaybeIncFailed(err)
				}
			}
		}
	}

	for i, c := range reqs {
		if c != nil && errs[i] == nil {
			retryStart := time.Now()
			rsps[i], errs[i] = c.Receive()
			cost := time.Since(retryStart)
			if cost > time.Millisecond*500 {
				levelLog.Infof("pipeline command %v to node %v slow, cost: %v", cmds[i], c.RemoteAddrStr(), cost)
			}
			redisHost, ok := redisHosts[c.RemoteAddrStr()]
			if errs[i] != nil {
				if ok {
					redisHost.MaybeIncFailed(errs[i])
				}
			} else {
				if ok {
					redisHost.IncSuccess()
				}
			}
		}
	}
	for _, c := range reusedConn {
		if c != nil {
			c.Close()
		}
	}
	return rsps, errs
}

func (self *ZanRedisClient) FlushAndWaitPipelineCmd(cmds PipelineCmdList) ([]interface{}, []error) {
	retry := uint32(0)
	rsps := make([]interface{}, len(cmds))
	errs := make([]error, len(cmds))
	reqStart := time.Now()
	ro := self.conf.ReadTimeout
	if ro == 0 {
		ro = time.Second
	}
	ro = ro * time.Duration(len(cmds))
	if ro > time.Second*30 {
		ro = time.Second * 30
	}
	for retry < 3 || time.Since(reqStart) < ro {
		retry++
		retryStart := time.Now()
		rsps, errs = self.doPipelineCmd(cmds, rsps, errs)
		cost := time.Since(retryStart)
		if cost > time.Millisecond*time.Duration(200+20*len(cmds)) {
			levelLog.Infof("pipeline command %v slow, cost: %v", len(cmds), cost)
		}
		needRetry := false
		for i, err := range errs {
			if err != nil {
				clusterChanged := self.cluster.MaybeTriggerCheckForError(err, 0)
				if clusterChanged {
					levelLog.Infof("pipeline command err for cluster changed: %v, %v", cmds[i], err)
					needRetry = true
					break
				} else {
					levelLog.Infof("pipeline command err: %v, %v", cmds[i], err)
				}
			}
		}
		if needRetry {
			time.Sleep(MinRetrySleep + MinRetrySleep*time.Duration(2<<retry))
		} else {
			break
		}
	}
	return rsps, errs
}

// DoRedisTryLocalRead should be used only for read command.
// If there is a local dc cluster and read the non-leader is allowed,
// it will try read local dc cluster first.
// If failed to read local dc for 2 times it will retry the primary dc cluster.
func (self *ZanRedisClient) DoRedisTryLocalRead(cmd string, shardingKey []byte, toLeader bool,
	args ...interface{}) (interface{}, error) {
	return self.internalDoRedis(cmd, shardingKey, readCmd, defCmd, toLeader, true, args...)
}

func (self *ZanRedisClient) DoRedisForRead(cmd string, shardingKey []byte, toLeader bool,
	args ...interface{}) (interface{}, error) {
	return self.internalDoRedis(cmd, shardingKey, readCmd, defCmd, toLeader, false, args...)
}

func (self *ZanRedisClient) DoRedisForWrite(cmd string, shardingKey []byte, toLeader bool,
	args ...interface{}) (interface{}, error) {
	return self.internalDoRedis(cmd, shardingKey, writeCmd, defCmd, toLeader, false, args...)
}

func (self *ZanRedisClient) DoRedisForException(cmd string, shardingKey []byte, toLeader bool,
	args ...interface{}) (interface{}, error) {
	return self.internalDoRedis(cmd, shardingKey, anyCmd, exceptionCmd, toLeader, false, args...)
}

func (self *ZanRedisClient) DoRedis(cmd string, shardingKey []byte, toLeader bool,
	args ...interface{}) (interface{}, error) {
	return self.internalDoRedis(cmd, shardingKey, anyCmd, defCmd, toLeader, false, args...)
}

func (self *ZanRedisClient) internalDoRedis(cmd string, shardingKey []byte,
	cmdKind int, ct int, toLeader bool,
	tryLocalRead bool, args ...interface{}) (interface{}, error) {
	if len(shardingKey) == 0 {
		return nil, errors.New("missing sharding key")
	}
	retry := uint32(0)
	retryCnt := 3
	var err error
	var rsp interface{}
	var conn redis.Conn
	var redisHost *RedisHost
	reqStart := time.Now()
	ro := self.conf.ReadTimeout
	if ro == 0 {
		ro = time.Second
	}
	isSlowQuery := IsSlowCmd(cmd)
	if ct == batchCmd {
		isSlowQuery = true
	}
	argsVSize := 0
	if ct == exceptionCmd || len(args) > defaultManyArgsNum {
		if self.largeKeyConf != nil {
			argsVSize = self.largeKeyConf.MaxAllowedValueSize - 1
		}
		retryCnt = 1
	} else {
		if cmdKind == writeCmd && self.largeKeyConf != nil && ct != exceptionCmd &&
			self.largeKeyConf.MaxAllowedValueSize > 0 {
			for i := 1; i < len(args); i++ {
				switch vt := args[i].(type) {
				case []byte:
					argsVSize += len(vt)
				case string:
					argsVSize += len(vt)
				default:
				}
				if argsVSize >= self.largeKeyConf.MaxAllowedValueSize {
					return rsp, ErrSizeExceedLimit
				}
			}
		}
	}
	isLargeKey := false
	if self.largeKeyConf != nil {
		isLargeKey = (argsVSize > self.largeKeyConf.MaxAllowedValueSize/4)
		if isLargeKey {
			levelLog.Infof("command %v-%v large key value: %v, %v", cmd, string(shardingKey), argsVSize, len(args))
		}
	}
	for retry < uint32(retryCnt) || time.Since(reqStart) < ro {
		retry++
		if retry > 2 {
			// fall back to read primary if failed to read local
			if tryLocalRead {
				levelLog.Infof("command %v-%v failed to read local dc, try primary", cmd, string(shardingKey))
				tryLocalRead = false
			}
		}
		retryStart := time.Now()

		cluster := self.cluster
		if argsVSize > 0 {
			redisHost, conn, err = cluster.GetHostAndConnForLarge(shardingKey, toLeader, tryLocalRead, argsVSize)
		} else {
			redisHost, conn, err = cluster.GetHostAndConn(shardingKey, toLeader, tryLocalRead, isSlowQuery)
		}

		if err != nil {
			clusterChanged := cluster.MaybeTriggerCheckForError(err, 0)
			if clusterChanged {
				levelLog.Debugf("command err for cluster changed: %v", cmd)
			} else {
				if isLargeKey {
					// avoid retry for large key or exception key to reduce
					// the affection for normal keys.
					levelLog.Infof("large or exception command err : %v-%v, %v, %v", cmd,
						string(shardingKey), argsVSize, err.Error())
					break
				}
				levelLog.Debugf("command err : %v-%v, %v, %v", cmd, string(shardingKey), err.Error(), argsVSize)
			}
			if redisHost != nil {
				redisHost.MaybeIncFailed(err)
			}
			time.Sleep(MinRetrySleep + MinRetrySleep*time.Duration(2<<retry))
			continue
		}
		remote := conn.RemoteAddrStr()
		cost1 := time.Since(retryStart)
		if cost1 > time.Millisecond*100 {
			levelLog.Infof("command %v-%v slow to get conn %v, cost: %v", cmd,
				string(shardingKey), remote, cost1)
		}
		rsp, err = DoRedisCmd(conn, cmd, args...)
		cost := time.Since(retryStart)
		if cost > time.Millisecond*200 {
			levelLog.Debugf("command %v-%v to node %v slow, cost: %v, get conn cost %v", cmd, string(shardingKey),
				remote, cost, cost1)
		}

		if err != nil {
			clusterChanged := cluster.MaybeTriggerCheckForError(err, 0)
			if clusterChanged {
				levelLog.Debugf("command err for cluster changed: %v, %v, node: %v",
					shardingKey, args, remote)
				// we can retry for cluster error
			} else {
				if _, ok := err.(redis.Error); ok {
					// other error from command reply no need retry
					// can fail fast for some un-recovery error
					break
				}
				if isLargeKey {
					levelLog.Infof("large or exception key err: %v-%v, %v, %v", cmd, string(shardingKey), argsVSize, err.Error())
					break
				}
				if cmdKind == writeCmd && IsTimeoutErr(err) {
					// do not retry on timeout for write to avoid dup write
					break
				}
				levelLog.Debugf("command err : %v-%v, %v, %v", cmd, string(shardingKey), err.Error(), argsVSize)
			}

			redisHost.MaybeIncFailed(err)
			time.Sleep(MinRetrySleep + MinRetrySleep*time.Duration(2<<retry))
		} else {
			redisHost.IncSuccess()
			break
		}
	}
	if self.largeKeyConf != nil {
		brsp, _ := redis.Bytes(rsp, err)
		if len(brsp) > self.largeKeyConf.MaxAllowedValueSize/4 {
			levelLog.Infof("command %v-%v large response: %v", cmd, string(shardingKey), len(brsp))
		}
	}
	return rsp, err
}

func (self *ZanRedisClient) KVGet(set string, key []byte) ([]byte, error) {
	pk := NewPKey(self.conf.Namespace, set, key)
	return redis.Bytes(self.DoRedis("GET", pk.ShardingKey(), true, pk.RawKey))
}

func (self *ZanRedisClient) KVSet(set string, key []byte, value []byte) error {
	pk := NewPKey(self.conf.Namespace, set, key)
	_, err := redis.String(self.DoRedis("SET", pk.ShardingKey(), true, pk.RawKey, value))
	return err
}

func (self *ZanRedisClient) KVDel(set string, key []byte) (int, error) {
	pk := NewPKey(self.conf.Namespace, set, key)
	cnt, err := redis.Int(self.DoRedis("DEL", pk.ShardingKey(), true, pk.RawKey))
	return cnt, err
}

func (self *ZanRedisClient) getPipelinesFromKeys(cmdName string, readLeader bool, pKeys ...*PKey) (int, []int, PipelineCmdList, error) {
	for _, pk := range pKeys {
		if pk.Namespace != self.conf.Namespace {
			return 0, nil, nil, fmt.Errorf("invalid Namespace:%s", pk.Namespace)
		}
	}

	partNum := self.cluster.GetPartitionNum()
	if partNum == 0 {
		return 0, nil, nil, errNoAnyPartitions
	}
	keysPart := make([]int, len(pKeys))

	type packedKeys struct {
		shardingKey []byte
		rawKeys     []interface{}
	}

	partitionKeys := make([]packedKeys, partNum)

	for i, pk := range pKeys {
		partID := GetHashedPartitionID(pk.ShardingKey(), partNum)
		keysPart[i] = partID

		partitionKeys[partID].rawKeys = append(partitionKeys[partID].rawKeys, pk.RawKey)
		partitionKeys[partID].shardingKey = pk.ShardingKey()
	}

	var pipelines PipelineCmdList
	for _, keys := range partitionKeys {
		if len(keys.rawKeys) > 0 {
			pipelines.Add(cmdName, keys.shardingKey, readLeader, keys.rawKeys...)
		}
	}
	return partNum, keysPart, pipelines, nil
}

func (self *ZanRedisClient) KVMDel(readLeader bool, pKeys ...*PKey) (int64, error) {
	_, _, pipelines, err := self.getPipelinesFromKeys("DEL", readLeader, pKeys...)
	if err != nil {
		return 0, err
	}
	rsps, errs := self.FlushAndWaitPipelineCmd(pipelines)

	totalDels := int64(0)
	for i, rsp := range rsps {
		val, err := redis.Int64(rsp, errs[i])
		if err != nil {
			return 0, err
		}
		totalDels += val
	}
	return totalDels, nil
}

func (self *ZanRedisClient) KVMExists(readLeader bool, pKeys ...*PKey) (int64, error) {
	_, _, pipelines, err := self.getPipelinesFromKeys("EXISTS", readLeader, pKeys...)
	if err != nil {
		return 0, err
	}
	rsps, errs := self.FlushAndWaitPipelineCmd(pipelines)

	totalExists := int64(0)
	for i, rsp := range rsps {
		val, err := redis.Int64(rsp, errs[i])
		if err != nil {
			return 0, err
		}
		totalExists += val
	}
	return totalExists, nil
}

func (self *ZanRedisClient) KVMGet(readLeader bool, pKeys ...*PKey) ([][]byte, error) {
	partNum, keysPart, pipelines, err := self.getPipelinesFromKeys("MGET", readLeader, pKeys...)
	if err != nil {
		return nil, err
	}
	rsps, errs := self.FlushAndWaitPipelineCmd(pipelines)

	partitionValues := make([][][]byte, partNum)
	for i, pipelineCmd := range pipelines {
		vals, err := redis.ByteSlices(rsps[i], errs[i])
		if err != nil {
			return nil, err
		} else {
			partID := GetHashedPartitionID(pipelineCmd.ShardingKey, partNum)
			partitionValues[partID] = vals
		}
	}

	resultVals := make([][]byte, len(pKeys))
	partPos := make([]int, partNum)

	for i, partID := range keysPart {
		vals := partitionValues[partID]
		pos := partPos[partID]

		if vals == nil || pos >= len(vals) {
			resultVals[i] = nil
		} else {
			resultVals[i] = vals[pos]
		}

		partPos[partID] = pos + 1
	}

	return resultVals, nil
}

func (self *ZanRedisClient) KVSetNX(set string, key []byte, value []byte) (int, error) {
	pk := NewPKey(self.conf.Namespace, set, key)
	return redis.Int(self.DoRedis("setnx", pk.ShardingKey(), true, pk.RawKey, value))
}

func (client *ZanRedisClient) FullScanChannel(tp, set string, stopC chan struct{}) chan interface{} {
	return client.DoFullScanChannel(tp, set, stopC)
}

func (client *ZanRedisClient) FullScan(tp, set string, count int, cursor []byte) ([]byte, []interface{}, error) {
	return client.DoFullScan("FULLSCAN", tp, set, count, cursor)
}

func (client *ZanRedisClient) AdvScanChannel(tp, set string, stopC chan struct{}) chan []byte {
	return client.DoScanChannel("ADVSCAN", tp, set, stopC)
}

func (client *ZanRedisClient) AdvScan(tp, set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.DoScan("ADVSCAN", tp, set, count, cursor)
}

func (client *ZanRedisClient) KVScanChannel(set string, stopC chan struct{}) chan []byte {
	return client.DoScanChannel("ADVSCAN", "kv", set, stopC)
}

func (client *ZanRedisClient) KVScan(set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.DoScan("ADVSCAN", "kv", set, count, cursor)
}

func (client *ZanRedisClient) doSubScan(cmd, set string, key []byte, count int, cursor []byte) ([]byte, [][]byte, error) {
	pk := NewPKey(client.conf.Namespace, set, key)
	rsp, err := redis.Values(client.DoRedis(cmd, pk.ShardingKey(), true, pk.RawKey, cursor, "count", count))
	if err != nil {
		return nil, nil, err
	}
	if len(rsp) != 2 {
		return nil, nil, errors.New("invalid response")
	}

	cursor, ok := rsp[0].([]byte)
	if !ok {
		return nil, nil, errors.New("invalid response")
	}
	vals, ok := rsp[1].([]interface{})
	if !ok {
		return nil, nil, errors.New("invalid response")
	}
	elems := make([][]byte, 0, len(vals))
	for _, v := range vals {
		e, ok := v.([]byte)
		if !ok {
			return nil, nil, errors.New("invalid response")
		}
		elems = append(elems, e)
	}
	return cursor, elems, nil
}

func (client *ZanRedisClient) doSubScanChannel(cmd, set string, key []byte, stopC chan struct{}) chan []byte {
	retCh := make(chan []byte, 100)
	go func() {
		cursor := []byte("")
		var elems [][]byte
		var err error
		defer func() {
			close(retCh)
		}()
		for {
			cursor, elems, err = client.doSubScan(cmd, set, key, 50, cursor)
			if err != nil {
				levelLog.Warningf("scan error:%v", err.Error())
				break
			}
			for _, e := range elems {
				select {
				case retCh <- e:
				case <-stopC:
					break
				}
			}
			if len(cursor) == 0 {
				break
			}
		}
	}()
	return retCh
}

type HashElem struct {
	Field []byte
	Value []byte
}

func (client *ZanRedisClient) HScanChannel(set string, key []byte, stopC chan struct{}) chan []byte {
	return client.doSubScanChannel("HSCAN", set, key, stopC)
}

func (client *ZanRedisClient) HScan(set string, key []byte, count int, cursor []byte) ([]byte, []HashElem, error) {
	ncursor, fvs, err := client.doSubScan("HSCAN", set, key, count, cursor)
	if err != nil {
		return nil, nil, err
	}
	hes := make([]HashElem, 0, len(fvs)/2)
	for i := 0; i < len(fvs)-1; i += 2 {
		hes = append(hes, HashElem{fvs[i], fvs[i+1]})
	}
	return ncursor, hes, nil
}

func (client *ZanRedisClient) SScanChannel(set string, key []byte, stopC chan struct{}) chan []byte {
	return client.doSubScanChannel("SSCAN", set, key, stopC)
}

func (client *ZanRedisClient) SScan(set string, key []byte, count int, cursor []byte) ([]byte, [][]byte, error) {
	return client.doSubScan("SSCAN", set, key, count, cursor)
}

type ZSetElem struct {
	Member []byte
	Score  float64
}

func (client *ZanRedisClient) ZScanChannel(set string, key []byte, stopC chan struct{}) chan ZSetElem {
	retCh := make(chan ZSetElem, 100)
	go func() {
		cursor := []byte("")
		var elems []ZSetElem
		var err error
		defer func() {
			close(retCh)
		}()
		for {
			cursor, elems, err = client.ZScan(set, key, 50, cursor)
			if err != nil {
				levelLog.Warningf("scan error:%v", err.Error())
				break
			}
			for _, e := range elems {
				select {
				case retCh <- e:
				case <-stopC:
					break
				}
			}
			if len(cursor) == 0 {
				break
			}
		}
	}()
	return retCh
}

func (client *ZanRedisClient) ZScan(set string, key []byte, count int, cursor []byte) ([]byte, []ZSetElem, error) {
	ncursor, elems, err := client.doSubScan("ZSCAN", set, key, count, cursor)
	if err != nil {
		return nil, nil, err
	}
	zses := make([]ZSetElem, 0, len(elems)/2)
	for i := 0; i < len(elems)-1; i += 2 {
		var e ZSetElem
		e.Member = elems[i]
		e.Score, err = strconv.ParseFloat(string(elems[i+1]), 64)
		if err != nil {
			return nil, nil, err
		}
		zses = append(zses, e)
	}
	return ncursor, zses, nil
}

func (client *ZanRedisClient) DoScan(cmd, tp, set string, count int, cursor []byte) ([]byte, [][]byte, error) {
	retry := uint32(0)
	var err error
	var rsps []interface{}
	var conns []redis.Conn
	var wg sync.WaitGroup
	connCursorMap := make(map[string]string)
	var mutex sync.Mutex
	ns := client.conf.Namespace
	for retry < 3 {
		retry++
		if len(cursor) == 0 {
			conns, err = client.cluster.GetConnsForAllParts(true)
			conns = filterDuplicateConn(conns)
		} else {
			decodedCursor, err := base64.StdEncoding.DecodeString(string(cursor))
			if err != nil {
				return nil, nil, err
			}

			cursors := bytes.Split(decodedCursor, []byte("|"))
			if len(cursors) <= 0 {
				return nil, nil, errors.New("invalid cursor")
			}

			var hosts []string
			for _, c := range cursors {
				if len(c) == 0 {
					continue
				}
				splits := bytes.Split(c, []byte("@"))
				if len(splits) != 2 {
					return nil, nil, errors.New("invalid cursor")
				}
				hosts = append(hosts, string(splits[0]))
				connCursorMap[string(splits[0])] = string(splits[1])
			}
			conns, err = client.cluster.GetConnsByHosts(hosts, true)
		}
		if err != nil {
			client.cluster.MaybeTriggerCheckForError(err, 0)
			time.Sleep(MinRetrySleep + MinRetrySleep*time.Duration(2<<retry))
			continue
		}
		rsps = make([]interface{}, len(conns))
		for i, c := range conns {
			wg.Add(1)
			go func(index int, c redis.Conn) {
				cnt := 0
				defer func() {
					levelLog.Infof("scan done on do scan :%v, cnt: %v", c.RemoteAddrStr(), cnt)
					wg.Done()
				}()
				mutex.Lock()
				cur := connCursorMap[c.RemoteAddrStr()]
				mutex.Unlock()
				var tmp bytes.Buffer
				tmp.WriteString(ns)
				tmp.WriteString(":")
				tmp.WriteString(set)
				tmp.WriteString(":")
				tmp.WriteString(cur)
				ay, err := redis.Values(c.Do(cmd, tmp.Bytes(), tp, "count", count))

				if err == nil &&
					len(ay) == 2 {
					originCursor := ay[0].([]byte)
					if len(originCursor) != 0 {
						var cursor []byte
						cursor = append(cursor, []byte(c.RemoteAddrStr())...)
						cursor = append(cursor, []byte("@")...)
						cursor = append(cursor, originCursor...)
						ay[0] = cursor
					}
					rsps[index] = ay
					cnt += len(ay[1].([]interface{}))
				}
			}(i, c)
		}
		wg.Wait()
		break
	}
	var newCursor []byte
	var keys [][]byte
	first := true
	for _, rsp := range rsps {
		if rsp == nil {
			continue
		}
		if len(rsp.([]interface{})) != 2 {
			continue
		}
		r := rsp.([]interface{})
		c := r[0].([]byte)
		if len(c) != 0 {
			if !first {
				newCursor = append(newCursor, '|')
			} else {
				first = false
			}
			newCursor = append(newCursor, c...)
		}
		for _, k := range r[1].([]interface{}) {
			newKey := k.([]byte)
			keys = append(keys, newKey)
		}
	}
	encodedCursor := base64.StdEncoding.EncodeToString(newCursor)
	return []byte(encodedCursor), keys, err
}

func (client *ZanRedisClient) DoScanChannel(cmd, tp, set string, stopC chan struct{}) chan []byte {
	ch := make(chan []byte, 10)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				levelLog.Errorf("scan panic error. [err=%v]\n", err)
			}
			close(ch)
		}()
		retry := uint32(0)
		var err error
		var conns []redis.Conn
		ns := client.conf.Namespace
		for retry < 3 {
			retry++
			conns, err = client.cluster.GetConnsForAllParts(true)
			conns = filterDuplicateConn(conns)
			if err != nil {
				client.cluster.MaybeTriggerCheckForError(err, 0)
				select {
				case <-stopC:
					break
				default:
				}
				time.Sleep(MinRetrySleep + MinRetrySleep*time.Duration(2<<retry))
				continue
			}
			var wg sync.WaitGroup
			for _, c := range conns {
				wg.Add(1)
				go func(c redis.Conn) {
					cnt := 0
					defer func() {
						levelLog.Infof("scan done on do scan channel:%v, cnt: %v", c.RemoteAddrStr(), cnt)
						if err := recover(); err != nil {
							levelLog.Errorf("scan panic error. [err=%v]\n", err)
						}
						wg.Done()
					}()
					cursor := []byte("")
					levelLog.Infof("scan on :%v", c.RemoteAddrStr())

					var tmp bytes.Buffer
					for {
						tmp.Truncate(0)
						tmp.WriteString(ns)
						tmp.WriteString(":")
						tmp.WriteString(set)
						tmp.WriteString(":")
						tmp.Write(cursor)
						ay, err := redis.Values(c.Do(cmd, tmp.Bytes(), tp, "count", 100))
						if err != nil {
							levelLog.Errorf("get error when DoScanChannel. [err=%v]\n", err)
							break
						}
						if len(ay) != 2 {
							levelLog.Errorf("response length is not 2 when DoScanChannel. [len=%d]\n", len(ay))
							break
						}

						cursor = ay[0].([]byte)
						for _, res := range ay[1].([]interface{}) {
							val := res.([]byte)
							select {
							case <-stopC:
								break
							case ch <- val:
								cnt++
							}
						}

						if len(cursor) == 0 {
							break
						}
					}
				}(c)
			}
			wg.Wait()
			break
		}
	}()
	return ch
}

func (client *ZanRedisClient) DoFullScanChannel(tp, set string, stopC chan struct{}) chan interface{} {
	ch := make(chan interface{}, 10)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				levelLog.Errorf("scan error. [err=%v]\n", err)
			}
			close(ch)
		}()
		retry := uint32(0)
		var err error
		var conns []redis.Conn
		ns := client.conf.Namespace
		for retry < 3 {
			retry++
			conns, err = client.cluster.GetConnsForAllParts(true)
			conns = filterDuplicateConn(conns)
			if err != nil {
				client.cluster.MaybeTriggerCheckForError(err, 0)
				select {
				case <-stopC:
					break
				default:
				}
				time.Sleep(MinRetrySleep + MinRetrySleep*time.Duration(2<<retry))
				continue
			}

			var wg sync.WaitGroup
			for _, c := range conns {
				wg.Add(1)
				go func(c redis.Conn) {
					defer func() {
						//levelLog.Infof("scan done on :%v, cnt: %v", c.RemoteAddrStr(), cnt)
						if err := recover(); err != nil {
							levelLog.Errorf("full scan error. [err=%v]\n", err)
						}
						wg.Done()
					}()
					cursor := []byte("")

					var tmp bytes.Buffer
					for {
						tmp.Truncate(0)
						tmp.WriteString(ns)
						tmp.WriteString(":")
						tmp.WriteString(set)
						tmp.WriteString(":")
						tmp.Write(cursor)
						ay, err := redis.Values(c.Do("FULLSCAN", tmp.Bytes(), tp, "count", 1000))
						if err != nil {
							levelLog.Errorf("get error when DoFullScan. [err=%v]\n", err)
							break
						}
						if len(ay) != 2 {
							levelLog.Errorf("response length is not 2 when DoFullScan. [len=%d]\n", len(ay))
							break
						}

						cursor = ay[0].([]byte)
						a, err := redis.MultiBulk(ay[1], nil)
						if err != nil {
							levelLog.Errorf("get error. [Err=%v]\n", err)
							break
						}

						select {
						case <-stopC:
							break
						case ch <- a:
						}

						if len(cursor) == 0 {
							break
						}
					}
				}(c)
			}
			wg.Wait()
			break
		}
	}()
	return ch
}

func (client *ZanRedisClient) DoFullScan(cmd, tp, set string, count int, cursor []byte) ([]byte, []interface{}, error) {
	retry := uint32(0)
	var err error
	var rsps []interface{}
	var conns []redis.Conn
	var wg sync.WaitGroup
	connCursorMap := make(map[string]string)
	ns := client.conf.Namespace
	for retry < 3 {
		retry++

		if len(cursor) == 0 {
			conns, err = client.cluster.GetConnsForAllParts(true)
			conns = filterDuplicateConn(conns)
		} else {
			decodedCursor, err := base64.StdEncoding.DecodeString(string(cursor))
			if err != nil {
				return nil, nil, err
			}

			cursors := bytes.Split(decodedCursor, []byte("|"))
			if len(cursors) <= 0 {
				return nil, nil, errors.New("invalid cursor")
			}

			hosts := make([]string, 0)
			for _, c := range cursors {
				if len(c) == 0 {
					continue
				}
				splits := bytes.Split(c, []byte("@"))
				if len(splits) != 2 {
					return nil, nil, errors.New("invalid cursor")
				}
				hosts = append(hosts, string(splits[0]))
				connCursorMap[string(splits[0])] = string(splits[1])
			}
			conns, err = client.cluster.GetConnsByHosts(hosts, true)
		}
		if err != nil {
			client.cluster.MaybeTriggerCheckForError(err, 0)
			time.Sleep(MinRetrySleep + MinRetrySleep*time.Duration(2<<retry))
			continue
		}

		rsps = make([]interface{}, len(conns))
		for i, c := range conns {
			wg.Add(1)
			go func(index int, c redis.Conn) {
				cnt := 0
				defer func() {
					levelLog.Infof("scan done on dofull scan :%v, cnt: %v", c.RemoteAddrStr(), cnt)
					wg.Done()
				}()
				cur := connCursorMap[c.RemoteAddrStr()]
				var tmp bytes.Buffer
				tmp.WriteString(ns)
				tmp.WriteString(":")
				tmp.WriteString(set)
				tmp.WriteString(":")
				tmp.WriteString(cur)
				ay, err := redis.Values(c.Do(cmd, tmp.Bytes(), tp, "count", count))
				if err == nil &&
					len(ay) == 2 {
					originCursor := ay[0].([]byte)
					if len(originCursor) != 0 {
						var cursor []byte
						cursor = append(cursor, []byte(c.RemoteAddrStr())...)
						cursor = append(cursor, []byte("@")...)
						cursor = append(cursor, originCursor...)
						ay[0] = cursor
					}
					rsps[index] = ay
					cnt += len(ay[1].([]interface{}))
				}
			}(i, c)
		}
		wg.Wait()
		break
	}
	var newCursor []byte
	var result []interface{}

	for _, rsp := range rsps {
		if rsp == nil || len(rsp.([]interface{})) != 2 {
			continue
		}

		r := rsp.([]interface{})
		c := r[0].([]byte)

		if len(newCursor) > 0 {
			newCursor = append(newCursor, '|')
		}
		newCursor = append(newCursor, c...)
		result = append(result, r[1].([]interface{})...)
	}

	encodedCursor := make([]byte, base64.StdEncoding.EncodedLen(len(newCursor)))
	base64.StdEncoding.Encode(encodedCursor, newCursor)

	return encodedCursor, result, err
}

func IsSlowCmd(cmd string) bool {
	lcmd := strings.ToLower(cmd)
	if _, ok := slowCmds[lcmd]; ok {
		return true
	}
	cmdLen := len(cmd)
	if cmdLen >= 4 && (lcmd[cmdLen-4:] == "scan") {
		return true
	}
	if cmdLen >= len("revrange") && strings.Contains(lcmd, "revrange") {
		return true
	}
	return false
}

var slowCmds = map[string]bool{
	"hgetall":  true,
	"hkeys":    true,
	"hvals":    true,
	"keys":     true,
	"smembers": true,
}
