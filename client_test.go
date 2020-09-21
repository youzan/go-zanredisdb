package zanredisdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/absolute8511/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func TestClientMget(t *testing.T) {
	conf := &Conf{
		DialTimeout:  time.Second * 2,
		ReadTimeout:  time.Second * 2,
		WriteTimeout: time.Second * 2,
		TendInterval: 1,
		Namespace:    testNS,
	}
	conf.LookupList = append(conf.LookupList, pdAddr)
	logLevel := int32(2)
	if testing.Verbose() {
		logLevel = 3
	}
	SetLogger(logLevel, newTestLogger(t))
	zanClient, err := NewZanRedisClient(conf)
	assert.Nil(t, err)
	zanClient.Start()
	defer zanClient.Stop()
	time.Sleep(time.Second)

	partNum := zanClient.cluster.GetPartitionNum()
	mutipleKeysNum := []int{partNum - 2, partNum, partNum * 5}

	for _, keysNum := range mutipleKeysNum {
		if keysNum < 0 {
			keysNum = 1
		}

		testPrefix := "rw" + strconv.FormatUint(rand.New(rand.NewSource(time.Now().Unix())).Uint64(), 36)
		testKeys := make([]*PKey, 0, keysNum)
		testValues := make([][]byte, 0, keysNum)
		var writePipelines PipelineCmdList

		for i := 0; i < keysNum; i++ {
			pk := NewPKey(conf.Namespace, "unittest", []byte(testPrefix+strconv.Itoa(i)))
			testKeys = append(testKeys, pk)
			rawKey := pk.RawKey
			testValue := pk.ShardingKey()
			writePipelines.Add("SET", pk.ShardingKey(), true, rawKey, testValue)
			testValues = append(testValues, testValue)
		}
		rsps, errs := zanClient.FlushAndWaitPipelineCmd(writePipelines)
		if len(rsps) != len(writePipelines) {
			t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(writePipelines))
		}
		t.Logf("test values: %v", len(testValues))
		for i, rsp := range rsps {
			if errs[i] != nil {
				t.Errorf("rsp error: %v", errs[i])
			}
			_, err := redis.String(rsp, errs[i])
			if err != nil {
				t.Error(err)
			}
		}
		mgetVals, err := zanClient.KVMGet(true, testKeys...)
		if err != nil {
			t.Errorf("failed mget: %v", err)
		}
		if len(mgetVals) != len(testKeys) {
			t.Errorf("mget count mismatch: %v %v", len(mgetVals), len(testKeys))
		}
		for i, val := range mgetVals {
			t.Log(string(val))
			if !bytes.Equal(val, testValues[i]) {
				t.Errorf("should equal: %v, %v", val, string(testValues[i]))
			}
		}
		var delPipelines PipelineCmdList
		for i := 0; i < len(testKeys); i++ {
			delPipelines.Add("DEL", testKeys[i].ShardingKey(), true, testKeys[i].RawKey)
		}
		rsps, errs = zanClient.FlushAndWaitPipelineCmd(delPipelines)
		if len(rsps) != len(delPipelines) {
			t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(delPipelines))
		}

		mgetVals, err = zanClient.KVMGet(true, testKeys...)
		if err != nil {
			t.Errorf("failed mget: %v", err)
		}
		if len(mgetVals) != len(testKeys) {
			t.Errorf("mget count mismatch: %v %v", len(mgetVals), len(testKeys))
		}
		t.Log(mgetVals)
		for _, val := range mgetVals {
			if len(val) > 0 {
				t.Errorf("should empty after del: %v", val)
			}
		}
	}
}

func TestClientMExistsAndDel(t *testing.T) {
	conf := &Conf{
		DialTimeout:  time.Second * 2,
		ReadTimeout:  time.Second * 2,
		WriteTimeout: time.Second * 2,
		TendInterval: 1,
		Namespace:    testNS,
	}
	conf.LookupList = append(conf.LookupList, pdAddr)
	logLevel := int32(2)
	if testing.Verbose() {
		logLevel = 3
	}
	SetLogger(logLevel, newTestLogger(t))
	zanClient, err := NewZanRedisClient(conf)
	assert.Nil(t, err)
	zanClient.Start()
	defer zanClient.Stop()
	time.Sleep(time.Second)

	partNum := zanClient.cluster.GetPartitionNum()
	mutipleKeysNum := []int{partNum - 2, partNum, partNum * 5}

	for _, keysNum := range mutipleKeysNum {
		if keysNum < 0 {
			keysNum = 1
		}

		testPrefix := "rw" + strconv.FormatUint(rand.New(rand.NewSource(time.Now().Unix())).Uint64(), 36)
		testKeys := make([]*PKey, 0, keysNum)
		testValues := make([][]byte, 0, keysNum)
		var writePipelines PipelineCmdList

		for i := 0; i < keysNum; i++ {
			pk := NewPKey(conf.Namespace, "unittest", []byte(testPrefix+strconv.Itoa(i)))
			testKeys = append(testKeys, pk)
			rawKey := pk.RawKey
			testValue := pk.ShardingKey()
			writePipelines.Add("SET", pk.ShardingKey(), true, rawKey, testValue)
			testValues = append(testValues, testValue)
		}

		existsCnt, err := zanClient.KVMExists(true, testKeys...)
		if err != nil {
			t.Errorf("failed exists: %v", err)
		}
		if existsCnt != int64(0) {
			t.Errorf("exists count %v should be 0 for init", existsCnt)
			return
		}

		rsps, errs := zanClient.FlushAndWaitPipelineCmd(writePipelines)
		if len(rsps) != len(writePipelines) {
			t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(writePipelines))
		}
		t.Logf("test values: %v", len(testValues))
		for i, rsp := range rsps {
			if errs[i] != nil {
				t.Errorf("rsp error: %v", errs[i])
			}
			_, err := redis.String(rsp, errs[i])
			if err != nil {
				t.Error(err)
			}
		}

		existsCnt, err = zanClient.KVMExists(true, testKeys...)
		if err != nil {
			t.Errorf("failed exists: %v", err)
		}
		if existsCnt != int64(len(testKeys)) {
			t.Errorf("exists count %v mismatch keys: %v", existsCnt, len(testKeys))
		}

		delCnt, err := zanClient.KVMDel(true, testKeys...)
		if err != nil {
			t.Errorf("failed del: %v", err)
		}
		if delCnt != int64(len(testKeys)) {
			t.Errorf("del cnt %v should equal keys: %v", delCnt, len(testKeys))
		}

		existsCnt, err = zanClient.KVMExists(true, testKeys...)
		if err != nil {
			t.Errorf("failed exists: %v", err)
		}
		if existsCnt != int64(0) {
			t.Errorf("exists count %v mismatch keys: %v", existsCnt, len(testKeys))
		}

		delCnt, err = zanClient.KVMDel(true, testKeys...)
		if err != nil {
			t.Errorf("failed del: %v", err)
		}
		if delCnt != int64(0) {
			t.Errorf("del cnt %v should be 0 after deleted", delCnt)
		}
	}
}

func TestClientExceptionKey(t *testing.T) {
	testClientLargeKey(t, true)
}

func TestClientLargeKey(t *testing.T) {
	testClientLargeKey(t, false)
}

func testClientLargeKey(t *testing.T, exception bool) {
	conf := &Conf{
		DialTimeout:   time.Second * 2,
		ReadTimeout:   time.Second * 2,
		WriteTimeout:  time.Second * 2,
		TendInterval:  1,
		Namespace:     testNS,
		MaxActiveConn: 32,
	}
	conf.LookupList = append(conf.LookupList, pdAddr)
	largeConf := NewLargeKeyConf()
	largeConf.MaxAllowedValueSize = 1024

	logLevel := int32(2)
	if testing.Verbose() {
		logLevel = 3
	}
	SetLogger(logLevel, newTestLogger(t))
	zanClient, err := NewZanRedisClient(conf)
	zanClient.SetLargeKeyConf(largeConf)
	assert.Nil(t, err)
	zanClient.Start()
	defer zanClient.Stop()
	time.Sleep(time.Second)

	largeK1Value := make([]byte, largeConf.MaxAllowedValueSize)
	_, err = zanClient.DoRedisForWrite("SET", []byte("testpk"), true, []byte("testpk"), largeK1Value)
	assert.Equal(t, ErrSizeExceedLimit, err)
	largeKVs := make([][]byte, 0)
	largeKVs = append(largeKVs, make([]byte, len(largeK1Value)-10))
	for i := 1; i < 5; i++ {
		largeV := make([]byte, len(largeK1Value)/(i*2)-10)
		largeKVs = append(largeKVs, largeV)
	}

	partNum := zanClient.cluster.GetPartitionNum()
	mutipleKeysNum := []int{partNum - 2, partNum, partNum * 5}

	for _, keysNum := range mutipleKeysNum {
		if keysNum < 0 {
			keysNum = 1
		}

		testPrefix := "rw" + strconv.FormatUint(rand.New(rand.NewSource(time.Now().Unix())).Uint64(), 36)
		testKeys := make([]*PKey, 0, keysNum)
		testValues := make([][]byte, 0, keysNum)

		for i := 0; i < keysNum; i++ {
			pk := NewPKey(conf.Namespace, "unittest", []byte(testPrefix+strconv.Itoa(i)))
			testKeys = append(testKeys, pk)
			rawKey := pk.RawKey
			testValue := largeKVs[i%len(largeKVs)]
			t.Logf("test set large value size: %d", len(testValue))
			if exception {
				_, err := zanClient.DoRedisForException("SET", pk.ShardingKey(), true, rawKey, testValue)
				assert.Nil(t, err)
			} else {
				_, err := zanClient.DoRedisForWrite("SET", pk.ShardingKey(), true, rawKey, testValue)
				assert.Nil(t, err)
			}
			testValues = append(testValues, testValue)
		}
		t.Logf("test values: %v", len(testValues))
		for index, pk := range testKeys {
			rsp, err := redis.Bytes(zanClient.DoRedis("GET", pk.ShardingKey(), true, pk.RawKey))
			assert.Nil(t, err)
			assert.Equal(t, testValues[index], rsp)
		}
		mgetVals, err := zanClient.KVMGet(true, testKeys...)
		if err != nil {
			t.Errorf("failed mget: %v", err)
		}
		if len(mgetVals) != len(testKeys) {
			t.Errorf("mget count mismatch: %v %v", len(mgetVals), len(testKeys))
		}
		for i, val := range mgetVals {
			t.Log(string(val))
			if !bytes.Equal(val, testValues[i]) {
				t.Errorf("should equal: %v, %v", val, string(testValues[i]))
			}
		}
		var delPipelines PipelineCmdList
		for i := 0; i < len(testKeys); i++ {
			delPipelines.Add("DEL", testKeys[i].ShardingKey(), true, testKeys[i].RawKey)
		}
		rsps, _ := zanClient.FlushAndWaitPipelineCmd(delPipelines)
		if len(rsps) != len(delPipelines) {
			t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(delPipelines))
		}

		mgetVals, err = zanClient.KVMGet(true, testKeys...)
		if err != nil {
			t.Errorf("failed mget: %v", err)
		}
		if len(mgetVals) != len(testKeys) {
			t.Errorf("mget count mismatch: %v %v", len(mgetVals), len(testKeys))
		}
		t.Log(mgetVals)
		for _, val := range mgetVals {
			if len(val) > 0 {
				t.Errorf("should empty after del: %v", val)
			}
		}
	}

	hss := zanClient.cluster.GetHostStats()
	d, _ := json.Marshal(hss)
	t.Logf("host stats: %v", string(d))
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 20; k++ {
				pk := NewPKey(conf.Namespace, "unittest", []byte("largekv_concurrent"+strconv.Itoa(k)))
				rawKey := pk.RawKey
				for _, v := range largeKVs {
					if exception {
						zanClient.DoRedisForException("SET", pk.ShardingKey(), true, rawKey, v)
					} else {
						zanClient.DoRedisForWrite("SET", pk.ShardingKey(), true, rawKey, v)
					}
				}
				_, err = zanClient.DoRedis("DEL", pk.ShardingKey(), true, rawKey)
			}
		}()
	}
	wg.Wait()
	hss = zanClient.cluster.GetHostStats()
	d, _ = json.Marshal(hss)
	t.Logf("host stats after concurrent: %v", string(d))
	for _, hs := range hss {
		assert.Equal(t, 1, hs.LargeKeyPoolCnt[0])
		assert.True(t, hs.LargeKeyPoolCnt[1] <= 2)
		assert.True(t, hs.LargeKeyPoolCnt[2] <= 4)
	}
}

func TestClientPipeline(t *testing.T) {
	conf := &Conf{
		DialTimeout:  time.Second * 2,
		ReadTimeout:  time.Second * 2,
		WriteTimeout: time.Second * 2,
		TendInterval: 1,
		Namespace:    testNS,
	}
	conf.LookupList = append(conf.LookupList, pdAddr)
	logLevel := int32(2)
	if testing.Verbose() {
		logLevel = 3
	}
	SetLogger(logLevel, newTestLogger(t))
	zanClient, err := NewZanRedisClient(conf)
	assert.Nil(t, err)
	zanClient.Start()
	defer zanClient.Stop()
	time.Sleep(time.Second)

	partNum := zanClient.cluster.GetPartitionNum()
	mutipleKeysNum := []int{partNum - 2, partNum, partNum * 5}

	for _, keysNum := range mutipleKeysNum {
		if keysNum < 0 {
			keysNum = 1
		}
		testPrefix := "rw" + strconv.FormatUint(rand.New(rand.NewSource(time.Now().Unix())).Uint64(), 36)
		testValues := make([][]byte, 0, keysNum)
		var writePipelines PipelineCmdList

		for i := 0; i < keysNum; i++ {
			pk := NewPKey(conf.Namespace, "unittest", []byte(testPrefix+strconv.Itoa(i)))
			rawKey := pk.RawKey
			testValue := pk.ShardingKey()
			writePipelines.Add("SET", pk.ShardingKey(), true, rawKey, testValue)
			testValues = append(testValues, testValue)
		}
		rsps, errs := zanClient.FlushAndWaitPipelineCmd(writePipelines)
		if len(rsps) != len(writePipelines) {
			t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(writePipelines))
		}
		t.Logf("test values: %v", len(testValues))
		for i, rsp := range rsps {
			if errs[i] != nil {
				t.Errorf("rsp error: %v", errs[i])
			}
			_, err := redis.String(rsp, errs[i])
			if err != nil {
				t.Fatal(err)
			}
		}
		var getPipelines PipelineCmdList
		for i := 0; i < len(testValues); i++ {
			pk := NewPKey(conf.Namespace, "unittest", []byte(testPrefix+strconv.Itoa(i)))
			rawKey := pk.RawKey
			getPipelines.Add("GET", pk.ShardingKey(), true, rawKey)
		}
		rsps, errs = zanClient.FlushAndWaitPipelineCmd(getPipelines)
		if len(rsps) != len(getPipelines) {
			t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(getPipelines))
		}
		for i, rsp := range rsps {
			if errs[i] != nil {
				t.Errorf("rsp error: %v", errs[i])
			}
			value, err := redis.Bytes(rsp, errs[i])
			if err != nil {
				t.Error(err)
			} else if !bytes.Equal(value, testValues[i]) {
				t.Errorf("should equal: %v, %v", value, testValues[i])
			}
		}
		var delPipelines PipelineCmdList
		for i := 0; i < len(testValues); i++ {
			pk := NewPKey(conf.Namespace, "unittest", []byte(testPrefix+strconv.Itoa(i)))
			rawKey := pk.RawKey
			delPipelines.Add("DEL", pk.ShardingKey(), true, rawKey)
		}
		rsps, errs = zanClient.FlushAndWaitPipelineCmd(delPipelines)
		if len(rsps) != len(delPipelines) {
			t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(delPipelines))
		}
		rsps, errs = zanClient.FlushAndWaitPipelineCmd(getPipelines)
		if len(rsps) != len(getPipelines) {
			t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(getPipelines))
		}
		for i, rsp := range rsps {
			if errs[i] != nil {
				t.Errorf("rsp error: %v", errs[i])
			}
			value, err := redis.Bytes(rsp, errs[i])
			if err != redis.ErrNil && len(value) > 0 {
				t.Fatalf("should be deleted:%v, value:%v", err, string(value))
			}
		}
	}
}

func TestClientScan(t *testing.T) {
	conf := &Conf{
		DialTimeout:  time.Second * 2,
		ReadTimeout:  time.Second * 2,
		WriteTimeout: time.Second * 2,
		TendInterval: 1,
		Namespace:    testNS,
	}
	conf.LookupList = append(conf.LookupList, pdAddr)
	logLevel := int32(2)
	if testing.Verbose() {
		logLevel = 3
	}
	SetLogger(logLevel, newTestLogger(t))
	zanClient, err := NewZanRedisClient(conf)
	assert.Nil(t, err)
	zanClient.Start()
	defer zanClient.Stop()
	time.Sleep(time.Second)

	testTable := "unittest2"

	partNum := zanClient.cluster.GetPartitionNum()
	mutipleKeysNum := []int{partNum - 2, partNum, partNum * 5}
	// clean old data
	recvCh := zanClient.AdvScanChannel("kv", testTable, nil)
	cnt := 0
	for k := range recvCh {
		t.Logf("advscan clean key: %v", string(k))
		delCnt, err := zanClient.KVDel(testTable, k)
		if err != nil {
			t.Errorf("del failed: %v", err)
		}
		if delCnt != 1 {
			t.Errorf("del failed: %v", delCnt)
			return
		}
		cnt++
	}

	for _, keysNum := range mutipleKeysNum {
		if keysNum < 0 {
			keysNum = 1
		}

		testPrefix := "rw" + strconv.FormatUint(rand.New(rand.NewSource(time.Now().Unix())).Uint64(), 36)
		testKeys := make([]*PKey, 0, keysNum)
		testValues := make([][]byte, 0, keysNum)
		var writePipelines PipelineCmdList

		for i := 0; i < keysNum; i++ {
			pk := NewPKey(conf.Namespace, testTable, []byte(testPrefix+strconv.Itoa(i)))
			testKeys = append(testKeys, pk)
			t.Logf("write key: %v", string(pk.String()))
			rawKey := pk.RawKey
			testValue := pk.ShardingKey()
			writePipelines.Add("SET", pk.ShardingKey(), true, rawKey, testValue)
			testValues = append(testValues, testValue)
		}
		rsps, errs := zanClient.FlushAndWaitPipelineCmd(writePipelines)
		if len(rsps) != len(writePipelines) {
			t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(writePipelines))
		}
		t.Logf("test values: %v", len(testValues))
		for i, rsp := range rsps {
			if errs[i] != nil {
				t.Errorf("rsp error: %v", errs[i])
			}
			_, err := redis.String(rsp, errs[i])
			if err != nil {
				t.Error(err)
			}
		}

		cur, scanKeys, err := zanClient.AdvScan("kv", testTable, len(testKeys), nil)
		if err != nil {
			t.Error(err)
		}
		if len(scanKeys) != len(testKeys) && len(cur) == 0 {
			t.Errorf("advscan keys mismatch:%v, %v, %v", string(cur), len(scanKeys), len(testKeys))
		}
		recvCh := zanClient.AdvScanChannel("kv", testTable, nil)
		cnt := 0
		for k := range recvCh {
			t.Logf("advscan key: %v", string(k))
			cnt++
		}
		if len(testKeys) != cnt {
			t.Errorf("advscan keys mismatch: %v, %v", cnt, len(testKeys))
		}

		cur, scanKVs, err := zanClient.FullScan("kv", testTable, len(testKeys), nil)
		if err != nil {
			t.Error(err)
		}
		if len(scanKVs) != len(testKeys) && len(cur) == 0 {
			t.Errorf("fullscan keys mismatch: %v, %v, %v", string(cur), len(scanKVs), len(testKeys))
		}
		recvKVCh := zanClient.FullScanChannel("kv", testTable, nil)
		cnt = 0
		for k := range recvKVCh {
			items := k.([]interface{})
			for _, item := range items {
				kv := item.([]interface{})
				t.Logf("fullscan key: %v", string(kv[0].([]byte)))
				cnt++
			}
		}
		if len(testKeys) != cnt {
			t.Errorf("fullscan keys mismatch: %v, %v", cnt, len(testKeys))
		}

		var delPipelines PipelineCmdList
		for i := 0; i < len(testKeys); i++ {
			delPipelines.Add("DEL", testKeys[i].ShardingKey(), true, testKeys[i].RawKey)
		}
		rsps, errs = zanClient.FlushAndWaitPipelineCmd(delPipelines)
		if len(rsps) != len(delPipelines) {
			t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(delPipelines))
		}
	}
}

func TestClientSScan(t *testing.T) {
	conf := &Conf{
		DialTimeout:  time.Second * 2,
		ReadTimeout:  time.Second * 2,
		WriteTimeout: time.Second * 2,
		TendInterval: 1,
		Namespace:    testNS,
	}
	conf.LookupList = append(conf.LookupList, pdAddr)
	logLevel := int32(2)
	if testing.Verbose() {
		logLevel = 3
	}
	SetLogger(logLevel, newTestLogger(t))
	zanClient, err := NewZanRedisClient(conf)
	assert.Nil(t, err)
	zanClient.Start()
	defer zanClient.Stop()
	time.Sleep(time.Second)

	testTable := "unittest2-sscan"

	testPrefix := "set-unittest-sscan"

	pk := NewPKey(conf.Namespace, testTable, []byte(testPrefix))
	rawKey := pk.RawKey
	testValues := make([][]byte, 0)

	var writePipelines PipelineCmdList
	for i := 0; i < 100; i++ {
		member := fmt.Sprintf("%04d", i)
		t.Logf("write member: %v", string(member))
		writePipelines.Add("SADD", pk.ShardingKey(), true, rawKey, member)
		testValues = append(testValues, []byte(member))
	}

	rsps, errs := zanClient.FlushAndWaitPipelineCmd(writePipelines)
	if len(rsps) != len(writePipelines) {
		t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(writePipelines))
	}
	t.Logf("test values: %v", len(testValues))
	for i, rsp := range rsps {
		if errs[i] != nil {
			t.Errorf("rsp error: %v", errs[i])
		}
		_, err := redis.Int64(rsp, errs[i])
		if err != nil {
			t.Error(err)
		}
	}
	vv, err := zanClient.DoRedis("SMEMBERS", pk.ShardingKey(), true, rawKey)
	t.Logf("test members: %v", vv)

	cur, scanMembers, err := zanClient.SScan(testTable, []byte(testPrefix), len(testValues), []byte(""))
	if err != nil {
		t.Error(err)
	}
	if len(scanMembers) != len(testValues) && len(cur) == 0 {
		t.Errorf("members mismatch:%v, %v, %v", string(cur), len(scanMembers), len(testValues))
	}
	for i, m := range scanMembers {
		if string(m) != string(testValues[i]) {
			t.Errorf("members mismatch:%v, %v", m, testValues[i])
		}
	}
	recvCh := zanClient.SScanChannel(testTable, []byte(testPrefix), nil)
	cnt := 0
	scanMembers = scanMembers[:0]
	for k := range recvCh {
		t.Logf("member key: %v", string(k))
		cnt++
		scanMembers = append(scanMembers, k)
	}
	if len(testValues) != cnt {
		t.Errorf("mismatch: %v, %v", cnt, len(testValues))
	}

	for i, m := range scanMembers {
		if string(m) != string(testValues[i]) {
			t.Errorf("members mismatch:%v, %v", m, testValues[i])
		}
	}
	_, err = zanClient.DoRedis("SCLEAR", pk.ShardingKey(), true, pk.RawKey)
	if err != nil {
		t.Errorf("rsp err: %v", err)
	}
}
func TestClientHScan(t *testing.T) {
	conf := &Conf{
		DialTimeout:  time.Second * 2,
		ReadTimeout:  time.Second * 2,
		WriteTimeout: time.Second * 2,
		TendInterval: 1,
		Namespace:    testNS,
	}
	conf.LookupList = append(conf.LookupList, pdAddr)
	logLevel := int32(2)
	if testing.Verbose() {
		logLevel = 3
	}
	SetLogger(logLevel, newTestLogger(t))
	zanClient, err := NewZanRedisClient(conf)
	assert.Nil(t, err)
	zanClient.Start()
	defer zanClient.Stop()
	time.Sleep(time.Second)

	testTable := "unittest2-hscan"

	testPrefix := "unittest-hscan-key"

	pk := NewPKey(conf.Namespace, testTable, []byte(testPrefix))
	rawKey := pk.RawKey
	testValues := make([][]byte, 0)

	var writePipelines PipelineCmdList
	for i := 0; i < 10; i++ {
		member := fmt.Sprintf("%04d", i)
		t.Logf("write : %v", string(member))
		writePipelines.Add("HSET", pk.ShardingKey(), true, rawKey, member, member)
		testValues = append(testValues, []byte(member))
	}

	rsps, errs := zanClient.FlushAndWaitPipelineCmd(writePipelines)
	if len(rsps) != len(writePipelines) {
		t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(writePipelines))
	}
	t.Logf("test values: %v", len(testValues))
	for i, rsp := range rsps {
		if errs[i] != nil {
			t.Errorf("rsp error: %v", errs[i])
		}
		_, err := redis.Int64(rsp, errs[i])
		if err != nil {
			t.Error(err)
		}
	}
	vv, err := zanClient.DoRedis("hgetall", pk.ShardingKey(), true, rawKey)
	t.Logf("test fvs: %v", vv)

	cur, scanRets, err := zanClient.HScan(testTable, []byte(testPrefix), len(testValues), []byte(""))
	if err != nil {
		t.Error(err)
	}
	if len(scanRets) != len(testValues) && len(cur) == 0 {
		t.Errorf("scan mismatch:%v, %v, %v", string(cur), len(scanRets), len(testValues))
	}
	if len(scanRets) == len(testValues) {
		for i, m := range scanRets {
			if string(m.Field) != string(testValues[i]) {
				t.Errorf("mismatch:%v, %v", m, testValues[i])
			}
			if string(m.Value) != string(testValues[i]) {
				t.Errorf("mismatch:%v, %v", string(m.Value), testValues[i])
			}
		}
	}
	recvCh := zanClient.HScanChannel(testTable, []byte(testPrefix), nil)
	cnt := 0
	scanRets = scanRets[:0]
	for k := range recvCh {
		t.Logf("member key: %v", string(k))
		cnt++
		var he HashElem
		he.Field = k
		v := <-recvCh
		he.Value = v
		scanRets = append(scanRets, he)
	}
	if len(testValues) != cnt {
		t.Errorf("mismatch: %v, %v", cnt, len(testValues))
	}

	for i, m := range scanRets {
		if string(m.Field) != string(testValues[i]) {
			t.Errorf("mismatch:%v, %v", m, testValues[i])
		}
		if string(m.Value) != string(testValues[i]) {
			t.Errorf("mismatch:%v, %v", m, testValues[i])
		}
	}
	_, err = zanClient.DoRedis("HCLEAR", pk.ShardingKey(), true, pk.RawKey)
	if err != nil {
		t.Errorf("rsp err: %v", err)
	}
}

func TestClientZScan(t *testing.T) {
	conf := &Conf{
		DialTimeout:  time.Second * 2,
		ReadTimeout:  time.Second * 2,
		WriteTimeout: time.Second * 2,
		TendInterval: 1,
		Namespace:    testNS,
	}
	conf.LookupList = append(conf.LookupList, pdAddr)
	logLevel := int32(2)
	if testing.Verbose() {
		logLevel = 3
	}
	SetLogger(logLevel, newTestLogger(t))
	zanClient, err := NewZanRedisClient(conf)
	assert.Nil(t, err)
	zanClient.Start()
	defer zanClient.Stop()
	time.Sleep(time.Second)

	testTable := "unittest2-zscan"

	testPrefix := "unittest-zscan-key"

	pk := NewPKey(conf.Namespace, testTable, []byte(testPrefix))
	rawKey := pk.RawKey
	testValues := make([][]byte, 0)

	var writePipelines PipelineCmdList
	for i := 0; i < 100; i++ {
		member := fmt.Sprintf("%04d", i)
		t.Logf("write : %v", string(member))
		writePipelines.Add("ZADD", pk.ShardingKey(), true, rawKey, i, member)
		testValues = append(testValues, []byte(member))
	}

	rsps, errs := zanClient.FlushAndWaitPipelineCmd(writePipelines)
	if len(rsps) != len(writePipelines) {
		t.Errorf("rsp list should be equal to pipeline num %v vs %v", len(rsps), len(writePipelines))
	}
	t.Logf("test values: %v", len(testValues))
	for i, rsp := range rsps {
		if errs[i] != nil {
			t.Errorf("rsp error: %v", errs[i])
		}
		_, err := redis.Int64(rsp, errs[i])
		if err != nil {
			t.Error(err)
		}
	}

	cur, scanRets, err := zanClient.ZScan(testTable, []byte(testPrefix), len(testValues), []byte(""))
	if err != nil {
		t.Error(err)
	}
	if len(scanRets) != len(testValues) && len(cur) == 0 {
		t.Errorf("scan mismatch:%v, %v, %v", string(cur), len(scanRets), len(testValues))
	}
	if len(scanRets) == len(testValues) {
		for i, m := range scanRets {
			if string(m.Member) != string(testValues[i]) {
				t.Errorf("mismatch:%v, %v", m, testValues[i])
			}
			score, _ := strconv.ParseFloat(string(testValues[i]), 64)
			if m.Score != score {
				t.Errorf("mismatch:%v, %v", m, testValues[i])
			}
		}
	}
	recvCh := zanClient.ZScanChannel(testTable, []byte(testPrefix), nil)
	cnt := 0
	scanRets = scanRets[:0]
	for k := range recvCh {
		t.Logf("member key: %v", k)
		cnt++
		scanRets = append(scanRets, k)
	}
	if len(testValues) != cnt {
		t.Errorf("mismatch: %v, %v", cnt, len(testValues))
	}

	for i, m := range scanRets {
		if string(m.Member) != string(testValues[i]) {
			t.Errorf("mismatch:%v, %v", m, testValues[i])
		}
		score, _ := strconv.ParseFloat(string(testValues[i]), 64)
		if m.Score != score {
			t.Errorf("mismatch:%v, %v", m, testValues[i])
		}
	}
	_, err = zanClient.DoRedis("ZCLEAR", pk.ShardingKey(), true, pk.RawKey)
	if err != nil {
		t.Errorf("rsp err: %v", err)
	}
}

func TestClientReadLocalPrimary(t *testing.T) {
	testClientReadLocal(t, true, false)
}

func TestClientReadLocalNonPrimary(t *testing.T) {
	testClientReadLocal(t, false, false)
}

func TestClientReadFailedLocalNonPrimary(t *testing.T) {
	testClientReadLocal(t, false, true)
}

func testClientReadLocal(t *testing.T, testSameWithPrimary bool, testFailedLocal bool) {
	conf := &Conf{
		DialTimeout:  time.Second * 2,
		ReadTimeout:  time.Second * 2,
		WriteTimeout: time.Second * 2,
		TendInterval: 1,
		Namespace:    testNS,
		DC:           "t1",
	}
	if testSameWithPrimary {
		conf.MultiConf = append(conf.MultiConf, RemoteClusterConf{
			LookupList: []string{pdAddr},
			IsPrimary:  true,
			ClusterDC:  "t1",
		},
			RemoteClusterConf{
				LookupList: []string{pdAddr},
				IsPrimary:  false,
				ClusterDC:  "t2",
			},
		)
	} else {
		failedPD := pdAddr
		if testFailedLocal {
			failedPD = "127.0.0.1:18001"
		}
		conf.MultiConf = append(conf.MultiConf, RemoteClusterConf{
			LookupList: []string{pdAddr},
			IsPrimary:  true,
			ClusterDC:  "t2",
		},
			RemoteClusterConf{
				LookupList: []string{failedPD},
				IsPrimary:  false,
				ClusterDC:  "t1",
			},
		)
	}

	logLevel := int32(2)
	if testing.Verbose() {
		logLevel = 3
	}
	SetLogger(logLevel, newTestLogger(t))
	zanClient, err := NewZanRedisClient(conf)
	assert.Nil(t, err)
	zanClient.Start()
	defer zanClient.Stop()
	time.Sleep(time.Second)
	for i := 0; i < 10; i++ {
		pk := NewPKey(conf.Namespace, "unittest_multiclient", []byte("rw11"+strconv.Itoa(i)))
		rawKey := pk.RawKey
		testValue := pk.ShardingKey()
		_, err := zanClient.DoRedis("DEL", pk.ShardingKey(), true, rawKey)
		value, err := redis.Bytes(zanClient.DoRedisTryLocalRead("GET", pk.ShardingKey(),
			true, rawKey))
		if err != redis.ErrNil && len(value) > 0 {
			t.Fatalf("should be deleted:%v, value:%v", err, value)
		}

		_, err = redis.String(zanClient.DoRedis("SET", pk.ShardingKey(), true, rawKey, testValue))
		if err != nil {
			t.Fatal(err)
		}
		value, err = redis.Bytes(zanClient.DoRedisTryLocalRead("GET", pk.ShardingKey(), true, rawKey))
		if err != nil {
			t.Error(err)
		} else if !bytes.Equal(value, testValue) {
			t.Errorf("should equal: %v, %v", value, testValue)
		}
		zanClient.DoRedis("DEL", pk.ShardingKey(), true, rawKey)
	}
}

func TestClientReadOnNotExistNamespace(t *testing.T) {
	conf := &Conf{
		DialTimeout:  time.Second * 2,
		ReadTimeout:  time.Second * 2,
		WriteTimeout: time.Second * 2,
		MaxConnWait:  time.Second / 4,
		TendInterval: 1,
		Namespace:    "not_exist",
	}
	conf.LookupList = append(conf.LookupList, pdAddr)
	logLevel := int32(2)
	if testing.Verbose() {
		logLevel = 3
	}
	SetLogger(logLevel, newTestLogger(t))
	zanClient, err := NewZanRedisClient(conf)
	assert.Nil(t, err)
	zanClient.Start()
	defer zanClient.Stop()
	time.Sleep(time.Second)

	s := time.Now()
	_, err = zanClient.KVGet("testset", []byte("getnokey"))
	assert.Equal(t, errNoAnyPartitions, err)
	cost := time.Since(s)
	t.Logf("cost %s", cost)
	assert.True(t, cost < conf.MaxConnWait*2)
}
