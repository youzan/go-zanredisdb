package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/redigo/redis"
	"github.com/youzan/go-zanredisdb"
)

var ip = flag.String("ip", "127.0.0.1", "pd server ip")
var port = flag.Int("port", 18001, "pd server port")
var number = flag.Int("n", 1000, "request number")
var keyNumber = flag.Int("keyn", 1000, "pkey*subkey for hash, list, set, zset, should large than pkn")
var dc = flag.String("dcinfo", "", "the dc info for this client")
var useLeader = flag.Bool("leader", true, "whether force only send request to leader, otherwise chose any node in the same dc first")
var logSlow = flag.Int("logslow", 20, "slow level to log")
var clients = flag.Int("c", 10, "number of clients")
var round = flag.Int("r", 1, "benchmark round number")
var logLevel = flag.Int("loglevel", 1, "log level")
var valueSize = flag.Int("vsize", 100, "kv value size")
var tests = flag.String("t", "set,get", "only run the comma separated list of tests(supported randget,del,lpush,rpush,lrange,lpop,rpop,hset,randhget,hget,hdel,sadd,sismember,srem,zadd,zrange,zrevrange,zdel,zrem, zremrangebyscore)")
var primaryKeyCnt = flag.Int("pkn", 100, "primary key count for kv,hash,list,set,zset")
var namespace = flag.String("namespace", "default", "the prefix namespace")
var table = flag.String("table", "test", "the table to write")
var maxExpireSecs = flag.Int("maxExpire", 60, "max expire seconds to be allowed with setex")
var minExpireSecs = flag.Int("minExpire", 10, "min expire seconds to be allowed with setex")
var anyCommand = flag.String("any", "", "run given command string, use __rand__ for rand arg and use __seq__ for sequence arg")
var checkData = flag.Bool("check_data", false, "whether check data after write")
var checkDataSeed = flag.String("check_data_seed", "s1", "seed is used for write seed data to tell different bench tool")
var logDetail = flag.Bool("log_detail", false, "log more")

var wg sync.WaitGroup
var loop int = 0
var latencyDistribute []int64

func doCommand(client *zanredisdb.ZanRedisClient, cmd string, args ...interface{}) error {
	_, err := doCommandWithRsp(client, cmd, args...)
	return err
}

func doCommandWithRsp(client *zanredisdb.ZanRedisClient, cmd string, args ...interface{}) (interface{}, error) {
	v := args[0]
	prefix := *namespace + ":" + *table + ":"
	sharding := ""
	switch vt := v.(type) {
	case string:
		sharding = *table + ":" + vt
		args[0] = prefix + vt
	case []byte:
		sharding = *table + ":" + string(vt)
		args[0] = []byte(prefix + string(vt))
	case int:
		sharding = *table + ":" + strconv.Itoa(vt)
		args[0] = prefix + strconv.Itoa(vt)
	case int64:
		sharding = *table + ":" + strconv.Itoa(int(vt))
		args[0] = prefix + strconv.Itoa(int(vt))
	}
	s := time.Now()
	rsp, err := client.DoRedis(strings.ToUpper(cmd), []byte(sharding), *useLeader, args...)
	if err != nil {
		fmt.Printf("do %s (%v) error %s\n", cmd, args[0], err.Error())
		return rsp, err
	}
	cost := time.Since(s).Nanoseconds()
	index := cost / 1000 / 1000
	if index < 100 {
		index = index / 10
	} else if index < 1000 {
		index = 9 + index/100
	} else if index < 10000 {
		index = 19 + index/1000
	} else {
		index = 29
	}
	if index >= int64(*logSlow) {
		fmt.Printf("do %s (%v) slow %v, %v\n", cmd, args[0], cost, time.Now().String())
	}
	atomic.AddInt64(&latencyDistribute[index], 1)
	return rsp, nil
}

func bench(cmd string, f func(c *zanredisdb.ZanRedisClient) error) {
	wg.Add(*clients)

	t1 := time.Now()
	pdAddr := fmt.Sprintf("%s:%d", *ip, *port)
	currentNumList := make([]int64, *clients)
	latencyDistribute = make([]int64, 32)
	errCnt := int64(0)
	done := int32(0)
	for i := 0; i < *clients; i++ {
		go func(clientIndex int) {
			var err error
			conf := &zanredisdb.Conf{
				DialTimeout:   time.Second * 15,
				ReadTimeout:   0,
				WriteTimeout:  0,
				TendInterval:  10,
				Namespace:     *namespace,
				MaxIdleConn:   10,
				MaxActiveConn: 100,
				DC:            *dc,
			}
			conf.LookupList = append(conf.LookupList, pdAddr)
			c, _ := zanredisdb.NewZanRedisClient(conf)
			c.Start()
			for j := 0; j < loop; j++ {
				err = f(c)
				if err != nil {
					atomic.AddInt64(&errCnt, 1)
				}
				atomic.AddInt64(&currentNumList[clientIndex], 1)
			}
			c.Stop()
			wg.Done()
		}(i)
	}
	go func() {
		lastNum := int64(0)
		lastTime := time.Now()
		printLatency := lastTime
		for atomic.LoadInt32(&done) == 0 {
			time.Sleep(time.Second * 30)
			t2 := time.Now()
			d := t2.Sub(lastTime)
			num := int64(0)
			for i, _ := range currentNumList {
				num += atomic.LoadInt64(&currentNumList[i])
			}
			if num <= lastNum {
				continue
			}
			fmt.Printf("%s: %s %0.3f micros/op, %0.2fop/s, err: %v, num:%v\n",
				cmd,
				d.String(),
				float64(d.Nanoseconds()/1e3)/float64(num-lastNum),
				float64(num-lastNum)/d.Seconds(),
				atomic.LoadInt64(&errCnt),
				num,
			)
			lastNum = num
			lastTime = t2
			if time.Since(printLatency) > time.Minute*30 {
				for i, v := range latencyDistribute {
					if i == 0 {
						fmt.Printf("latency below 100ms:\n")
					} else if i == 10 {
						fmt.Printf("\nlatency between 100ms ~ 999ms:\n")
					} else if i == 20 {
						fmt.Printf("\nlatency above 1s:\n")
					}
					fmt.Printf("%d: %v,", i, v)
				}
				printLatency = t2
			}
		}
	}()

	wg.Wait()
	atomic.StoreInt32(&done, 1)
	t2 := time.Now()
	d := t2.Sub(t1)

	fmt.Printf("%s: %s %0.3f micros/op, %0.2fop/s, err: %v, num:%v\n",
		cmd,
		d.String(),
		float64(d.Nanoseconds()/1e3)/float64(*number),
		float64(*number)/d.Seconds(),
		atomic.LoadInt64(&errCnt),
		*number,
	)
	for i, v := range latencyDistribute {
		if i == 0 {
			fmt.Printf("latency below 100ms\n")
		} else if i == 10 {
			fmt.Printf("latency between 100ms ~ 999ms\n")
		} else if i == 20 {
			fmt.Printf("latency above 1s\n")
		}
		fmt.Printf("latency interval %d: %v\n", i, v)
	}
}

var anyCmdBaseCnt int64 = 0

func benchAnyCommand() {
	anyCmds := strings.Split(*anyCommand, " ")
	fmt.Printf("bench any command: %v\n", anyCmds)
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&anyCmdBaseCnt, 1) % int64(*primaryKeyCnt)
		seqStr := fmt.Sprintf("%010d", int(n))
		rd := int64(rand.Int31()) % int64(*primaryKeyCnt)
		rdStr := fmt.Sprintf("%010d", int(rd))
		var args []interface{}
		for _, cmd := range anyCmds[1:] {
			ncmd := strings.Replace(cmd, "__rand__", rdStr, -1)
			ncmd = strings.Replace(ncmd, "__seq__", seqStr, -1)
			args = append(args, ncmd)
		}
		return doCommand(c, anyCmds[0], args...)
	}
	bench(anyCmds[0], f)
}

func benchIncr() {

}

func benchGeoAdd() {

}
func benchHScan() {

}
func benchZScan() {

}

var kvSetBase int64 = 0
var kvGetBase int64 = 0
var kvIncrBase int64 = 0
var kvDelBase int64 = 0

func benchSet() {
	valueSample := make([]byte, *valueSize)
	for i := 0; i < len(valueSample); i++ {
		valueSample[i] = byte(i % 255)
	}
	f := func(c *zanredisdb.ZanRedisClient) error {
		value := make([]byte, *valueSize)
		copy(value, valueSample)
		n := atomic.AddInt64(&kvSetBase, 1) % int64(*primaryKeyCnt)
		tmp := fmt.Sprintf("%010d", int(n))
		index := 0
		if index < *valueSize {
			copy(value[index:], tmp)
			index += len(tmp)
		}
		tn := time.Now()
		ts := strconv.FormatInt(tn.UnixNano(), 10)
		if index < *valueSize {
			copy(value[index:], ts)
			index += len(ts)
		}
		if *checkData && index < *valueSize {
			seedStr := fmt.Sprintf("%s", *checkDataSeed)
			copy(value[index:], seedStr)
			index += len(seedStr)
		}
		if *logDetail {
			fmt.Printf("set %s to %s\n", tmp, value)
		}
		err := doCommand(c, "SET", tmp, value)
		if err != nil {
			return err
		}
		if *checkData {
			ret, err := redis.Bytes(doCommandWithRsp(c, "GET", tmp))
			if err != nil {
				return err
			}
			// check if this is new written
			tstr := string(ret[len(tmp) : len(tmp)+19])
			nano, err := strconv.ParseInt(tstr, 10, 64)
			if err != nil {
				fmt.Printf("%s time parse error: %v, %s\n", tmp, err.Error(), ret)
				return err
			}
			vt := time.Unix(0, nano)
			if vt.Before(tn) {
				fmt.Printf("%s check data error %s at %s\n", tmp, ret, ts)
				return err
			}
			if !bytes.Equal(ret[:index], value[:index]) {
				fmt.Printf("set %s check data error %s, %s\n", tmp, value[:index], ret[:index])
				return errors.New("check data error")
			}
		}
		return nil
	}
	bench("set", f)
}

func benchSetEx() {
	atomic.StoreInt64(&kvSetBase, 0)

	valueSample := make([]byte, *valueSize)
	for i := 0; i < len(valueSample); i++ {
		valueSample[i] = byte(i % 255)
	}

	f := func(c *zanredisdb.ZanRedisClient) error {
		value := make([]byte, *valueSize)
		copy(value, valueSample)
		n := atomic.AddInt64(&kvSetBase, 1) % int64(*primaryKeyCnt)
		ttl := rand.Int31n(int32(*maxExpireSecs-*minExpireSecs)) + int32(*minExpireSecs)
		tmp := fmt.Sprintf("%010d-%d-%s", int(n), ttl, time.Now().Format(time.RFC3339Nano))
		tn := time.Now()
		ts := strconv.FormatInt(tn.UnixNano(), 10)
		index := 0
		if index < *valueSize {
			copy(value[index:], tmp)
			index += len(tmp)
		}
		if index < *valueSize {
			copy(value[index:], ts)
			index += len(ts)
		}
		if *checkData && index < *valueSize {
			seedStr := fmt.Sprintf("%s", *checkDataSeed)
			copy(value[index:], seedStr)
			index += len(seedStr)
		}
		if *logDetail {
			fmt.Printf("set %s to %s\n", tmp, value)
		}
		err := doCommand(c, "SETEX", tmp, ttl, value)
		if err != nil {
			return err
		}
		if *checkData {
			ret, err := redis.Bytes(doCommandWithRsp(c, "GET", tmp))
			if err != nil {
				fmt.Printf("set %s check data error %s\n", tmp, err.Error())
				return err
			}
			// check if this is new written
			tstr := string(ret[len(tmp) : len(tmp)+19])
			nano, err := strconv.ParseInt(tstr, 10, 64)
			if err != nil {
				fmt.Printf("%s time parse error: %v, %s\n", tmp, err.Error(), ret)
				return err
			}
			vt := time.Unix(0, nano)
			if err != nil {
				fmt.Printf("%s time parse error: %v, %s\n", tmp, err.Error(), ret)
				return err
			}
			if vt.Before(tn) {
				fmt.Printf("%s check data error %s at %s\n", tmp, ret, ts)
				return err
			}
			if !bytes.Equal(ret[:index], value[:index]) {
				fmt.Printf("set %s check data error %s, %s\n", tmp, value[:index], ret[:index])
				return errors.New("check data error")
			}
		}
		return nil
	}

	bench("setex", f)
}

func benchGet() {
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&kvGetBase, 1) % int64(*primaryKeyCnt)
		k := fmt.Sprintf("%010d", int(n))
		ret, err := redis.Bytes(doCommandWithRsp(c, "GET", k))
		if err != nil {
			if err == redis.ErrNil {
				return nil
			}
			return err
		}
		if *checkData && len(ret) > 0 {
			if len(ret) < len(k) || string(ret[:len(k)]) != k {
				fmt.Printf("get %s check data error %s\n", k, ret)
				return errors.New("check data error")
			}
		}
		return nil
	}
	bench("get", f)
}

func benchRandGet() {
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := rand.Int() % *number
		k := fmt.Sprintf("%010d", int(n))
		ret, err := redis.Bytes(doCommandWithRsp(c, "GET", k))
		if err != nil {
			if err == redis.ErrNil {
				return nil
			}
			return err
		}
		if *checkData && len(ret) > 0 {
			if len(ret) < len(k) || string(ret[:len(k)]) != k {
				fmt.Printf("get %s check data error %s\n", k, ret)
				return errors.New("check data error")
			}
		}
		return nil
	}
	bench("randget", f)
}

func benchDel() {
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&kvDelBase, 1) % int64(*primaryKeyCnt)
		k := fmt.Sprintf("%010d", int(n))
		ts := time.Now()
		err := doCommand(c, "DEL", k)
		if err != nil {
			return err
		}
		ret, err := redis.Bytes(doCommandWithRsp(c, "GET", k))
		if err != nil {
			if err == redis.ErrNil {
				return nil
			}
			return err
		}
		if *checkData && len(ret) > 0 {
			if len(ret) < len(k) || string(ret[:len(k)]) != k {
				fmt.Printf("del %s check data error %s\n", k, ret)
				return errors.New("check data error")
			}
			// check if this is new written
			tstr := string(ret[len(k) : len(k)+19])
			nano, err := strconv.ParseInt(tstr, 10, 64)
			if err != nil {
				fmt.Printf("%s time parse error: %v, %s\n", k, err.Error(), ret)
				return err
			}
			vt := time.Unix(0, nano)
			if vt.Before(ts) {
				fmt.Printf("del %s check data error %s at %s\n", k, ret, ts)
				return err
			}
		}
		return nil
	}

	bench("del", f)
}

func benchPFAdd() {
	subKeyCnt := int64(*keyNumber / (*primaryKeyCnt))
	f := func(c *zanredisdb.ZanRedisClient) error {
		rn := rand.Int()
		n := int64(rn % *keyNumber)
		pk := n / subKeyCnt
		tmp := fmt.Sprintf("%010d", int(pk))
		subkey := n - pk*subKeyCnt
		return doCommand(c, "PFADD", tmp, subkey, rn)
	}
	bench("PFADD", f)
}

func benchPFCount() {
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := int64(rand.Int() % *keyNumber)
		pk := n % int64(*primaryKeyCnt)
		tmp := fmt.Sprintf("%010d", int(pk))
		return doCommand(c, "PFCOUNT", tmp)
	}
	bench("PFCOUNT", f)
}

var listPushBase int64
var listRange10Base int64
var listRange50Base int64
var listRange100Base int64
var listPopBase int64

func benchLPushList() {
	benchPushList("lpush")
}
func benchRPushList() {
	benchPushList("rpush")
}
func benchPushList(pushCmd string) {
	valueSample := make([]byte, *valueSize)
	for i := 0; i < len(valueSample); i++ {
		valueSample[i] = byte(i % 255)
	}
	magicIdentify := make([]byte, 9+3+3)
	for i := 0; i < len(magicIdentify); i++ {
		if i < 3 || i > len(magicIdentify)-3 {
			magicIdentify[i] = 0
		} else {
			magicIdentify[i] = byte(i % 3)
		}
	}
	f := func(c *zanredisdb.ZanRedisClient) error {
		value := make([]byte, *valueSize)
		copy(value, valueSample)
		n := atomic.AddInt64(&listPushBase, 1) % int64(*primaryKeyCnt)
		tmp := fmt.Sprintf("%010d", int(n))
		ts := strconv.FormatInt(time.Now().UnixNano(), 10)
		index := 0
		copy(value[index:], magicIdentify)
		index += len(magicIdentify)
		if index < *valueSize {
			copy(value[index:], ts)
			index += len(ts)
		}
		if index < *valueSize {
			copy(value[index:], tmp)
			index += len(tmp)
		}
		if *valueSize > len(magicIdentify) {
			copy(value[len(value)-len(magicIdentify):], magicIdentify)
		}
		return doCommand(c, pushCmd, "mytestlist"+tmp, value)
	}

	bench(pushCmd, f)
}

func benchRangeList10() {
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&listRange10Base, 1) % int64(*primaryKeyCnt)
		tmp := fmt.Sprintf("%010d", int(n))
		return doCommand(c, "LRANGE", "mytestlist"+tmp, 0, 10)
	}

	bench("lrange10", f)
}

func benchRangeList50() {
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&listRange50Base, 1) % int64(*primaryKeyCnt)
		tmp := fmt.Sprintf("%010d", int(n))
		return doCommand(c, "LRANGE", "mytestlist"+tmp, 0, 50)
	}

	bench("lrange50", f)
}

func benchRangeList100() {
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&listRange100Base, 1) % int64(*primaryKeyCnt)
		tmp := fmt.Sprintf("%010d", int(n))
		return doCommand(c, "LRANGE", "mytestlist"+tmp, 0, 100)
	}

	bench("lrange100", f)
}

func benchRPopList() {
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&listPopBase, 1) % int64(*primaryKeyCnt)
		tmp := fmt.Sprintf("%010d", int(n))
		return doCommand(c, "RPOP", "mytestlist"+tmp)
	}

	bench("rpop", f)
}

func benchLPopList() {
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&listPopBase, 1) % int64(*primaryKeyCnt)
		tmp := fmt.Sprintf("%010d", int(n))
		return doCommand(c, "LPOP", "mytestlist"+tmp)
	}

	bench("lpop", f)
}

var hashPKBase int64
var hashSetBase int64
var hashIncrBase int64
var hashGetBase int64
var hashDelBase int64

func benchHset() {
	valueSample := make([]byte, *valueSize)
	for i := 0; i < len(valueSample); i++ {
		valueSample[i] = byte(i % 255)
	}
	atomic.StoreInt64(&hashPKBase, 0)
	subKeyCnt := int64(*keyNumber / (*primaryKeyCnt))
	f := func(c *zanredisdb.ZanRedisClient) error {
		value := make([]byte, *valueSize)
		copy(value, valueSample)

		n := atomic.AddInt64(&hashSetBase, 1) % int64(*keyNumber)
		pk := n / subKeyCnt
		tmp := fmt.Sprintf("%010d", int(pk))
		subkey := n - pk*subKeyCnt
		ts := strconv.FormatInt(time.Now().UnixNano(), 10)

		index := 0
		if index < *valueSize {
			copy(value[index:], tmp)
			index += len(tmp)
		}
		if index < *valueSize {
			subks := fmt.Sprintf("%010d", subkey)
			copy(value[index:], subks)
			index += len(subks)
		}
		if index < *valueSize {
			copy(value[index:], ts)
			index += len(ts)
		}
		if *checkData && index < *valueSize {
			seedStr := fmt.Sprintf("%s", *checkDataSeed)
			copy(value[index:], seedStr)
			index += len(seedStr)
		}
		if *logDetail {
			fmt.Printf("hset %s-%v to %s\n", tmp, subkey, value)
		}
		err := doCommand(c, "HMSET", "myhashkey"+tmp, subkey, value, "intv", subkey, "strv", tmp)
		if err != nil {
			return nil
		}
		if *checkData {
			ret, err := redis.Bytes(doCommandWithRsp(c, "hget", "myhashkey"+tmp, subkey))
			if err != nil {
				return err
			}
			if len(ret) < index || !bytes.Equal(value[:index], ret[:index]) {
				fmt.Printf("hmset %s check data error %s, %s\n", tmp, ret, value)
				return errors.New("hmset check data error")
			}
		}
		return nil
	}

	bench("hmset", f)
}

func benchHGet() {
	atomic.StoreInt64(&hashPKBase, 0)
	subKeyCnt := int64(*keyNumber / (*primaryKeyCnt))
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&hashGetBase, 1) % int64(*keyNumber)
		pk := n / subKeyCnt
		tmp := fmt.Sprintf("%010d", int(pk))
		subkey := n - pk*subKeyCnt
		ret, err := redis.Bytes(doCommandWithRsp(c, "HGET", "myhashkey"+tmp, subkey))
		if err != nil {
			if err == redis.ErrNil {
				return nil
			}
			return err
		}
		if *checkData {
			subs := fmt.Sprintf("%010d", subkey)
			if len(ret) < len(tmp)+len(subs) || string(ret[:len(tmp)+len(subs)]) != tmp+subs {
				fmt.Printf("hmset %s check data error %s\n", tmp, ret)
				return errors.New("hmset check data error")
			}
		}
		return nil
	}

	bench("hget", f)
}

func benchHRandGet() {
	atomic.StoreInt64(&hashPKBase, 0)
	subKeyCnt := int64(*keyNumber / (*primaryKeyCnt))
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := int64(rand.Int() % *keyNumber)
		pk := n / subKeyCnt
		tmp := fmt.Sprintf("%010d", int(pk))
		subkey := n - pk*subKeyCnt
		ret, err := doCommandWithRsp(c, "HGET", "myhashkey"+tmp, subkey)
		if *checkData {
			retBytes, err := redis.Bytes(ret, err)
			if err != nil {
				if err == redis.ErrNil {
					return nil
				}
				return err
			}
			subs := fmt.Sprintf("%010d", subkey)
			if len(retBytes) < len(tmp)+len(subs) || string(retBytes[:len(tmp)+len(subs)]) != tmp+subs {
				fmt.Printf("hmset %s check data error %s\n", tmp, retBytes)
				return errors.New("hmset check data error")
			}
		}
		return err
	}

	bench("hrandget", f)
}

func benchHDel() {
	atomic.StoreInt64(&hashPKBase, 0)
	subKeyCnt := int64(*keyNumber / (*primaryKeyCnt))
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&hashDelBase, 1) % int64(*keyNumber)
		pk := n / subKeyCnt
		tmp := fmt.Sprintf("%010d", int(pk))
		subkey := n - pk*subKeyCnt
		ts := time.Now()
		err := doCommand(c, "HDEL", "myhashkey"+tmp, subkey)
		if err != nil {
			return err
		}
		ret, err := redis.Bytes(doCommandWithRsp(c, "HGET", "myhashkey"+tmp, subkey))
		if err != nil {
			if err == redis.ErrNil {
				return nil
			}
			return err
		}
		if *checkData && len(ret) > 0 {
			subs := fmt.Sprintf("%010d", subkey)
			if len(ret) < len(tmp) || string(ret[:len(tmp)+len(subs)]) != tmp+subs {
				fmt.Printf("del %s check data error %s\n", tmp+subs, ret)
				return errors.New("check data error")
			}
			// check if this is new written
			tstr := string(ret[len(tmp)+len(subs) : len(tmp)+len(subs)+19])
			nano, err := strconv.ParseInt(tstr, 10, 64)
			if err != nil {
				fmt.Printf("%s time parse error: %v, %s\n", tmp, err.Error(), ret)
				return err
			}
			vt := time.Unix(0, nano)
			if vt.Before(ts) {
				fmt.Printf("del %s check data error %s at %s\n", tmp+subs, ret, ts)
				return err
			}
		}
		return nil
	}

	bench("hdel", f)
}

var setPKBase int64
var setAddBase int64
var setDelBase int64

func benchSAdd() {
	atomic.StoreInt64(&setPKBase, 0)
	subKeyCnt := int64(*keyNumber / (*primaryKeyCnt))
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&setAddBase, 1) % int64(*keyNumber)
		pk := n / subKeyCnt
		tmp := fmt.Sprintf("%010d", int(pk))
		subkey := n - pk*subKeyCnt
		return doCommand(c, "SADD", "mysetkey"+tmp, subkey)
	}
	bench("sadd", f)
}

func benchSRem() {
	atomic.StoreInt64(&setPKBase, 0)
	subKeyCnt := int64(*keyNumber / (*primaryKeyCnt))
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&setDelBase, 1) % int64(*keyNumber)
		pk := n / subKeyCnt
		tmp := fmt.Sprintf("%010d", int(pk))
		subkey := n - pk*subKeyCnt
		return doCommand(c, "SREM", "mysetkey"+tmp, subkey)
	}

	bench("srem", f)
}

func benchSPop() {
	atomic.StoreInt64(&setPKBase, 0)
	subKeyCnt := int64(*keyNumber / (*primaryKeyCnt))
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&setDelBase, 1) % int64(*keyNumber)
		pk := n / subKeyCnt
		tmp := fmt.Sprintf("%010d", int(pk))
		return doCommand(c, "SPOP", "mysetkey"+tmp)
	}

	bench("spop", f)
}

func benchSClear() {
	atomic.StoreInt64(&setPKBase, 0)
	subKeyCnt := int64(*keyNumber / (*primaryKeyCnt))
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&setDelBase, 1) % int64(*keyNumber)
		pk := n / subKeyCnt
		tmp := fmt.Sprintf("%010d", int(pk))
		return doCommand(c, "SCLEAR", "mysetkey"+tmp)
	}

	bench("sclear", f)
}

func benchSIsMember() {
	atomic.StoreInt64(&setPKBase, 0)
	subKeyCnt := int64(*keyNumber / (*primaryKeyCnt))
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := int64(rand.Int() % *keyNumber)
		pk := n / subKeyCnt
		tmp := fmt.Sprintf("%010d", int(pk))
		subkey := n - pk*subKeyCnt
		return doCommand(c, "SISMEMBER", "mysetkey"+tmp, subkey)
	}

	bench("sismember", f)
}

func benchSMembers() {
	atomic.StoreInt64(&setPKBase, 0)
	subKeyCnt := int64(*keyNumber / (*primaryKeyCnt))
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := int64(rand.Int() % *keyNumber)
		pk := n / subKeyCnt
		tmp := fmt.Sprintf("%010d", int(pk))
		return doCommand(c, "SMEMBERS", "mysetkey"+tmp)
	}

	bench("smembers", f)
}

var zsetPKBase int64
var zsetAddBase int64
var zsetDelBase int64

func benchZAdd() {
	atomic.StoreInt64(&zsetPKBase, 0)
	subKeyCnt := int64(*keyNumber / (*primaryKeyCnt))
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&zsetAddBase, 1) % int64(*keyNumber)
		pk := n / subKeyCnt
		tmp := fmt.Sprintf("%010d", int(pk))
		subkey := n - pk*subKeyCnt
		member := strconv.Itoa(int(subkey))
		member += tmp
		return doCommand(c, "ZADD", "myzsetkey"+tmp, subkey, member)
	}

	bench("zadd", f)
}

func benchZRem() {
	atomic.StoreInt64(&zsetPKBase, 0)
	subKeyCnt := int64(*keyNumber / (*primaryKeyCnt))
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&zsetDelBase, 1) % int64(*keyNumber)
		pk := n / subKeyCnt
		tmp := fmt.Sprintf("%010d", int(pk))
		subkey := n - pk*subKeyCnt
		member := strconv.Itoa(int(subkey))
		member += tmp
		return doCommand(c, "ZREM", "myzsetkey"+tmp, member)
	}

	bench("zrem", f)
}

func benchZRemRangeByScore() {
	atomic.StoreInt64(&zsetPKBase, 0)
	subKeyCnt := int64(*keyNumber / (*primaryKeyCnt))
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&zsetPKBase, 1) % int64(*keyNumber)
		pk := n / subKeyCnt
		tmp := fmt.Sprintf("%010d", int(pk))
		return doCommand(c, "ZREMRANGEBYSCORE", "myzsetkey"+tmp, 0, rand.Int()%int(subKeyCnt))
	}

	bench("zremrangebyscore", f)
}

func benchZRangeByScore() {
	atomic.StoreInt64(&zsetPKBase, 0)
	subKeyCnt := int64(*keyNumber / (*primaryKeyCnt))
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&zsetPKBase, 1) % int64(*keyNumber)
		pk := n / subKeyCnt
		tmp := fmt.Sprintf("%010d", int(pk))
		return doCommand(c, "ZRANGEBYSCORE", "myzsetkey"+tmp, 0, rand.Int()%int(subKeyCnt), "limit", rand.Int()%100, 100)
	}

	bench("zrangebyscore", f)
}

func benchZRangeByRank() {
	atomic.StoreInt64(&zsetPKBase, 0)
	subKeyCnt := int64(*keyNumber / (*primaryKeyCnt))
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&zsetPKBase, 1) % int64(*keyNumber)
		pk := n / subKeyCnt
		tmp := fmt.Sprintf("%010d", int(pk))
		return doCommand(c, "ZRANGE", "myzsetkey"+tmp, 0, rand.Int()%100)
	}

	bench("zrange", f)
}

func benchZRevRangeByScore() {
	atomic.StoreInt64(&zsetPKBase, 0)
	subKeyCnt := int64(*keyNumber / (*primaryKeyCnt))
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&zsetPKBase, 1) % int64(*keyNumber)
		pk := n / subKeyCnt
		tmp := fmt.Sprintf("%010d", int(pk))
		return doCommand(c, "ZREVRANGEBYSCORE", "myzsetkey"+tmp, 0, rand.Int()%int(subKeyCnt), "limit", rand.Int()%100, 100)
	}

	bench("zrevrangebyscore", f)
}

func benchZRevRangeByRank() {
	atomic.StoreInt64(&zsetPKBase, 0)
	subKeyCnt := int64(*keyNumber / (*primaryKeyCnt))
	f := func(c *zanredisdb.ZanRedisClient) error {
		n := atomic.AddInt64(&zsetPKBase, 1) % int64(*keyNumber)
		pk := n / subKeyCnt
		tmp := fmt.Sprintf("%010d", int(pk))
		return doCommand(c, "ZREVRANGE", "myzsetkey"+tmp, 0, rand.Int()%100)
	}

	bench("zrevrange", f)
}

func runOrCheck(f func(), wg *sync.WaitGroup) {
	if *checkData {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f()
		}()
	} else {
		f()
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	if *number <= 0 {
		panic("invalid number")
		return
	}

	if *clients <= 0 || *number < *clients {
		panic("invalid client number")
		return
	}

	loop = *number / *clients

	if *round <= 0 {
		*round = 1
	}

	zanredisdb.SetLogger(int32(*logLevel), zanredisdb.NewSimpleLogger())
	ts := strings.Split(*tests, ",")
	for i := 0; i < *round; i++ {
		var wg sync.WaitGroup
		if *anyCommand != "" {
			runOrCheck(benchAnyCommand, &wg)
		}
		for _, s := range ts {
			switch strings.ToLower(s) {
			case "set":
				runOrCheck(benchSet, &wg)
			case "setex":
				runOrCheck(benchSetEx, &wg)
			case "pfadd":
				runOrCheck(benchPFAdd, &wg)
			case "pfcount":
				runOrCheck(benchPFCount, &wg)
			case "get":
				runOrCheck(benchGet, &wg)
			case "randget":
				runOrCheck(benchRandGet, &wg)
			case "del":
				runOrCheck(benchDel, &wg)
			case "incr":
				runOrCheck(benchIncr, &wg)
			case "lpush":
				runOrCheck(benchLPushList, &wg)
			case "rpush":
				runOrCheck(benchRPushList, &wg)
			case "lrange":
				benchRangeList10()
				benchRangeList50()
				benchRangeList100()
			case "lpop":
				benchLPopList()
			case "rpop":
				benchRPopList()
			case "hset":
				runOrCheck(benchHset, &wg)
			case "hget":
				runOrCheck(benchHGet, &wg)
			case "randhget":
				runOrCheck(benchHRandGet, &wg)
			case "hdel":
				runOrCheck(benchHDel, &wg)
			case "sadd":
				benchSAdd()
			case "srem":
				benchSRem()
			case "spop":
				benchSPop()
			case "sismember":
				benchSIsMember()
			case "smembers":
				benchSMembers()
			case "sclear":
				benchSClear()
			case "zadd":
				benchZAdd()
			case "zrange":
				benchZRangeByRank()
				benchZRangeByScore()
			case "zrevrange":
				//rev is too slow in leveldb, rocksdb or other
				//maybe disable for huge data benchmark
				benchZRevRangeByRank()
				benchZRevRangeByScore()
			case "zrem":
				benchZRem()
			case "zremrangebyscore":
				benchZRemRangeByScore()
			case "geoadd":
				benchGeoAdd()
			case "geodist":
			case "georadius":
			case "georadiusbymember":
			case "hscan":
				benchHScan()
			case "zscan":
				benchZScan()
			}
		}
		wg.Wait()
		println("")
	}
}

func init() {
	latencyDistribute = make([]int64, 32)
}
