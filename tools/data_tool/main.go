package main

import (
	"flag"
	"fmt"
	"log"
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
var checkMode = flag.String("mode", "", "supported check-list/fix-list/dump-keys/import-noexist/import-bigger/checkrem-zset")
var dataType = flag.String("data-type", "kv", "data type support kv/hash/list/zset/set")
var namespace = flag.String("namespace", "default", "the prefix namespace")
var table = flag.String("table", "test", "the table to write")
var sleep = flag.Duration("sleep", time.Microsecond, "how much to sleep every 100 keys during scan")
var maxNum = flag.Int64("max-check", 100000, "max number of keys to check")

var startScore = flag.Int64("start-score", 1531892320000, "start date for scan")
var stopScore = flag.Int64("stop-score", 1532392320000, "stop date for scan")

var concurrency = flag.Int("concurrency", 1, "concurrency for task")

var destIP = flag.String("dest_ip", "", "dest proxy ip")
var destPort = flag.Int("dest_port", 3803, "dest proxy port")
var destNamespace = flag.String("dest_namespace", "", "the prefix namespace")
var destTable = flag.String("dest_table", "", "the table to write")

func doCommand(client *zanredisdb.ZanRedisClient, cmd string, args ...interface{}) (interface{}, error) {
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
	rsp, err := client.DoRedis(strings.ToUpper(cmd), []byte(sharding), true, args...)
	if err != nil {
		log.Printf("do %s (%v) error %s\n", cmd, args[0], err.Error())
		return rsp, err
	}
	return rsp, nil
}

func checkList(tryFix bool, c *zanredisdb.ZanRedisClient) {
	stopC := make(chan struct{})
	defer close(stopC)
	ch := c.AdvScanChannel("list", *table, stopC)
	cnt := int64(0)
	wrongKeys := int64(0)
	defer func() {
		log.Printf("list total checked %v,  mimatch %v", cnt, wrongKeys)
	}()
	log.Printf("begin checking")
	for k := range ch {
		cnt++
		if cnt > *maxNum {
			break
		}
		if cnt%100 == 0 {
			fmt.Print(".")
			if *sleep > 0 {
				time.Sleep(*sleep)
			}
		}
		if cnt%1000 == 0 {
			fmt.Printf("%d(%d)", cnt, wrongKeys)
		}
		rsp, err := doCommand(c, "llen", k)
		listLen, err := redis.Int64(rsp, err)
		if err != nil {
			log.Printf("list %v llen return invalid: %v", string(k), err)
			continue
		}
		if listLen > 1000 {
			log.Printf("list %v llen too much, just range small: %v", string(k), listLen)
			listLen = 1000
		}
		rsp, err = doCommand(c, "lrange", k, 0, listLen)
		ay, err := redis.MultiBulk(rsp, err)
		if err != nil {
			log.Printf("list %v range return invalid: %v", string(k), err)
			continue
		}
		if int64(len(ay)) != listLen {
			wrongKeys++
			if tryFix {
				_, err = doCommand(c, "lfixkey", k)
				if err != nil {
					log.Printf("list %v fix return error: %v", string(k), err)
				}
			} else {
				log.Printf("list %v llen %v not matching the lrange %v", string(k), listLen, len(ay))
			}
		}
	}
}

func dumpKeys(c *zanredisdb.ZanRedisClient) {
	stopC := make(chan struct{})
	defer close(stopC)
	if *dataType != "kv" && *dataType != "hash" && *dataType != "list" && *dataType != "set" && *dataType != "zset" {
		log.Printf("data type not supported %v\n", *dataType)
		return
	}
	ch := c.AdvScanChannel(*dataType, *table, stopC)
	cnt := int64(0)
	defer func() {
		log.Printf("total scanned %v", cnt)
	}()
	log.Printf("begin checking")
	for k := range ch {
		cnt++
		if cnt > *maxNum {
			break
		}
		if cnt%100 == 0 {
			if *sleep > 0 {
				time.Sleep(*sleep)
			}
		}
		log.Printf("%s\n", string(k))
	}
}

func importNoexist(c *zanredisdb.ZanRedisClient) {
	stopC := make(chan struct{})
	defer close(stopC)
	if *dataType != "kv" {
		log.Printf("data type not supported %v\n", *dataType)
		return
	}

	writeNs := *destNamespace
	writeTable := *destTable
	if writeNs == "" {
		writeNs = *namespace
	}
	if writeTable == "" {
		writeTable = *table
	}

	ch := c.KVScanChannel(*table, stopC)
	cnt := int64(0)
	success := int64(0)
	defer func() {
		log.Printf("total scanned %v, success: %v\n", cnt, success)
	}()
	log.Printf("begin import %v\n", *table)
	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				log.Printf("one scanned done\n")
			}()
			proxyAddr := fmt.Sprintf("%s:%d", *destIP, *destPort)
			destClient, err := redis.Dial("tcp", proxyAddr)
			if err != nil {
				log.Printf("failed init dest proxy: %v, %v", proxyAddr, err.Error())
				return
			}
			defer destClient.Close()
			for k := range ch {
				atomic.AddInt64(&cnt, 1)
				if *maxNum > 0 && atomic.LoadInt64(&cnt) > *maxNum {
					break
				}
				fk := fmt.Sprintf("%s:%s:%s", writeNs, writeTable, string(k))
				rsp, err := redis.Int(destClient.Do("exists", fk))
				if err != nil {
					log.Printf("error exists %v, %v, err: %v\n", string(k), string(fk), err.Error())
					continue
				}
				if rsp == 1 {
					continue
				}
				v, err := c.KVGet(*table, k)
				if err != nil {
					continue
				}
				rsp, err = redis.Int(destClient.Do("setnx", fk, v))
				if rsp == 1 {
					atomic.AddInt64(&success, 1)
					log.Printf("scanned %v, %d success setnx src:%v(dest:%v), value: %v\n",
						atomic.LoadInt64(&cnt), atomic.LoadInt64(&success), string(k), string(fk), string(v))
				} else if err != nil {
					log.Printf("error setnx %v, %v, : %v\n", string(k), string(fk), err.Error())
				}
			}
		}()
	}
	wg.Wait()
}

func importBigger(c *zanredisdb.ZanRedisClient) {
	stopC := make(chan struct{})
	defer close(stopC)
	if *dataType != "kv" {
		log.Printf("data type not supported %v\n", *dataType)
		return
	}

	if *destIP == "" {
		log.Printf("dest ip should be set\n")
		return
	}

	writeNs := *destNamespace
	writeTable := *destTable
	if writeNs == "" {
		writeNs = *namespace
	}
	if writeTable == "" {
		writeTable = *table
	}

	ch := c.KVScanChannel(*table, stopC)
	cnt := int64(0)
	success := int64(0)
	defer func() {
		log.Printf("total scanned %v, success: %v\n", cnt, success)
	}()
	log.Printf("begin import %v\n", *table)
	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				log.Printf("one scanned done\n")
			}()
			proxyAddr := fmt.Sprintf("%s:%d", *destIP, *destPort)
			destClient, err := redis.Dial("tcp", proxyAddr)
			if err != nil {
				log.Printf("failed init dest proxy: %v, %v", proxyAddr, err.Error())
				return
			}
			defer destClient.Close()
			for k := range ch {
				atomic.AddInt64(&cnt, 1)
				if *maxNum > 0 && atomic.LoadInt64(&cnt) > *maxNum {
					break
				}
				v, err := redis.Int(c.KVGet(*table, k))
				if err != nil {
					continue
				}
				fk := fmt.Sprintf("%s:%s:%s", writeNs, writeTable, string(k))
				rsp, err := redis.Int(destClient.Do("get", fk))
				if err != nil {
					if err == redis.ErrNil {
						rsp, _ := redis.Int(destClient.Do("setnx", fk, v))
						if rsp == 1 {
							atomic.AddInt64(&success, 1)
							log.Printf("scanned %v, setnx src:%v(dest:%v) to value: %v\n",
								atomic.LoadInt64(&cnt), string(k), string(fk), v)
						}

					}
					continue
				}
				if rsp >= v {
					continue
				}
				diff := v - rsp

				atomic.AddInt64(&success, 1)
				rsp2, _ := redis.Int(destClient.Do("incrby", fk, diff))
				log.Printf("scanned %v, %d found bigger src:%v(dest:%v), value: %v, dest: %v, fixed to: %v\n",
					atomic.LoadInt64(&cnt), atomic.LoadInt64(&success), string(k), string(fk), v, rsp, rsp2)
			}
		}()
	}
	wg.Wait()
}

func checkRemZset(c *zanredisdb.ZanRedisClient) {
	stopC := make(chan struct{})
	defer close(stopC)
	if *dataType != "kv" {
		log.Printf("data type not supported %v\n", *dataType)
		return
	}

	if *destIP == "" {
		log.Printf("dest ip should be set\n")
		return
	}

	writeNs := *destNamespace
	writeTable := *destTable
	if writeNs == "" {
		writeNs = *namespace
	}
	if writeTable == "" {
		writeTable = *table
	}

	ch := c.AdvScanChannel("zset", *table, stopC)
	cnt := int64(0)
	success := int64(0)
	defer func() {
		log.Printf("total scanned %v, success: %v\n", cnt, success)
	}()
	log.Printf("begin check %v\n", *table)
	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				log.Printf("one scanned done\n")
			}()
			proxyAddr := fmt.Sprintf("%s:%d", *destIP, *destPort)
			destClient, err := redis.Dial("tcp", proxyAddr)
			if err != nil {
				log.Printf("failed init dest proxy: %v, %v", proxyAddr, err.Error())
				return
			}
			defer destClient.Close()
			for k := range ch {
				atomic.AddInt64(&cnt, 1)
				if *maxNum > 0 && atomic.LoadInt64(&cnt) > *maxNum {
					break
				}

				fk := fmt.Sprintf("%s:%s:%s", writeNs, writeTable, string(k))
				rsp, err := redis.Values(destClient.Do("ZRANGEBYSCORE", fk, *startScore, *stopScore))
				if err != nil {
					if err != redis.ErrNil {
						log.Printf("zrange %v error: %v", string(fk), err.Error())
					}
					continue
				}
				if len(rsp) == 0 {
					continue
				}
				members := make(map[string]bool)
				for i := 0; i < len(rsp); i++ {
					v, ok := rsp[i].([]byte)
					if ok {
						members[string(v)] = true
					}
				}
				num := len(members)

				pk := zanredisdb.NewPKey(*namespace, *table, k)
				vs, err := redis.Values(c.DoRedis("ZRANGEBYSCORE", pk.ShardingKey(), true, pk.RawKey, *startScore, *stopScore))
				if err != nil {
					log.Printf("zrangebyscore %v failed: %v", string(k), err.Error())
					if err == redis.ErrNil {
					} else {
						continue
					}
				}
				invalid := false
				for _, m := range vs {
					v, ok := m.([]byte)
					if ok {
						delete(members, string(v))
					} else {
						log.Printf("src zrangebyscore %v member invalid: %v", string(k), m)
						invalid = true
					}
				}
				if len(members) == 0 || invalid {
					continue
				}
				atomic.AddInt64(&success, 1)
				if len(members) == num {
					// all should be removed
					removed := 0
					if num > 5 {
						removed, err = redis.Int(destClient.Do("ZREMRANGEBYSCORE", fk, *startScore, *stopScore))
					}
					if err != nil {
						log.Printf("remove zset %v error: %v", string(fk), err.Error())
					}
					log.Printf("scanned %v, %d removed zset member src:%v(dest:%v), members: %v removed: %v\n",
						atomic.LoadInt64(&cnt), atomic.LoadInt64(&success), string(k), string(fk), members, removed)
					continue
				}
				diff := num - len(members)
				rsp = rsp[:0]
				rsp = append(rsp, fk)
				for m, _ := range members {
					rsp = append(rsp, m)
				}
				removed := 0
				if len(members) > 5 {
					removed, _ = redis.Int(destClient.Do("zrem", rsp...))
				}
				log.Printf("scanned %v, %d should remove zset member src:%v(dest:%v), members: %v, removed: %v, diff:%v\n",
					atomic.LoadInt64(&cnt), atomic.LoadInt64(&success), string(k), string(fk), rsp, removed, diff)
			}
		}()
	}
	wg.Wait()
}

func main() {
	flag.Parse()
	zanredisdb.SetLogger(1, zanredisdb.NewSimpleLogger())
	checkModeList := strings.Split(*checkMode, ",")

	conf := &zanredisdb.Conf{
		DialTimeout:  time.Second * 15,
		ReadTimeout:  0,
		WriteTimeout: 0,
		TendInterval: 10,
		Namespace:    *namespace,
	}
	pdAddr := fmt.Sprintf("%s:%d", *ip, *port)
	conf.LookupList = append(conf.LookupList, pdAddr)
	c, err := zanredisdb.NewZanRedisClient(conf)
	if err != nil {
		panic(err)
	}
	c.Start()
	defer c.Stop()
	for _, mode := range checkModeList {
		switch strings.ToLower(mode) {
		case "check-list":
			checkList(false, c)
		case "fix-list":
			checkList(true, c)
		case "dump-keys":
			dumpKeys(c)
		case "import-noexist":
			importNoexist(c)
		case "import-bigger":
			importBigger(c)
		case "checkrem-zset":
			checkRemZset(c)
		default:
			log.Printf("unknown check mode: %v", mode)
		}
	}
}
