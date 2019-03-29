package main

import (
	"bytes"
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
var mode = flag.String("mode", "", "supported dump-keys/import-data/compare-data")
var dataType = flag.String("data-type", "kv", "data type support kv/hash/list/zset/set")
var namespace = flag.String("namespace", "default", "the prefix namespace")
var table = flag.String("table", "test", "the table to write")
var sleep = flag.Duration("sleep", time.Microsecond, "how much to sleep every 100 keys during scan")
var maxNum = flag.Int64("max-num", 100000, "max number of keys to export")
var logDetail = flag.Bool("logdetail", true, "log keys while migrate")
var ignoreExist = flag.Bool("ignore_exist", true, "ignore exist keys in dest while migrate")
var ttl = flag.Int("ttl", 0, "the default ttl for dest")

var concurrency = flag.Int("concurrency", 1, "concurrency for task")

var destIP = flag.String("dest_ip", "", "dest proxy ip")
var destPort = flag.Int("dest_port", 3803, "dest proxy port")
var destNamespace = flag.String("dest_namespace", "", "the prefix namespace")
var destTable = flag.String("dest_table", "", "the table to write")

type Handler func(c *zanredisdb.ZanRedisClient, destClient redis.Conn, srck []byte, destK string) (bool, error)

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
		log.Printf("total scanned %v\n", cnt)
	}()
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

func getAndWriteWithNxEx(c *zanredisdb.ZanRedisClient, destClient redis.Conn,
	srck []byte, fk string, ttl int, nx bool) (bool, error) {
	v, err := c.KVGet(*table, srck)
	if err != nil {
		log.Printf("error while get from source %v, err: %v\n", string(srck), err.Error())
		return false, err
	}
	if v == nil {
		return false, nil
	}
	if ttl <= 0 {
		if !nx {
			rsp, err := redis.String(destClient.Do("set", fk, v))
			return rsp == "OK", err
		} else {
			rsp, err := redis.Int(destClient.Do("setnx", fk, v))
			return rsp == 1, err
		}
	} else {
		if !nx {
			rsp, err := redis.String(destClient.Do("setex", fk, ttl, v))
			return rsp == "OK", err
		} else {
			rsp, err := redis.String(destClient.Do("set", fk, v, "EX", ttl, "NX"))
			return rsp == "OK", err
		}
	}
}

func importHandler(c *zanredisdb.ZanRedisClient, destClient redis.Conn, k []byte, fk string) (bool, error) {
	if !*ignoreExist {
		ok, err := getAndWriteWithNxEx(c, destClient, k, fk, *ttl, false)
		if err != nil {
			log.Printf("error setnx %v, %v, : %v\n", string(k), string(fk), err.Error())
		}
		return ok, err
	} else {
		rsp, err := redis.Int(destClient.Do("exists", fk))
		if err != nil {
			log.Printf("error exists %v, %v, err: %v\n", string(k), string(fk), err.Error())
			return false, err
		}
		if rsp == 1 {
			return false, nil
		}
		ok, err := getAndWriteWithNxEx(c, destClient, k, fk, *ttl, true)
		if err != nil {
			log.Printf("error setnx %v, %v, : %v\n", string(k), string(fk), err.Error())
		}
		return ok, err
	}
}

func importData(c *zanredisdb.ZanRedisClient) {
	scanDataAndProcess(c, importHandler)
}

func compareHandler(c *zanredisdb.ZanRedisClient, destClient redis.Conn, srck []byte, destK string) (bool, error) {
	srcv, err := c.KVGet(*table, srck)
	if err != nil {
		log.Printf("error while get from source %v, err: %v\n", string(srck), err.Error())
		return false, err
	}
	rsp, err := redis.Bytes(destClient.Do("get", destK))
	if err != nil {
		log.Printf("error get dest %v, %v, err: %v\n", string(srck), string(destK), err.Error())
		return false, err
	}
	if bytes.Equal(srcv, rsp) {
		return true, nil
	} else {
		log.Printf("key mismatch :%v(dest:%v), value: %v, %v\n",
			string(srck), string(destK),
			string(srcv), string(rsp),
		)
		return false, nil
	}
}

func compareData(c *zanredisdb.ZanRedisClient) {
	scanDataAndProcess(c, compareHandler)
}

func scanDataAndProcess(c *zanredisdb.ZanRedisClient, processFunc Handler) {
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
		log.Printf("total scanned %v, success: %v\n", atomic.LoadInt64(&cnt),
			atomic.LoadInt64(&success))
	}()
	log.Printf("begin compare %v\n", *table)
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
				ok, err := processFunc(c, destClient, k, fk)
				if ok {
					atomic.AddInt64(&success, 1)
					if *logDetail {
						log.Printf("scanned %v, %d success key src:%v(dest:%v)\n",
							atomic.LoadInt64(&cnt), atomic.LoadInt64(&success), string(k), string(fk))
					}
				} else if err != nil {
					log.Printf("scanned %v, %d success, key :%v(dest:%v), err: %v\n",
						atomic.LoadInt64(&cnt), atomic.LoadInt64(&success), string(k), string(fk),
						err.Error(),
					)
				} else if *logDetail {
					log.Printf("scanned %v, %d success\n",
						atomic.LoadInt64(&cnt), atomic.LoadInt64(&success))
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
				_, err = redis.Int(destClient.Do("get", fk))
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
			}
		}()
	}
	wg.Wait()
}

func main() {
	flag.Parse()
	zanredisdb.SetLogger(1, zanredisdb.NewSimpleLogger())
	modeList := strings.Split(*mode, ",")

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
	for _, mode := range modeList {
		switch strings.ToLower(mode) {
		case "dump-keys":
			dumpKeys(c)
		case "import-data":
			importData(c)
		case "compare-data":
			compareData(c)
		default:
			log.Printf("unknown mode: %v", mode)
		}
	}
}
