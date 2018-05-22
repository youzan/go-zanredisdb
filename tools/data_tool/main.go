package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/youzan/go-zanredisdb"
	"github.com/absolute8511/redigo/redis"
)

var ip = flag.String("ip", "127.0.0.1", "pd server ip")
var port = flag.Int("port", 18001, "pd server port")
var checkMode = flag.String("mode", "", "supported check-list/fix-list/dump-keys")
var dataType = flag.String("data-type", "kv", "data type support kv/hash/list/zset/set")
var namespace = flag.String("namespace", "default", "the prefix namespace")
var table = flag.String("table", "test", "the table to write")
var sleep = flag.Duration("sleep", time.Microsecond, "how much to sleep every 100 keys during scan")
var maxNum = flag.Int64("max-check", 100000, "max number of keys to check")

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
	c := zanredisdb.NewZanRedisClient(conf)
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
		default:
			log.Printf("unknown check mode: %v", mode)
		}
	}
}
