package kv

import (
	"bufio"
	"config"
	"genericserver"
	"math/rand"
	"monitor"
	"mvdb"
	"time"
)

//A generic KV server
type TxnHandler interface {
	Run()
}

//A generic KV server
type KVServer struct {
	*genericserver.Server
	DB            *mvdb.DB
	ClientWriters []*bufio.Writer
	Monitor       *monitor.Monitor
	Offset        int64 //simulate loosely synchronized clock
}

func NewKVServer(id int, shardNum int, doMonitor bool) *KVServer {
	rand.Seed(time.Now().UnixNano())
	offsetBound := config.GetConfig().Offset
	server := &KVServer{
		genericserver.NewServer(id, doMonitor),
		nil,
		make([]*bufio.Writer, 0),
		monitor.NewMonitor(doMonitor),
		int64(rand.Intn(2*offsetBound+1) - offsetBound),
	}
	server.DB = mvdb.NewDB(shardNum, server.Monitor)
	return server
}
