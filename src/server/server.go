package main

import (
	"abd"
	"config"
	"dlog"
	"epaxos"
	"flag"
	"fmt"
	"gossiper"
	"kv"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"wabd"
)

var id = flag.Int("id", 1, "ID represented by an integer.")
var algo = flag.String("algo", "abd", "algorithm")

func init() {
	flag.Parse()
	dlog.Setup(*id)
	config.Load()
}

func main() {
	if config.GetConfig().DoProfile {
		f, err := os.Create("cpuprofile.out")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)

		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt)
		go catchKill(interrupt)
	}
	shardnum := config.GetConfig().ShardNum
	workerNum := config.GetConfig().WorkerNum
	doMonitor := config.GetConfig().DoMonitor
	abdType := config.GetConfig().AbdType // 0:hybrid, 1:cabd, 2:pure watermark
	log.Printf("Server %d starting...\n", *id)
	if *algo == "abd" {
		log.Printf("ADB type %d...\n", abdType)
		server := kv.NewKVServer(*id, shardnum, doMonitor)
		if abdType != config.CLASSIC {
			g := gossiper.NewWatermarkManager(*id, server)
			for i := 0; i < workerNum; i++ {
				txnHandler := wabd.NewTxnManager(i, server, g)
				go txnHandler.Run()

			}
		} else {
			for i := 0; i < workerNum; i++ {
				txnHandler := abd.NewTxnManager(i, server)
				go txnHandler.Run()
			}
		}
		server.Run()
	} else {
		log.Println("Starting Egalitarian Paxos replica...")
		durable := false
		doReply := true
		doExec := true
		batchSize := config.GetConfig().BatchSize
		r := epaxos.NewReplica(*id, false, doExec, doReply, false, durable, batchSize)
		r.Run()
	}
}

func catchKill(interrupt chan os.Signal) {
	<-interrupt
	if config.GetConfig().DoProfile {
		pprof.StopCPUProfile()
	}
	fmt.Println("Caught signal")
	os.Exit(0)
}
