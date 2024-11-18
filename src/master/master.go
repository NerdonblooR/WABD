package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
	"strings"
	"masterproto"
)

var portnum *int = flag.Int("port", 7087, "Port # to listen on. Defaults to 7087")
var numNodes *int = flag.Int("N", 3, "Number of replicas. Defaults to 3.")
var static  *bool = flag.Bool("static", false, "use hard-coded replica list")
var nodes *string = flag.String("nl", "", "provided nodeLists")



type Master struct {
	N        int
	nodeList  []string
	addrList  []string
	portList  []int
	lock     *sync.Mutex
	nodes    []*rpc.Client
	leader   []bool
	alive    []bool
	repNum   int
	totalNum int
	totalLat int64
}

func main() {
	flag.Parse()

	log.Printf("Master starting on port %d\n", *portnum)
	log.Printf("...waiting for %d replicas\n", *numNodes)

	master := &Master{*numNodes,
		make([]string, 0, *numNodes),
		make([]string, 0, *numNodes),
		make([]int, 0, *numNodes),
		new(sync.Mutex),
		make([]*rpc.Client, *numNodes),
		make([]bool, *numNodes),
		make([]bool, *numNodes),
		0,
		0,
		int64(0)}

	rpc.Register(master)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum))
	if err != nil {
		log.Fatal("Master listen error:", err)
	}

	go master.run()

	http.Serve(l, nil)
}

func (master *Master) run() {
	for true {
		master.lock.Lock()
		if len(master.nodeList) == master.N {
			master.lock.Unlock()
			break
		}
		master.lock.Unlock()
		time.Sleep(100000000)
	}
	time.Sleep(5 * time.Second)

	// connect to servers
	for i := 0; i < master.N; i++ {
		var err error
		addr := fmt.Sprintf("%s:%d", master.addrList[i], master.portList[i]+1000)
		master.nodes[i], err = rpc.DialHTTP("tcp", addr)
		if err != nil {
			log.Fatalf("Error connecting to replica %d\n", i)
		}
		master.leader[i] = false
	}
	master.leader[0] = true
}

func (master *Master) Register(args *masterproto.RegisterArgs, reply *masterproto.RegisterReply) error {
	master.lock.Lock()
	defer master.lock.Unlock()


	nlen := len(master.nodeList)
	index := nlen
	addrPort := fmt.Sprintf("%s:%d", args.Addr, args.Port)

	log.Printf("%s connected", addrPort)

	for i, ap := range master.nodeList {
		if addrPort == ap {
			index = i
			break
		}
	}

	if index == nlen {
		master.nodeList = master.nodeList[0 : nlen+1]
		master.nodeList[nlen] = addrPort
		master.addrList = master.addrList[0 : nlen+1]
		master.addrList[nlen] = args.Addr
		master.portList = master.portList[0 : nlen+1]
		master.portList[nlen] = args.Port
		nlen++
	}

	if nlen == master.N {//n replicas has started
		reply.Ready = true
		reply.ReplicaId = index
		reply.NodeList = master.nodeList
		log.Printf("master reply nodes: %v", master.nodeList)
		if *static{
			nodes := strings.Split(*nodes, ",")
			log.Printf("given nodes: %v", nodes)
			for i, ap := range nodes {
				if addrPort == ap {
					index = i
					break
				}
			}
			reply.ReplicaId = index
			reply.NodeList = nodes
		}
	} else {
		reply.Ready = false
	}

	return nil
}

func (master *Master) GetReplicaList(args *masterproto.GetReplicaListArgs, reply *masterproto.GetReplicaListReply) error {
	master.lock.Lock()
	defer master.lock.Unlock()
	if len(master.nodeList) == master.N {
		if *static{
			reply.ReplicaList = strings.Split(*nodes, ",")
		}else{
			reply.ReplicaList = master.nodeList
		}

		reply.Ready = true
	} else {
		reply.Ready = false
	}

	return nil
}


func (master *Master) ReportResult(args *masterproto.ReportResultArgs, reply *masterproto.ReportResultReply) error {
	master.lock.Lock()
	defer master.lock.Unlock()
	master.repNum++
	master.totalNum += args.TotalBtachNum
	master.totalLat += args.TotalLatency


	log.Printf("Total batch number of replica %d:  %d\n", args.ReplicaId, args.TotalBtachNum)
	log.Printf("Total latency of replica %d:  %d\n", args.ReplicaId, args.TotalLatency)
	reply.Ready = true

	if master.repNum == master.N{
		log.Printf("All_T,%d\n", master.totalNum)
		log.Printf("All_L,%d\n", master.totalLat)
		log.Printf("All_AVGL,%d\n", master.totalLat/int64(master.totalNum))
		master.repNum = 0
		master.totalNum = 0
		master.totalLat = 0
	}
	return nil
}