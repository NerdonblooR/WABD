package main

import (
	"bufio"
	"bytes"
	"config"
	"dlog"
	"encoding/gob"
	"flag"
	"fmt"
	"genericsmrproto"
	"kvproto"
	"log"
	"math/rand"
	"net"
	"os"
	"sort"
	"state"
	"sync"
	"time"
	"util"
)

var serverId = flag.Int("id", 0, "Id of server")
var startRange = flag.Int("sr", 0, "Key range start")
var sport = flag.Int("sport", 7070, "the port of the server")
var separate = flag.Bool("sp", false, "each batch contain one opeartion type")
var algo = flag.String("algo", "abd", "algorithm")

var INV = uint8(0)
var RES = uint8(1)

func init() {
	config.Load()
	flag.Parse()
	dlog.Setup(*serverId)
}

type Summary struct {
	AckNum     int
	TotalLat   int64
	LatArray   []int64
	MaxLat     int64
	TotalQueue int64
	TotalPrc   int64
	TotalExec  int64
	TotalSlow  int64
	TotalRead  int64
}

type OperationLog struct {
	Type uint8
	Op   kvproto.Operation
	K    kvproto.Key
	V    kvproto.Value
	Ts   int64
}

func main() {
	b := config.GetConfig().Benchmark
	conflicts := b.Conflicts
	readRatio := 1 - b.W
	reqNum := b.Throttle
	batchSize := config.GetConfig().BatchSize
	concurrency := b.Concurrency

	if conflicts > 100 {
		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	}

	tsArray := make([]int64, reqNum)
	ackTsArray := make([]int64, reqNum)
	readArray := make([]bool, reqNum)
	kArray := make([]int64, reqNum)
	zipGenerator := util.NewZipfianWithItems(int64(b.K), b.ZipfianTheta)
	seed := rand.New(rand.NewSource(int64(*serverId)))
	for i := 0; i < reqNum; i++ {
		if b.Distribution == "zipfan" {
			kArray[i] = zipGenerator.Next(seed)
		} else {
			r := rand.Intn(100)
			if r < conflicts {
				kArray[i] = 42
			} else {
				kArray[i] = int64(*startRange + 43 + i)
			}
		}
		tsArray[i] = 0
		ackTsArray[i] = 0
	}

	for i := 0; i < reqNum; {
		bNum := min(batchSize, reqNum-i)
		isRead := false
		if rand.Float64() < readRatio {
			isRead = true
		}
		if *separate {
			for j := 0; j < bNum; j++ {
				readArray[i] = isRead
				if isRead {
				}
				i++
			}
		} else {
			readArray[i] = isRead
			i++
		}
	}

	outfilelock := &sync.Mutex{}
	outFileName := "./linearizability.out"
	f, _ := os.Create(outFileName)
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()
	server, err := net.Dial("tcp", fmt.Sprintf(":%d", *sport))
	if err != nil {
		log.Printf("Error connecting to replica %d\n", *serverId)
	}
	reader := bufio.NewReader(server)
	writer := bufio.NewWriter(server)
	totalCount := 0
	totalLatency := int64(0)
	var respSummary Summary

	if *algo == "abd" {
		inFlight := make(chan bool, concurrency)
		done := make(chan Summary)
		go abdClient(writer, 0, kArray, readArray, reqNum, inFlight, outfilelock, w)
		go getAbdResponse(reader, done, inFlight)
		respSummary = <-done
		totalCount = respSummary.AckNum
		totalLatency = respSummary.TotalLat
	} else { //epaxos
		inFlight := make(chan int, concurrency)
		done := make(chan Summary)
		go epaxosClient(writer, kArray, readArray, reqNum, inFlight, outfilelock, w)
		go readFastEpaxosResponse(reader, inFlight, done)
		respSummary = <-done
		totalCount = respSummary.AckNum
		totalLatency = respSummary.TotalLat
		respSummary.TotalRead = int64(totalCount)
	}

	log.Printf("Throughput: %d req/s", totalCount/b.T)
	log.Printf("Lat per req: %d ms\n", totalLatency/int64(totalCount))
	log.Printf("Total latency: %d ms\n", totalLatency)
	log.Printf("Total slow %d\n", respSummary.TotalSlow)
	log.Printf("Slow rate %f\n", float64(respSummary.TotalSlow)/float64(respSummary.TotalRead))
	stat := Statistic(respSummary.LatArray[:totalCount])
	log.Println(stat)

	dlog.Infof("Throughput: %d req/s", totalCount/b.T)
	dlog.Infof("Lat per req: %d ms\n", totalLatency/int64(totalCount))
	dlog.Infof("Total latency: %d ms\n", totalLatency)
	dlog.Infof("Total slow %d\n", respSummary.TotalSlow)
	dlog.Infof("Slow rate %f\n", float64(respSummary.TotalSlow)/float64(respSummary.TotalRead))
	dlog.Info(stat)

	if b.DumpLatency {
		stat.WriteFile("latency." + fmt.Sprint(*serverId) + ".out")
	}

	server.Close()
}

func min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

//send a single object
func sendObject(writer *bufio.Writer, object interface{}) {
	var bbuf bytes.Buffer
	var byteMsg []byte
	encoder := gob.NewEncoder(&bbuf)
	encoder.Encode(object)
	byteMsg = bbuf.Bytes()
	writer.Write(byteMsg)
	writer.Flush()
}

func abdClient(
	writer *bufio.Writer,
	clientId int,
	kArray []int64,
	rArray []bool,
	txnNum int,
	inFlight chan bool,
	fileLock *sync.Mutex,
	fileWriter *bufio.Writer) {
	time.Sleep(time.Duration(*serverId) * time.Millisecond)
	batchSize := config.GetConfig().BatchSize
	cmd := kvproto.Command{Op: kvproto.PUT, K: 0, Val: 0}
	n := txnNum //per client txn number
	batchNum := (n / batchSize) + 1
	log.Printf("Total req num is %d\n", n)
	benchTime := config.GetConfig().Benchmark.T
	batchInterval := time.Duration(config.GetConfig().Benchmark.T * 1e9 / batchNum)
	ticker := time.NewTicker(batchInterval)
	timer := time.NewTimer(time.Duration(benchTime) * time.Second)
	i := 0
loop:
	for i < n {
		select {
		case <-timer.C:
			break loop
		default:
			//construct transaction
			bNum := min(batchSize, n-i)
			var txn kvproto.Transaction
			for j := 0; j < bNum; j++ {
				cmd.K = kvproto.Key(kArray[clientId*txnNum+i])
				if rArray[i] {
					cmd.Op = kvproto.GET
				} else {
					cmd.Op = kvproto.PUT
					cmd.Val = kvproto.Value(rand.Int63n(10000000))
				}

				txn.Commands = append(txn.Commands, cmd)
				i++
			}

			if rArray[i-1] {
				txn.ReadOnly = 1
			} else {
				txn.ReadOnly = 0
			}

			//hack to avoid deadlock
			sort.Slice(txn.Commands, func(i, j int) bool {
				return txn.Commands[i].K < txn.Commands[j].K
			})

			<-ticker.C
			txn.Ts = util.MakeTimestamp(0)
			txn.TID = int64(i)
			sendObject(writer, txn)
			inFlight <- true
		}
	}
}

func getAbdResponse(reader *bufio.Reader, done chan Summary, inFlight chan bool) {
	var summary Summary //result summary
	summary.LatArray = make([]int64, config.GetConfig().Benchmark.Throttle)
	timer := time.NewTimer(time.Duration(config.GetConfig().Benchmark.T) * time.Second)
	idx := 0
	batchSize := config.GetConfig().BatchSize
	respMap := make(map[int64]int)
	respChan := make(chan kvproto.Response, config.GetConfig().BufferSize)

	go func() {
		for {
			gobReader := gob.NewDecoder(reader)
			var resp kvproto.Response
			if err := gobReader.Decode(&resp); err != nil {
				continue
			}
			respChan <- resp
		}
	}()

loop:
	for {
		select {
		case <-timer.C:
			break loop
		case resp := <-respChan:
			tsNow := util.MakeTimestamp(0)
			summary.AckNum += resp.Size
			respMap[resp.TID] += resp.Size
			for i := 0; i < resp.Size; i++ {
				summary.LatArray[idx] = tsNow - resp.Ts
				idx++
			}
			summary.TotalLat += (tsNow - resp.Ts) * int64(resp.Size)
			if respMap[resp.TID] == batchSize {
				<-inFlight
			}

			if len(resp.Vals) > 0 {
				summary.TotalRead += int64(resp.Size)
				if resp.IsFast == uint8(0) {
					summary.TotalSlow += int64(resp.Size)
				}
			}

		}
	}
	log.Print("Client done\n")
	done <- summary
}

func epaxosClient(writer *bufio.Writer,
	kArray []int64, rArray []bool, txnNum int, inFlight chan int,
	fileLock *sync.Mutex, fileWriter *bufio.Writer) {
	time.Sleep(time.Duration(*serverId) * time.Millisecond)
	batchSize := config.GetConfig().BatchSize
	log.Printf("Total req num is %d\n", txnNum)
	var dummyValue state.Value
	testStart := time.Now()
	benchTime := config.GetConfig().Benchmark.T
	batchNum := (txnNum / batchSize) + 1
	batchInterval := time.Duration(config.GetConfig().Benchmark.T * 1e9 / batchNum)
	ticker := time.NewTicker(batchInterval)
	i := 0
	for i < txnNum {
		args := genericsmrproto.Propose{
			CommandId: 0,
			Command: state.Command{
				Op: state.PUT,
				K:  0,
				V:  dummyValue},
			Timestamp: 0,
			OutQueue:  0,
			Commit:    0,
			Executed:  0,
			Slow:      0}
		now := time.Now()
		if (now.Sub(testStart)).Seconds() > float64(benchTime) {
			break //terminate test
		}
		//construct send
		//write in a batch
		bNum := min(batchSize, txnNum-i)
		//i_start := i
		timeInt64 := util.MakeTimestamp(0)
		for j := 0; j < bNum; j++ {
			args.Timestamp = timeInt64
			args.Command.K = state.Key(kArray[i])
			if rArray[i] {
				args.Command.Op = state.GET
			} else {
				args.Command.Op = state.PUT
				args.Command.V = state.Value(rand.Int63n(10000000))
			}
			writer.WriteByte(genericsmrproto.PROPOSE)
			args.Marshal(writer)
			i++
		}
		inFlight <- bNum
		writer.Flush()
		<-ticker.C
	}
}

func readFastEpaxosResponse(
	reader *bufio.Reader,
	inFlight chan int,
	done chan Summary) {
	var summary Summary //result summary
	summary.LatArray = make([]int64, config.GetConfig().Benchmark.Throttle)
	benchTime := config.GetConfig().Benchmark.T
	reply := new(genericsmrproto.ProposeDetailReply)
	timer := time.NewTimer(time.Duration(benchTime) * time.Second)
	idx := 0

loop:
	for {
		select {
		case bSize := <-inFlight:
			for i := 0; i < bSize; i++ {
				if err := reply.Unmarshal(reader); err != nil {
					log.Println("Error when reading:", err)
					continue
				}
				timeInt64 := util.MakeTimestamp(0)
				lat := timeInt64 - reply.Timestamp
				summary.TotalSlow += int64(reply.Slow)
				summary.AckNum += 1
				summary.LatArray[idx] = lat
				summary.TotalLat += lat
				idx++
			}
		case <-timer.C:
			break loop
		}
	}
	done <- summary
}
