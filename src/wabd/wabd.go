package wabd

import (
	"abdmeta"
	"abdproto"
	"config"
	"dlog"
	"genericserver"
	"gossiper"
	"kv"
	"kvproto"
	"log"
	"math"
	"math/rand"
	"time"
	"util"
)

const RRANGE = int32(5000)

var COUNTER int32 = 0

type WabdHandler struct {
	server      *kv.KVServer
	id          int
	pendingTxns map[kvproto.Version]*abdmeta.TxnState
	g           *gossiper.WatermarkManager
	Q           int
	offset      int64
}

func NewTxnManager(id int, server *kv.KVServer, g *gossiper.WatermarkManager) *WabdHandler {
	m := &WabdHandler{
		server,
		id,
		make(map[kvproto.Version]*abdmeta.TxnState),
		g,
		server.N/2 + 1,
		server.Offset,
	}
	return m
}

func (handler *WabdHandler) Run() {
	log.Printf("Abd Txn handler Running...")
	//the main process to process message
	for !handler.server.Shutdown {
		m := handler.server.Recv(handler.id)
		switch m := m.(type) {
		case genericserver.ClientTxnRequest:
			handler.processTxn(&m)
		case genericserver.Message:
			from := m.PeerId
			switch payload := m.M.(type) {
			case abdproto.AbdRound1Req:
				handler.handleRound1Req(from, &payload)
			case abdproto.AbdRound1Resp:
				handler.handleRound1Resp(&payload)
			case abdproto.AbdRead2Req:
				handler.handleRead2Req(from, &payload)
			case abdproto.AbdRead2Ack:
				handler.handleRead2Ack(&payload)
			case abdproto.AbdWrite2Req:
				handler.handleWrite2Req(from, &payload)
			case abdproto.AbdWriteAck:
				handler.handleWriteAck(&payload)
			default:
				log.Printf("MISSING")
			}
		default:
			log.Printf("MISSING")
		}
	}
}

func (handler *WabdHandler) makeVersion() kvproto.Version {
	vs := kvproto.Version{
		ServerId: int32(handler.server.Id),
		ThreadId: int32(handler.id),
		Ts:       util.MakeTimestamp(handler.offset),
		R:        COUNTER,
	}
	COUNTER++
	return vs
}

/**
The main procedure to handle transaction.
*/
func (handler *WabdHandler) processTxn(clientReq *genericserver.ClientTxnRequest) {
	//create a version of the txn
	dlog.Printf("Handling Txn")
	server := handler.server
	txn := clientReq.Txn

	readVer := handler.makeVersion()
	readTxnState := abdmeta.NewTxnState(server, &readVer, clientReq, true)

	writeVer := handler.makeVersion()
	writeTxnState := abdmeta.NewTxnState(server, &writeVer, clientReq, false)

	server.Monitor.IncrementQueueDelay(util.MakeTimestamp(handler.offset) - txn.Ts)
	server.Monitor.IncrementTotalTxnNum(1)

	readReq := abdproto.AbdRound1Req{
		IsRead: true,
		Ks:     make([]kvproto.Key, 0),
		Tid:    readVer,
	}

	writeReq := abdproto.AbdWrite2Req{
		Tid:  writeVer,
		Ks:   make([]kvproto.Key, 0),
		Vals: make([]kvproto.Value, 0),
	}

	for _, cmd := range txn.Commands {
		if cmd.Op == kvproto.GET {
			readReq.Ks = append(readReq.Ks, cmd.K)
			readTxnState.ReadKeys = append(readTxnState.ReadKeys, cmd.K)
			readTxnState.ReadValues = append(readTxnState.ReadValues, kvproto.Value(0))
			readTxnState.ReadMaxVs = append(readTxnState.ReadMaxVs, kvproto.DUMMYVS)
			readTxnState.ReadMaxVsCount = append(readTxnState.ReadMaxVsCount, 0)
		} else {
			writeTxnState.WriteKeys = append(writeTxnState.WriteKeys, cmd.K)
			writeTxnState.WriteValues = append(writeTxnState.WriteValues, cmd.Val)
			writeReq.Ks = append(writeReq.Ks, cmd.K)
			writeReq.Vals = append(writeReq.Vals, cmd.Val)
		}
	}

	writeTxnState.WriteMaxVs = kvproto.DUMMYVS
	handler.pendingTxns[readVer] = readTxnState
	handler.pendingTxns[writeVer] = writeTxnState

	if len(readReq.Ks) > 0 {
		readTxnState.Read1RespNum += 1
		readVals := server.DB.GetLatestValueVersions(readReq.Ks)
		for i, readResult := range readVals {
			if readResult.HasKey {
				readTxnState.InitializeSlot(i, readResult.Val, readResult.Vs)
			}
		}
		if config.GetConfig().AbdType == config.CABD {
			server.BcastInterface(readReq)
		} else if config.GetConfig().AbdType == config.HYBRID {
			prob_d := 1.0
			pendingNum := handler.g.GetPendingReadLen()
			readThresh := config.GetConfig().ReadThresh
			if pendingNum > readThresh {
				exponent := float64(pendingNum-readThresh) / float64(readThresh)
				prob_d = 1.0 / (1.0 + math.Exp(exponent))
			}
			if rand.Float64() < prob_d {
				server.BcastInterface(readReq)
			}
		}

		if config.GetConfig().AbdType != config.CABD {
			handler.g.TrackTxnState(readTxnState)
		}
	}

	if len(writeReq.Ks) > 0 {
		server.BcastInterface(writeReq)
		handler.g.TrackTxnState(writeTxnState)
	}
	//add txn to pendingTxn list
	dlog.Printf("server id : %d, Handling txn version:", server.Id)
}

func (abdhandler *WabdHandler) handleRound1Req(peerId int, rReq *abdproto.AbdRound1Req) {
	dlog.Printf("server id : %d, handleRound1Req", abdhandler.server.Id)
	server := abdhandler.server
	readResp := abdproto.AbdRound1Resp{
		Tid:  rReq.Tid,
		Ks:   rReq.Ks,
		Vals: make([]kvproto.Value, 0),
		Vss:  make([]kvproto.Version, 0),
	}
	var value kvproto.Value
	var vs kvproto.Version

	valAndVers := server.DB.GetLatestValueVersions(rReq.Ks)

	for _, valAndVer := range valAndVers {
		if valAndVer.HasKey {
			value = valAndVer.Val
			vs = valAndVer.Vs
		}
		if rReq.IsRead {
			readResp.Vals = append(readResp.Vals, value)
		}
		readResp.Vss = append(readResp.Vss, vs)
	}
	server.Send(peerId, readResp)
}

func (abdhandler *WabdHandler) handleForRead(txnState *abdmeta.TxnState, resp *abdproto.AbdRound1Resp) {
	txnState.UpdateRead1Response(resp)
	server := abdhandler.server
	if txnState.Read1RespNum == abdhandler.Q {
		txnState.Phase1Done = true
		read2Req := abdproto.AbdRead2Req{
			Tid:  txnState.Tid,
			Ks:   make([]kvproto.Key, 0),
			Vals: make([]kvproto.Value, 0),
			Vss:  make([]kvproto.Version, 0),
		}
		server.DB.UpdateValues(txnState.ReadKeys, txnState.ReadValues, txnState.ReadMaxVs)
		for idx, k := range txnState.ReadKeys {
			vs := txnState.ReadMaxVs[idx]
			val := txnState.ReadValues[idx]
			count := txnState.ReadMaxVsCount[idx]
			if count < abdhandler.Q {
				read2Req.Ks = append(read2Req.Ks, k)
				read2Req.Vals = append(read2Req.Vals, val)
				read2Req.Vss = append(read2Req.Vss, vs)
				txnState.SlowReadIdx = append(txnState.SlowReadIdx, idx)
			} else {
				txnState.FastReadIdx = append(txnState.FastReadIdx, idx)
			}
		}

		if len(read2Req.Ks) > 0 {
			txnState.Read2RespNum += 1 //count my own response.
			abdhandler.server.Monitor.IncrementTwoRoundCommit(1)
			abdhandler.server.BcastInterface(read2Req)
		} else {
			//txn done
			txnState.Done = true
			txnState.ProcDoneTS = util.MakeTimestamp(0)
			abdhandler.server.Monitor.IncrementProcDelay(len(txnState.ReadKeys), txnState.ProcDoneTS-txnState.QueueOut)
		}

		if len(txnState.FastReadIdx) > 0 {
			maxVer := txnState.GetReadMaxVs(txnState.FastReadIdx)
			go func() {
				sleepTil(maxVer.Ts+int64(4*config.GetConfig().Offset), abdhandler.offset)
				txnState.FastReplyToClient()
			}()
		}
	}
}

func (abdhandler *WabdHandler) handleRound1Resp(resp *abdproto.AbdRound1Resp) {
	dlog.Printf("server id : %d, handleRead1Resp", abdhandler.server.Id)
	txnState := abdhandler.pendingTxns[resp.Tid]
	if !txnState.Phase1Done {
		//only handle read
		abdhandler.handleForRead(txnState, resp)
	}
}

func (abdhandler *WabdHandler) handleRead2Req(peerId int, rReq *abdproto.AbdRead2Req) {
	dlog.Printf("server id : %d, handleRead2Req", abdhandler.server.Id)
	server := abdhandler.server
	read2Ack := abdproto.AbdRead2Ack{
		Tid: rReq.Tid,
	}
	server.DB.UpdateValues(rReq.Ks, rReq.Vals, rReq.Vss)
	abdhandler.server.Send(peerId, read2Ack)
}

func (abdhandler *WabdHandler) handleRead2Ack(r2Ack *abdproto.AbdRead2Ack) {
	dlog.Printf("server id : %d, handleReadResp", abdhandler.server.Id)
	txnState := abdhandler.pendingTxns[r2Ack.Tid]
	if !txnState.Done {
		txnState.Read2RespNum += 1
		if txnState.Read2RespNum == abdhandler.Q {
			txnState.Done = true
			txnState.ProcDoneTS = util.MakeTimestamp(0)
			abdhandler.server.Monitor.IncrementProcDelay(len(txnState.ReadKeys), txnState.ProcDoneTS-txnState.QueueOut)
			maxVer := txnState.GetReadMaxVs(txnState.SlowReadIdx)
			go func() {
				sleepTil(maxVer.Ts+int64(4*config.GetConfig().Offset), abdhandler.offset)
				txnState.ReplyToClient()
			}()
		}
	}
}

/*
=========Writes=====================
*/
func (abdhandler *WabdHandler) handleWrite2Req(peerId int, wReq *abdproto.AbdWrite2Req) {
	dlog.Printf("server id : %d, handleWrite2Req", abdhandler.server.Id)
	server := abdhandler.server
	//request version
	rVs := wReq.Tid
	server.DB.UpdateValuesOneVer(wReq.Ks, wReq.Vals, rVs)
	writeAck := abdproto.AbdWriteAck{
		Tid: wReq.Tid,
	}
	//send out Ack
	server.Send(peerId, writeAck)
}

func (abdhandler *WabdHandler) handleWriteAck(wAck *abdproto.AbdWriteAck) {
	dlog.Printf("server id : %d, handleWrite2Ack", abdhandler.server.Id)
	server := abdhandler.server
	txnState := abdhandler.pendingTxns[wAck.Tid]
	txnState.Write2RespNum += 1
	if txnState.Write2RespNum == config.GetConfig().N()-1 { //exclude itself
		txnState.FullyReplicated = true
	}
	if config.GetConfig().AbdType != config.PUREWATER {
		if !txnState.Done {
			if txnState.Write2RespNum == abdhandler.Q {
				txnState.Done = true
				txnState.ProcDoneTS = util.MakeTimestamp(0)
				abdhandler.server.Monitor.IncrementProcDelay(len(txnState.ReadKeys), txnState.ProcDoneTS-txnState.QueueOut)
				go func() {
					sleepTil(txnState.Tid.Ts+int64(4*config.GetConfig().Offset), abdhandler.offset)
					server.DB.UpdateValuesOneVer(txnState.WriteKeys, txnState.WriteValues, txnState.Tid)
					txnState.ReplyToClient()
				}()
			}
		}
	}
}

func sleepTil(end int64, offset int64) {
	now := util.MakeTimestamp(offset)
	if end > now {
		duration := time.Duration(end - now)
		time.Sleep(duration * time.Millisecond)
	}
}
