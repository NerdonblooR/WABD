package abd

import (
	"abdmeta"
	"abdproto"
	"dlog"
	"genericserver"
	"kv"
	"kvproto"
	"log"
	"util"
)

const RRANGE = int32(5000)

var COUNTER int32 = 0

type AbdTxnHandler struct {
	server      *kv.KVServer
	id          int
	pendingTxns map[kvproto.Version]*abdmeta.TxnState //(kvproto.Version, *TxnState)
	Q           int
}

func NewTxnManager(id int, server *kv.KVServer) *AbdTxnHandler {
	m := &AbdTxnHandler{
		server,
		id,
		make(map[kvproto.Version]*abdmeta.TxnState),
		server.N/2 + 1,
	}
	return m
}

func (abdhandler *AbdTxnHandler) Run() {
	log.Printf("Abd Txn handler Running...")
	//the main process to process message
	for !abdhandler.server.Shutdown {
		m := abdhandler.server.Recv(abdhandler.id)
		switch m := m.(type) {
		case genericserver.ClientTxnRequest:
			abdhandler.processTxn(&m)
		case genericserver.Message:
			from := m.PeerId
			switch payload := m.M.(type) {
			case abdproto.AbdRound1Req:
				abdhandler.handleRound1Req(from, &payload)
			case abdproto.AbdRound1Resp:
				abdhandler.handleRound1Resp(&payload)
			case abdproto.AbdRead2Req:
				abdhandler.handleRead2Req(from, &payload)
			case abdproto.AbdRead2Ack:
				abdhandler.handleRead2Ack(&payload)
			case abdproto.AbdWrite2Req:
				abdhandler.handleWrite2Req(from, &payload)
			case abdproto.AbdWriteAck:
				abdhandler.handleWriteAck(&payload)
			default:
				log.Printf("MISSING")
			}
		default:
			log.Printf("MISSING")
		}
	}
}

func (abdhandler *AbdTxnHandler) makeVersion() kvproto.Version {
	vs := kvproto.Version{
		ServerId: int32(abdhandler.server.Id),
		ThreadId: int32(abdhandler.id),
		Ts:       util.MakeTimestampNoOff(),
		R:        COUNTER,
	}
	COUNTER++
	return vs
}

/**
The main procedure to handle transaction.
*/
func (abdhandler *AbdTxnHandler) processTxn(clientReq *genericserver.ClientTxnRequest) {
	dlog.Printf("Handling Txn")
	server := abdhandler.server
	txn := clientReq.Txn

	readVer := abdhandler.makeVersion()
	readTxnState := abdmeta.NewTxnState(server, &readVer, clientReq, true)

	writeVer := abdhandler.makeVersion()
	writeTxnState := abdmeta.NewTxnState(server, &writeVer, clientReq, false)

	server.Monitor.IncrementQueueDelay(util.MakeTimestampNoOff() - txn.Ts)
	server.Monitor.IncrementTotalTxnNum(1)

	readReq := abdproto.AbdRound1Req{
		IsRead: true,
		Ks:     make([]kvproto.Key, 0),
		Tid:    readVer,
	}

	writeReq := abdproto.AbdRound1Req{
		IsRead: false,
		Ks:     make([]kvproto.Key, 0),
		Tid:    writeVer,
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
		}
	}

	writeTxnState.WriteMaxVs = kvproto.DUMMYVS
	abdhandler.pendingTxns[readVer] = readTxnState
	abdhandler.pendingTxns[writeVer] = writeTxnState

	rres := server.DB.GetLatestValueVersions(readReq.Ks)
	wres := server.DB.GetLatestValueVersions(writeReq.Ks)
	for i, r := range rres {
		readTxnState.InitializeSlot(i, r.Val, r.Vs)

	}

	for _, r := range wres {
		if r.Vs.LargerThan(writeTxnState.WriteMaxVs) {
			writeTxnState.WriteMaxVs = r.Vs
		}
	}

	if len(readReq.Ks) > 0 {
		readTxnState.Read1RespNum += 1
		server.BcastInterface(readReq)
	}

	if len(writeReq.Ks) > 0 {
		writeTxnState.Write1RespNum += 1
		server.BcastInterface(writeReq)
	}

	dlog.Printf("server id : %d, Handling txn version:", server.Id)
}

func (abdhandler *AbdTxnHandler) handleRound1Req(peerId int, rReq *abdproto.AbdRound1Req) {
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
		readResp.Vals = append(readResp.Vals, value)
		readResp.Vss = append(readResp.Vss, vs)
	}
	server.Send(peerId, readResp)
}

func (abdhandler *AbdTxnHandler) handleForRead(txnState *abdmeta.TxnState, resp *abdproto.AbdRound1Resp) {
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
			//bypass the second round, if a quorum return the same max vs
			if count < abdhandler.Q {
				read2Req.Ks = append(read2Req.Ks, k)
				read2Req.Vals = append(read2Req.Vals, val)
				read2Req.Vss = append(read2Req.Vss, vs)
				txnState.SlowReadIdx = append(txnState.SlowReadIdx, idx)
			} else {
				//fast round commit: add to fast round commit set
				//add another mthod to reply fast round
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
			//partial fast reply
			txnState.FastReplyToClient()
		}
	}
}

func (abdhandler *AbdTxnHandler) handleForWrite(txnState *abdmeta.TxnState, resp *abdproto.AbdRound1Resp) {
	server := abdhandler.server
	txnState.UpdateWrite1Response(resp)
	server.DB.UpdateValues(resp.Ks, resp.Vals, resp.Vss)
	if txnState.Write1RespNum == abdhandler.Q {
		txnState.Phase1Done = true
		write2Req := &abdproto.AbdWrite2Req{
			Tid:  txnState.Tid,
			Ks:   txnState.WriteKeys,
			Vals: txnState.WriteValues,
			Vs:   kvproto.Version{},
		}

		txnState.WriteMaxVs.Ts += 1
		txnState.WriteMaxVs.ServerId = txnState.Tid.ServerId
		txnState.WriteMaxVs.ThreadId = txnState.Tid.ThreadId
		txnState.WriteMaxVs.R = txnState.Tid.R

		write2Req.Vs = txnState.WriteMaxVs

		server.DB.UpdateValuesOneVer(write2Req.Ks, write2Req.Vals, txnState.WriteMaxVs)
		txnState.Write2RespNum += 1
		abdhandler.server.BcastInterface(write2Req)
	}
}

func (abdhandler *AbdTxnHandler) handleRound1Resp(resp *abdproto.AbdRound1Resp) {
	dlog.Printf("server id : %d, handleRead1Resp", abdhandler.server.Id)
	txnState := abdhandler.pendingTxns[resp.Tid]
	if !txnState.Phase1Done {
		if txnState.IsRead {
			abdhandler.handleForRead(txnState, resp)
		} else {
			abdhandler.handleForWrite(txnState, resp)
		}
	}
}

func (abdhandler *AbdTxnHandler) handleRead2Req(peerId int, rReq *abdproto.AbdRead2Req) {
	dlog.Printf("server id : %d, handleRead2Req", abdhandler.server.Id)
	server := abdhandler.server
	read2Ack := abdproto.AbdRead2Ack{
		Tid: rReq.Tid,
	}
	server.DB.UpdateValues(rReq.Ks, rReq.Vals, rReq.Vss)
	abdhandler.server.Send(peerId, read2Ack)
}

func (abdhandler *AbdTxnHandler) handleRead2Ack(r2Ack *abdproto.AbdRead2Ack) {
	dlog.Printf("server id : %d, handleReadResp", abdhandler.server.Id)
	txnState := abdhandler.pendingTxns[r2Ack.Tid]
	if !txnState.Done {
		txnState.Read2RespNum += 1
		if txnState.Read2RespNum == abdhandler.Q {
			txnState.Done = true
			txnState.ProcDoneTS = util.MakeTimestamp(0)
			abdhandler.server.Monitor.IncrementProcDelay(len(txnState.ReadKeys), txnState.ProcDoneTS-txnState.QueueOut)
			txnState.ReplyToClient()
		}
	}
}

/*
=========Writes=====================
*/
func (abdhandler *AbdTxnHandler) handleWrite2Req(peerId int, wReq *abdproto.AbdWrite2Req) {
	dlog.Printf("server id : %d, handleWrite2Req", abdhandler.server.Id)
	server := abdhandler.server
	//request version
	rVs := wReq.Vs
	server.DB.UpdateValuesOneVer(wReq.Ks, wReq.Vals, rVs)
	writeAck := abdproto.AbdWriteAck{
		Tid: wReq.Tid,
	}
	//send out Ack
	server.Send(peerId, writeAck)
}

func (abdhandler *AbdTxnHandler) handleWriteAck(wAck *abdproto.AbdWriteAck) {
	dlog.Printf("server id : %d, handleWrite2Ack", abdhandler.server.Id)
	txnState := abdhandler.pendingTxns[wAck.Tid]
	if !txnState.Done {
		txnState.Write2RespNum += 1
		if txnState.Write2RespNum == abdhandler.Q {
			txnState.Done = true
			txnState.ProcDoneTS = util.MakeTimestamp(0)
			abdhandler.server.Monitor.IncrementProcDelay(len(txnState.ReadKeys), txnState.ProcDoneTS-txnState.QueueOut)
			txnState.ReplyToClient()
		}
	}
}
