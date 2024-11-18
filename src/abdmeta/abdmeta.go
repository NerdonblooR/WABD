package abdmeta

import (
	"abdproto"
	"dlog"
	"genericserver"
	"kv"
	"kvproto"
	"sync"
)

type TxnState struct {
	sync.RWMutex
	Tid             kvproto.Version
	IsRead          bool
	Write1RespNum   int
	Write2RespNum   int
	Read1RespNum    int
	Read2RespNum    int
	ReadKeys        []kvproto.Key
	WriteKeys       []kvproto.Key
	WriteValues     []kvproto.Value
	WriteMaxVs      kvproto.Version
	ReadMaxVs       []kvproto.Version
	ReadValues      []kvproto.Value
	ReadMaxVsCount  []int
	FastReadIdx     []int
	SlowReadIdx     []int
	QueueOut        int64
	ProcDoneTS      int64
	Phase1Done      bool
	Done            bool
	FullyReplicated bool
	Replied         bool
	FastReplied     bool
	ClientReq       *genericserver.ClientTxnRequest
	server          *kv.KVServer
}

func NewTxnState(s *kv.KVServer, txnVs *kvproto.Version, req *genericserver.ClientTxnRequest, isRead bool) *TxnState {
	txnState := &TxnState{
		Tid:             *txnVs,
		IsRead:          isRead,
		Write1RespNum:   0,
		Write2RespNum:   0,
		Read1RespNum:    0,
		Read2RespNum:    0,
		ReadKeys:        make([]kvproto.Key, 0),
		WriteKeys:       make([]kvproto.Key, 0),
		WriteValues:     make([]kvproto.Value, 0),
		WriteMaxVs:      kvproto.DUMMYVS,
		ReadMaxVs:       make([]kvproto.Version, 0),
		ReadValues:      make([]kvproto.Value, 0),
		FastReadIdx:     make([]int, 0),
		SlowReadIdx:     make([]int, 0),
		ReadMaxVsCount:  make([]int, 0),
		Phase1Done:      false,
		Done:            false,
		FullyReplicated: false,
		Replied:         false,
		ClientReq:       req,
		server:          s,
	}
	return txnState
}

func (ts *TxnState) InitializeSlot(idx int, value kvproto.Value, vs kvproto.Version) {
	ts.ReadMaxVs[idx] = vs
	ts.ReadValues[idx] = value
	ts.ReadMaxVsCount[idx] = 1
}

func (ts *TxnState) UpdateRead1Response(rResp *abdproto.AbdRound1Resp) {
	dlog.Println("Update Response")
	for idx := range rResp.Ks {
		vs := rResp.Vss[idx]
		if vs.LargerThan(ts.ReadMaxVs[idx]) {
			ts.ReadMaxVs[idx] = vs
			ts.ReadValues[idx] = rResp.Vals[idx]
			ts.ReadMaxVsCount[idx] = 1
		} else if vs.Equal(&ts.ReadMaxVs[idx]) {
			ts.ReadMaxVsCount[idx] += 1
		}
	}
	ts.Read1RespNum += 1
}

func (ts *TxnState) UpdateWrite1Response(rResp *abdproto.AbdRound1Resp) {
	dlog.Println("Update Response")
	ts.Write1RespNum += 1
	for idx := range rResp.Ks {
		vs := rResp.Vss[idx]
		if vs.LargerThan(ts.WriteMaxVs) {
			ts.WriteMaxVs = vs
		}
	}
}

func (ts *TxnState) GetReadMaxVs(cmdIdx []int) kvproto.Version {
	maxVer := kvproto.DUMMYVS
	for _, cId := range cmdIdx {
		if ts.ReadMaxVs[cId].LargerThan(maxVer) {
			maxVer = ts.ReadMaxVs[cId]
		}
	}
	return maxVer
}

func (ts *TxnState) ReplyToClient() {
	ts.Lock()
	defer ts.Unlock()
	if !ts.Replied {
		dlog.Printf("Reply to Client")
		var response kvproto.Response
		response.TID = ts.ClientReq.Txn.TID
		response.Ts = ts.ClientReq.Txn.Ts
		response.Size = 0
		response.IsFast = uint8(0)
		if ts.IsRead {
			if !ts.FastReplied {
				for idx := range ts.FastReadIdx {
					response.Vals = append(response.Vals, ts.ReadValues[idx])
				}
				response.Size += len(ts.FastReadIdx)
				ts.FastReplied = true
			}
			for idx := range ts.SlowReadIdx {
				response.Vals = append(response.Vals, ts.ReadValues[idx])
			}
			response.Size += len(ts.SlowReadIdx)
		} else {
			response.Size = len(ts.WriteKeys)
		}
		ts.Replied = true
		ts.server.ReplyClient(ts.ClientReq.ClientWriter, response)
	}
}

func (ts *TxnState) FastReplyToClient() {
	ts.Lock()
	defer ts.Unlock()
	if !ts.Replied {
		dlog.Printf("Fast Reply to Client")
		var response kvproto.Response
		response.TID = ts.ClientReq.Txn.TID
		response.Ts = ts.ClientReq.Txn.Ts
		response.IsFast = uint8(1)
		for idx := range ts.FastReadIdx {
			response.Vals = append(response.Vals, ts.ReadValues[idx])
		}
		response.Size = len(ts.FastReadIdx)
		ts.FastReplied = true
		if len(ts.SlowReadIdx) == 0 {
			ts.Replied = true
		}
		ts.server.ReplyClient(ts.ClientReq.ClientWriter, response)
	}
}

func (ts *TxnState) WatermarkReplyToClient() {
	ts.Lock()
	defer ts.Unlock()
	if !ts.Replied {
		dlog.Printf("Reply to Client")
		var response kvproto.Response
		response.TID = ts.ClientReq.Txn.TID
		response.Ts = ts.ClientReq.Txn.Ts
		response.IsWater = uint8(1)
		response.Size = 0
		if ts.IsRead {
			if len(ts.FastReadIdx) == 0 && len(ts.SlowReadIdx) == 0 {
				response.Vals = ts.ReadValues
				response.Size = len(ts.ReadKeys)
			} else {
				if !ts.FastReplied {
					for idx := range ts.FastReadIdx {
						response.Vals = append(response.Vals, ts.ReadValues[idx])
					}
					response.Size += len(ts.FastReadIdx)
					ts.FastReplied = true
				}
				for idx := range ts.SlowReadIdx {
					response.Vals = append(response.Vals, ts.ReadValues[idx])
				}
				response.Size += len(ts.SlowReadIdx)
			}
		} else {
			response.Size = len(ts.WriteKeys)
		}
		ts.Replied = true
		ts.server.ReplyClient(ts.ClientReq.ClientWriter, response)
	}
}
