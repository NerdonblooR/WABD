package gossiper

import (
	"abdmeta"
	"config"
	"dlog"
	"encoding/gob"
	"kv"
	"net"
	"sync"
	"time"
	"util"
)

//to do add this to confic
const PERIOD = 2

func init() {
	gob.Register(Watermark{})
}

type Watermark struct {
	ID int
	Ts int64
}

type WatermarkManager struct {
	sync.Mutex
	watermarks     map[int]*int64 //map server id to gossiper
	server         *kv.KVServer
	encoders       []*gob.Encoder
	watermarkRead  chan *abdmeta.TxnState
	watermarkWrite chan *abdmeta.TxnState //for replying
	pendingWrites  chan *abdmeta.TxnState //updateing watermark
}

// NewReplica generates WABD replica
func NewWatermarkManager(id int, server *kv.KVServer) *WatermarkManager {
	g := new(WatermarkManager)
	g.server = server
	if config.GetConfig().AbdType != config.CABD {
		g.encoders = make([]*gob.Encoder, config.GetConfig().N())
		g.watermarks = make(map[int]*int64)
		g.watermarkRead = make(chan *abdmeta.TxnState, config.GetConfig().BufferSize)
		g.watermarkWrite = make(chan *abdmeta.TxnState, config.GetConfig().BufferSize)
		g.pendingWrites = make(chan *abdmeta.TxnState, config.GetConfig().BufferSize)

		for id := 0; id < config.GetConfig().N(); id++ {
			wm := int64(0)
			g.watermarks[id] = &wm
			if id != server.Id {
				g.encoders[id] = gob.NewEncoder(server.SecondaryPeers[id])
			}
		}
		g.run()
	}
	return g
}

func (w *WatermarkManager) run() {
	for idx, conn := range w.server.SecondaryPeers {
		if idx != w.server.Id {
			//initiate message receiver
			go func(conn net.Conn, peerId int) {
				decoder := gob.NewDecoder(conn)
				defer conn.Close()
				for {
					var wm Watermark
					err := decoder.Decode(&wm)
					if err != nil {
						dlog.Error(err)
						return
					}
					*w.watermarks[peerId] = wm.Ts
				}
			}(conn, idx)
		}
	}
	go w.updateWatermark()
	go w.handlePendingOps(w.watermarkRead)
	go w.handlePendingOps(w.watermarkWrite)
}

func (w *WatermarkManager) TrackTxnState(txnState *abdmeta.TxnState) {
	if config.GetConfig().AbdType != config.CABD {
		if txnState.IsRead {
			w.watermarkRead <- txnState

		} else {
			w.pendingWrites <- txnState
			w.watermarkWrite <- txnState
		}
	}
}

/*
Watermark Management
*/
func (w *WatermarkManager) updateWatermark() {
	//last entry that has not been done
	var last *abdmeta.TxnState = nil
	lastWaterMark := int64(0)
	ticker := time.NewTicker(time.Duration(PERIOD) * time.Millisecond)
	for {
		myWatermark := lastWaterMark
		if last == nil || last.FullyReplicated {
			if last != nil {
				myWatermark = last.Tid.Ts - 1
			}
			if len(w.pendingWrites) == 0 {
				myWatermark = util.MakeTimestamp(w.server.Offset) - 1
				last = nil
			} else {
				st := <-w.pendingWrites
				for st.FullyReplicated && len(w.pendingWrites) > 0 {
					myWatermark = st.Tid.Ts - 1
					st = <-w.pendingWrites
				}
				if !st.FullyReplicated {
					last = st
				} else {
					myWatermark = st.Tid.Ts - 1
					last = nil
				}
			}
		}

		if myWatermark > lastWaterMark {
			*w.watermarks[w.server.Id] = myWatermark
			err := w.gossipWatermark(myWatermark)
			if err != nil {
				return
			}
			lastWaterMark = myWatermark
		}
		<-ticker.C
	}
}

func (w *WatermarkManager) handlePendingOps(opChan chan *abdmeta.TxnState) {
	//process pending reads
	lastWatermark := int64(0)
	for st := range opChan {
		for st.Tid.Ts >= lastWatermark {
			lastWatermark = w.GetGlobalWatermark()
			time.Sleep(time.Duration(PERIOD) * time.Millisecond)
		}
		if !st.Replied { //some can be done but not replied yet
			st.Done = true
			if len(st.ReadKeys) > 0 {
				for _, result := range w.server.DB.GetLatestValueVersions(st.ReadKeys) {
					st.ReadValues = append(st.ReadValues, result.Val)
				}
			}
			st.WatermarkReplyToClient()
		}
	}
}

func (w *WatermarkManager) gossipWatermark(waterMark int64) error {
	wm := Watermark{ID: w.server.Id, Ts: waterMark}
	for idx, encoder := range w.encoders {
		if idx != w.server.Id {
			err := encoder.Encode(&wm)
			if err != nil {
				dlog.Error(err)
				return err
			}
		}
	}
	return nil
}

func (w *WatermarkManager) GetPendingReadLen() int {
	return len(w.watermarkRead)
}

func (w *WatermarkManager) GetGlobalWatermark() int64 {
	min := *w.watermarks[0]
	for _, v := range w.watermarks {
		if *v < min {
			min = *v
		}
	}
	return min
}
