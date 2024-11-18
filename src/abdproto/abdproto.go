package abdproto

import (
	"encoding/gob"
	"kvproto"
)

func init() {
	gob.Register(AbdWrite2Req{})
	gob.Register(AbdWriteAck{})
	gob.Register(AbdRound1Req{})
	gob.Register(AbdRound1Resp{})
	gob.Register(AbdRead2Req{})
	gob.Register(AbdRead2Ack{})
}

const (
	NONE uint8 = iota
	WRITE1REQ
	WRITE1RESP
	WRITE2REQ
	WRITE2ACK
	WATERMARK
	READ1REQ
	READ1RESP
	READ2REQ
	READ2ACK

	WRITE1RESPWM
	WRITE2REQWM
	WRITE2ACKWM
	WATERMARKWM
	READ1REQWM
	READ1RESPWM
	READ2REQWM
	READ2ACKWM
)

type AbdWrite2Req struct {
	Tid  kvproto.Version
	Ks   []kvproto.Key
	Vals []kvproto.Value
	Vs   kvproto.Version
}

type AbdWriteAck struct {
	//Ks   kvproto.Key
	Tid kvproto.Version
}

type AbdRound1Req struct {
	IsRead bool
	Ks     []kvproto.Key
	Tid    kvproto.Version
}

type AbdRound1Resp struct {
	Tid  kvproto.Version
	Ks   []kvproto.Key
	Vals []kvproto.Value
	Vss  []kvproto.Version
}

type AbdRead2Req struct {
	Tid  kvproto.Version
	Ks   []kvproto.Key
	Vals []kvproto.Value
	Vss  []kvproto.Version
}

type AbdRead2Ack struct {
	Tid kvproto.Version
}

//================add WaterMark info==================================
type AbdWrite2ReqWm struct {
	AbdWrite2Req
	Watermark int64
}

type AbdWriteAckWm struct {
	//Ks   kvproto.Key
	Tid       kvproto.Version
	Watermark int64
}

type AbdRound1ReqWm struct {
	AbdRound1Req
	Watermark int64
}

type AbdRound1RespWm struct {
	AbdRound1Resp
	Watermark int64
}

type AbdRead2ReqWm struct {
	AbdRead2Req
	Watermark int64
}

type AbdRead2AckWm struct {
	Tid       kvproto.Version
	Watermark int64
}
