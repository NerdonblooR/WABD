package monitor

import (
	"encoding/json"
	"log"
	"sync/atomic"
)

type Monitor struct {
	EnableMonitor          bool
	TotalTxnNum            uint32
	TotalRead              uint32
	TotalWrite             uint32
	TwoRoundCommit         uint32
	TotalReadCoord         int64
	TotalWriteCoord        int64
	DbReadLat              int64
	DbWriteLat             int64
	DbReadCount            uint32 //time invoked read function
	DBWriteCount           uint32 //time invoked write function
	QueueDelay             int64
	ReadProcDelay          int64
	WriteProcDelay         int64
	OffsetExp              uint32
	WatermarkRead          uint32
	WatermarkWrite         uint32
	WatermarkReadServeTime int64
}

func NewMonitor(doMonitor bool) *Monitor {
	return &Monitor{
		EnableMonitor:          doMonitor,
		TotalTxnNum:            0,
		TotalRead:              0,
		TotalWrite:             0,
		TwoRoundCommit:         0,
		TotalReadCoord:         0,
		TotalWriteCoord:        0,
		DbReadLat:              0,
		DbWriteLat:             0,
		DbReadCount:            0,
		DBWriteCount:           0,
		QueueDelay:             0,
		ReadProcDelay:          0,
		WriteProcDelay:         0,
		OffsetExp:              0,
		WatermarkRead:          0,
		WatermarkWrite:         0,
		WatermarkReadServeTime: 0,
	}
}

func (m *Monitor) IncrementWatermarkServeTime(num int64) {
	if m.EnableMonitor {
		atomic.AddInt64(&m.WatermarkReadServeTime, num)
	}
}

func (m *Monitor) IncrementTotalTxnNum(num uint32) {
	if m.EnableMonitor {
		atomic.AddUint32(&m.TotalTxnNum, num)
	}
}

func (m *Monitor) IncrementTwoRoundCommit(num uint32) {
	if m.EnableMonitor {
		atomic.AddUint32(&m.TwoRoundCommit, num)
	}
}

func (m *Monitor) IncrementTotalWriteCoord(num int64) {
	if m.EnableMonitor {
		atomic.AddInt64(&m.TotalWriteCoord, num)
		atomic.AddUint32(&m.TotalWrite, 1)
	}
}

func (m *Monitor) IncrementTotalReadCoord(num int64) {
	if m.EnableMonitor {
		atomic.AddInt64(&m.TotalReadCoord, num)
		atomic.AddUint32(&m.TotalRead, 1)
	}
}

func (m *Monitor) IncrementDbReadLat(num int64) {
	if m.EnableMonitor {
		atomic.AddInt64(&m.DbReadLat, num)
		atomic.AddUint32(&m.DbReadCount, 1)
	}
}

func (m *Monitor) IncrementDbWriteLat(num int64) {
	if m.EnableMonitor {
		atomic.AddInt64(&m.DbWriteLat, num)
		atomic.AddUint32(&m.DBWriteCount, 1)
	}
}

func (m *Monitor) IncrementQueueDelay(num int64) {
	if m.EnableMonitor {
		atomic.AddInt64(&m.QueueDelay, num)
	}
}

func (m *Monitor) IncrementProcDelay(readKeyNum int, num int64) {
	if m.EnableMonitor {
		if readKeyNum > 0 {
			atomic.AddInt64(&m.ReadProcDelay, num)
		} else {
			atomic.AddInt64(&m.WriteProcDelay, num)
		}

	}
}

func (m *Monitor) IncrementOffExp(num uint32) {
	if m.EnableMonitor {
		atomic.AddUint32(&m.OffsetExp, num)
	}
}

func (m *Monitor) IncrementFastRead(num uint32) {
	if m.EnableMonitor {
		atomic.AddUint32(&m.OffsetExp, num)
		atomic.AddUint32(&m.WatermarkRead, num)
	}
}

func (m *Monitor) IncrementFastWrite(num uint32) {
	if m.EnableMonitor {
		atomic.AddUint32(&m.OffsetExp, num)
		atomic.AddUint32(&m.WatermarkWrite, num)
	}
}

func (m *Monitor) Print() {
	if m.EnableMonitor {
		res, _ := json.Marshal(m)
		log.Println(string(res))
	}
}
