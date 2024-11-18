package mvdb

import (
	"kvproto"
	"log"
	"monitor"
	"sync"
)

type Result struct {
	Val    kvproto.Value
	Vs     kvproto.Version
	HasKey bool
}

type Entry struct {
	Lock *sync.RWMutex
	Vs   *kvproto.Version                  //latest version of the key
	Vals map[kvproto.Version]kvproto.Value //historical values
}

type DB struct {
	sync.RWMutex
	shardNum int
	storage  map[kvproto.Key]kvproto.Value
	vsMap    map[kvproto.Key]kvproto.Version
	monitor  *monitor.Monitor
}

func NewDB(shardNum int, m *monitor.Monitor) *DB {
	db := &DB{
		shardNum: shardNum,
		storage:  make(map[kvproto.Key]kvproto.Value),
		vsMap:    make(map[kvproto.Key]kvproto.Version),
		monitor:  m,
	}
	//load 100K key
	for k := 0; k < 100000; k++ {
		db.storage[kvproto.Key(k)] = kvproto.DUMMYVAL
		db.vsMap[kvproto.Key(k)] = kvproto.DUMMYVS
	}
	log.Printf("Done creating db")
	//maintain a heap
	return db
}

func (db *DB) updateValue(k kvproto.Key, value kvproto.Value, vs kvproto.Version) {
	latestVs := db.vsMap[k]
	if vs.LargerThan(latestVs) {
		db.storage[k] = value
		db.vsMap[k] = vs
	}
}

func (db *DB) UpdateValues(ks []kvproto.Key, values []kvproto.Value, vss []kvproto.Version) {
	db.Lock()
	defer db.Unlock()
	for idx, k := range ks {
		db.updateValue(k, values[idx], vss[idx])
	}
}

func (db *DB) UpdateValuesOneVer(ks []kvproto.Key, values []kvproto.Value, vs kvproto.Version) {
	db.Lock()
	defer db.Unlock()
	for idx, k := range ks {
		db.updateValue(k, values[idx], vs)
	}
}

func (db *DB) GetLatestValueVersions(ks []kvproto.Key) []Result {
	db.RLock()
	defer db.RUnlock()
	res := make([]Result, len(ks))
	for idx, k := range ks {
		res[idx] = Result{Val: db.storage[k], Vs: db.vsMap[k], HasKey: true}
	}
	return res
}
