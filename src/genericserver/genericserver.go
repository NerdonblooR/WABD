package genericserver

import (
	"bufio"
	"bytes"
	"config"
	"dlog"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"kvproto"
	"log"
	"monitor"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const PORTOFF = 2000

type reply struct {
	writer *bufio.Writer
	object interface{}
}

type Message struct {
	PeerId int
	M      interface{}
}

type ClientTxnRequest struct {
	CId          int
	Txn          *kvproto.Transaction
	ClientWriter *bufio.Writer
}

//A generic server
type Server struct {
	Id       int
	N        int
	Listener net.Listener
	//cache of connections to all other replicas
	Peers       []net.Conn
	PeerReaders []*bufio.Reader
	PeerWriters []chan *bufio.Writer
	//dirty hack to separate watermark communication from protocol messages
	SecondaryListener    net.Listener
	SecondaryPeers       []net.Conn
	SecondaryPeerReaders []*bufio.Reader
	SecondaryPeerWriters []chan *bufio.Writer
	OnClientConnect      chan bool
	Shutdown             bool
	ClientWriters        []*bufio.Writer
	Monitor              *monitor.Monitor
	toSend               []chan interface{}
	toProcess            []chan interface{}
	replyChan            chan reply
}

func NewServer(id int, doMonitor bool) *Server {
	N := config.GetConfig().N()
	workerNum := config.GetConfig().WorkerNum
	server := &Server{
		Id:                   id,
		N:                    N,
		Listener:             nil,
		Peers:                make([]net.Conn, N),
		PeerReaders:          make([]*bufio.Reader, N),
		PeerWriters:          make([]chan *bufio.Writer, N),
		SecondaryListener:    nil,
		SecondaryPeers:       make([]net.Conn, N),
		SecondaryPeerReaders: make([]*bufio.Reader, N),
		SecondaryPeerWriters: make([]chan *bufio.Writer, N),
		OnClientConnect:      make(chan bool, 100),
		Shutdown:             false,
		ClientWriters:        make([]*bufio.Writer, 0),
		Monitor:              monitor.NewMonitor(doMonitor),
		toSend:               make([]chan interface{}, N),
		toProcess:            make([]chan interface{}, workerNum),
		replyChan:            make(chan reply, config.GetConfig().ChanBufferSize),
	}
	for i := 0; i < workerNum; i++ {
		server.toProcess[i] = make(chan interface{}, config.GetConfig().ChanBufferSize*N)
	}
	//initialize channels
	for i := 0; i < server.N; i++ {
		if i != server.Id {
			server.PeerWriters[i] = make(chan *bufio.Writer, 1)
			server.SecondaryPeerWriters[i] = make(chan *bufio.Writer, 1)
			server.toSend[i] = make(chan interface{}, config.GetConfig().ChanBufferSize)
		}
	}
	go server.replyWorker()
	server.connectToPeers()
	return server
}

/* ============= */
func (r *Server) connectToPeers() {
	var b [4]byte
	bs := b[:4]
	done := make(chan bool)
	go r.waitForPeerConnections(done)
	//connect to peers
	for i := 0; i < r.Id; i++ {
		for done := false; !done; {
			if conn, err := net.Dial("tcp", config.GetConfig().Addrs[config.ID(fmt.Sprint(i))]); err == nil {
				r.Peers[i] = conn
				done = true
			} else {
				time.Sleep(1e9)
			}
		}
		binary.LittleEndian.PutUint32(bs, uint32(r.Id))
		if _, err := r.Peers[i].Write(bs); err != nil {
			fmt.Println("Write id error:", err)
			continue
		}

	}

	for i := 0; i < r.Id; i++ {
		addr_pair := strings.Split(config.GetConfig().Addrs[config.ID(fmt.Sprint(i))], ":")
		portNum, _ := strconv.Atoi(addr_pair[1])
		secd_addr := fmt.Sprintf("%s:%d", addr_pair[0], PORTOFF+portNum)
		for done := false; !done; {
			if conn, err := net.Dial("tcp", secd_addr); err == nil {
				r.SecondaryPeers[i] = conn
				done = true
			} else {
				time.Sleep(1e9)
			}
		}
		binary.LittleEndian.PutUint32(bs, uint32(r.Id))
		if _, err := r.SecondaryPeers[i].Write(bs); err != nil {
			fmt.Println("Write id error:", err)
			continue
		}

	}

	<-done
	for i := 0; i < r.N; i++ {
		if i != r.Id {
			log.Printf("Replica id: %d initializing Reader Writer For replica %d", r.Id, i)
			r.PeerReaders[i] = bufio.NewReader(r.Peers[i])
			r.PeerWriters[i] <- bufio.NewWriter(r.Peers[i])
			r.SecondaryPeerReaders[i] = bufio.NewReader(r.SecondaryPeers[i])
			r.SecondaryPeerWriters[i] <- bufio.NewWriter(r.SecondaryPeers[i])

		}
	}

	for idx, conn := range r.Peers {
		if idx != r.Id {
			//initiate message sender
			go func(conn net.Conn, peerId int) {
				// w := bufio.NewWriter(conn)
				// codec := NewCodec(config.Codec, conn)
				encoder := gob.NewEncoder(conn)
				defer conn.Close()
				for m := range r.toSend[peerId] {
					err := encoder.Encode(&m)
					if err != nil {
						dlog.Error(err)
					}
				}
			}(conn, idx)

			//initiate message receiver
			go func(conn net.Conn, peerId int) {
				// codec := NewCodec(config.Codec, conn)
				decoder := gob.NewDecoder(conn)
				defer conn.Close()
				//r := bufio.NewReader(conn)
				for {
					var m interface{}
					err := decoder.Decode(&m)
					if err != nil {
						dlog.Error(err)
						return
					}

					//hack using reflection
					value := reflect.ValueOf(m)
					version := value.FieldByName("Tid")
					tId := version.FieldByName("ThreadId").Int()

					r.toProcess[tId] <- Message{peerId, m}
					// select {
					// case <-t.close:
					// 	return
					// default:
					// }
				}
			}(conn, idx)
		}
	}

	log.Printf("Replica id: %d. Done connecting to peers\n", r.Id)
}

func (r *Server) waitForPeerConnections(done chan bool) {
	var b [4]byte
	bs := b[:4]
	portNum, _ := strconv.Atoi(strings.Split(config.GetConfig().Addrs[config.ID(fmt.Sprint(r.Id))], ":")[1])
	r.Listener, _ = net.Listen("tcp", fmt.Sprintf(":%d", portNum))
	r.SecondaryListener, _ = net.Listen("tcp", fmt.Sprintf(":%d", PORTOFF+portNum))
	for i := r.Id + 1; i < r.N; i++ {
		conn, err := r.Listener.Accept()
		if err != nil {
			fmt.Println("Accept error:", err)
			continue
		}
		if _, err := io.ReadFull(conn, bs); err != nil {
			fmt.Println("Connection establish error:", err)
			continue
		}
		id := int32(binary.LittleEndian.Uint32(bs))
		r.Peers[id] = conn
	}

	for i := r.Id + 1; i < r.N; i++ {
		conn, err := r.SecondaryListener.Accept()
		if err != nil {
			fmt.Println("Accept error:", err)
			continue
		}
		if _, err := io.ReadFull(conn, bs); err != nil {
			fmt.Println("Connection establish error:", err)
			continue
		}
		id := int32(binary.LittleEndian.Uint32(bs))
		r.SecondaryPeers[id] = conn
	}
	done <- true
}

//main event processing thread
func (server *Server) Run() {
	log.Printf("handle client request")
	go server.WaitForClientConnections()
	for !server.Shutdown {
		<-server.OnClientConnect
		dlog.Printf("server id : %d, Client connected", server.Id)
	}
}

/* Client connections dispatcher */
func (server *Server) WaitForClientConnections() {
	log.Printf("Waiting....")
	cId := 0
	for !server.Shutdown {
		conn, err := server.Listener.Accept()
		if err != nil {
			log.Println("Accept error:", err)
			continue
		}
		dlog.Printf("server id : %d, Client %d connected", server.Id, cId)
		go server.clientListener(cId, conn)
		server.OnClientConnect <- true
		cId++
	}
}

//client transaction processing dispatcher
func (server *Server) clientListener(cId int, conn net.Conn) {
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	server.ClientWriters = append(server.ClientWriters, writer)
	workerNum := config.GetConfig().WorkerNum

	for !server.Shutdown {

		for i := 0; i < workerNum; i++ {
			decoder := gob.NewDecoder(reader)
			var txn kvproto.Transaction
			if err := decoder.Decode(&txn); err != nil {
				log.Println("Hit error reading txn from client")
				log.Println(err)
				server.Monitor.Print()
				return //EOF
			}
			if config.GetConfig().JustReply {
				var response kvproto.Response
				response.TID = txn.TID
				for _, cmd := range txn.Commands {
					if cmd.Op == kvproto.GET {
						v := kvproto.Value(42)
						response.Vals = append(response.Vals, v)
					} else {
						break
					}
				}
				response.Size = len(txn.Commands)
				server.ReplyClient(writer, response)
			} else {
				clientReq := ClientTxnRequest{
					CId:          cId,
					Txn:          &txn,
					ClientWriter: writer,
				}
				server.toProcess[i] <- clientReq

			}

		}
		//can add a ticket channel here to limit concurrent txn
		// server.TxnHandler.HandleTxn(cId, &txn, writer)
	}
}

func (server *Server) BcastInterface(msg interface{}) {
	for i := 0; i < server.N; i++ {
		if i != server.Id {
			server.toSend[i] <- msg
		}
	}
}

func (server *Server) Send(peerId int, msg interface{}) {
	server.toSend[peerId] <- msg
}

func (server *Server) Recv(tId int) interface{} {
	return <-server.toProcess[tId]
}

func (server *Server) Bcast(msg []byte) {
	for i := 0; i < server.N; i++ {
		if i != server.Id {
			writer := <-server.PeerWriters[i]
			writer.Write(msg)
			writer.Flush()
			server.PeerWriters[i] <- writer
		}
	}
}

func (server *Server) ReplyClient(writer *bufio.Writer, object interface{}) {
	server.replyChan <- reply{writer: writer, object: object}
}

func (server *Server) replyWorker() {
	for rep := range server.replyChan {
		var bbuf bytes.Buffer
		var byteMsg []byte
		encoder := gob.NewEncoder(&bbuf)
		encoder.Encode(rep.object)
		byteMsg = bbuf.Bytes()
		rep.writer.Write(byteMsg)
		rep.writer.Flush()
	}
}
