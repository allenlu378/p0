// Implementation of a KeyValueServer. Students should write their code in this file.

package p0partA

import (
	"bufio"
	"bytes"
	"net"
	"strconv"

	"github.com/cmu440/p0partA/kvstore"
)

type keyValueServer struct {
	// TODO: implement this!
	active      []client
	dropped     []client
	getValue    chan []([]byte)
	dict        kvstore.KVStore
	serverStop  chan bool
	messages    chan message
	connections chan net.Conn
	activeChan  chan int
	droppedChan chan int
	queries     chan query
}
type client struct {
	connection     net.Conn
	responseBuffer chan string
	quit           chan int
}

type query struct {
	queryType string
	key       string
	v1        []byte
	v2        []byte
}
type message struct {
	messClient client
	content    string
}

// New creates and returns (but does not start) a new KeyValueServer.
func New(store kvstore.KVStore) KeyValueServer {
	// TODO: implement this!
	//fmt.Println("////////////////////////////////////////////////////////////////////////////////////////////////////////")
	server := &keyValueServer{
		active:      nil,
		dropped:     nil,
		getValue:    make(chan []([]byte)),
		dict:        store,
		serverStop:  make(chan bool),
		messages:    make(chan message),
		connections: make(chan net.Conn),
		activeChan:  make(chan int),
		droppedChan: make(chan int),
		queries:     make(chan query),
	}
	return KeyValueServer(server)
}
func (kvs *keyValueServer) writeClientBuffer(currClient client) {
	for {
		select {
		case <-currClient.quit:
			//fmt.Println("EXIT WRITE")
			return
		case clientMessage := <-currClient.responseBuffer:
			_, err := currClient.connection.Write([]byte(clientMessage))
			if err != nil {
				//fmt.Println("ERROR IN READING OFF CLIENT BUFFER FOR CONNECTION:" + currClient.connection.LocalAddr().String())
			}

		}

	}
}
func (kvs *keyValueServer) bgClientRead(currClient client) {
	reader := bufio.NewReader(currClient.connection)
	for {
		select {
		case <-currClient.quit:
			//fmt.Println("EXIT READ")
			return
		default:
			msg, e := reader.ReadBytes('\n')
			if e == nil {
				splitMessage := bytes.Split(msg[:len(msg)-1], []byte(":"))
				if string(splitMessage[0]) == "Put" {
					kvs.queries <- query{
						key:       string(splitMessage[1]),
						v1:        splitMessage[2],
						queryType: "put",
					}

				} else if string(splitMessage[0]) == "Delete" {
					kvs.queries <- query{
						key:       string(splitMessage[1]),
						queryType: "delete",
					}

				} else if string(splitMessage[0]) == "Update" {
					kvs.queries <- query{
						key:       string(splitMessage[1]),
						v1:        splitMessage[2],
						v2:        splitMessage[3],
						queryType: "update",
					}

				} else if string(splitMessage[0]) == "Get" {
					key := string(splitMessage[1])
					// //fmt.Println("GETTTT")
					kvs.queries <- query{
						key:       key,
						queryType: "get",
					}
					// //fmt.Println("WAIT FOR GETVALUE")
					getValue := <-kvs.getValue
					for _, returnValue := range getValue {

						kvs.messages <- message{
							messClient: currClient,
							content:    key + ":" + string(returnValue) + "\n",
						}
					}

				}
			}
		}

	}
}
func (kvs *keyValueServer) bgRoutine() {
	//fmt.Println("START BACKGROUND")
	for {
		select {
		case <-kvs.serverStop:
			//fmt.Println("FINISH")
			for _, currClient := range kvs.active {
				currClient.quit <- 0
			}
		case query := <-kvs.queries:
			if query.queryType == "active" {
				kvs.activeChan <- len(kvs.active)
			} else if query.queryType == "drop" {
				kvs.droppedChan <- len(kvs.dropped)
			} else if query.queryType == "put" {
				kvs.dict.Put(query.key, query.v1)
			} else if query.queryType == "delete" {
				kvs.dict.Delete(query.key)
			} else if query.queryType == "update" {
				kvs.dict.Update(query.key, query.v1, query.v2)
			} else if query.queryType == "get" {
				// fmt.Println("POP GET QUERY")
				// fmt.Println(query.key)
				// fmt.Println(kvs.dict)
				returnedValues := kvs.dict.Get(query.key)
				// fmt.Println(returnedValues)
				kvs.getValue <- returnedValues
			}
		case conn := <-kvs.connections:
			if conn != nil {
				newClient := client{connection: conn, responseBuffer: make(chan string, 500)}
				kvs.active = append(kvs.active, newClient)
				go kvs.writeClientBuffer(newClient)
				go kvs.bgClientRead(newClient)
			}
		case msg := <-kvs.messages:
			msg.messClient.responseBuffer <- msg.content
		}
	}
}

func (kvs *keyValueServer) Start(port int) error {
	// TODO: implement this!
	l, err := net.Listen("tcp", "localhost"+":"+strconv.Itoa(port))
	if err != nil {
		//fmt.Println("Error listening:", err.Error())
	}
	// Close the listener when the application closes.
	// defer l.Close()
	//fmt.Println("Listening on " + "localhost" + ":" + strconv.Itoa(port))

	//Start background routine
	go kvs.bgRoutine()
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				//fmt.Println("Error accepting: ", err.Error())
			}
			kvs.connections <- conn
		}
	}()

	return nil
}

func (kvs *keyValueServer) Close() {
	//fmt.Println("CLOSE")
	kvs.serverStop <- true
	// TODO: implement this!
}

func (kvs *keyValueServer) CountActive() int {
	// TODO: implement this!
	//fmt.Println("ACTIVE")
	//fmt.Println(kvs.active)
	kvs.queries <- query{
		queryType: "active",
	}
	ans := <-kvs.activeChan
	//fmt.Println(ans)
	return ans
}

func (kvs *keyValueServer) CountDropped() int {
	//fmt.Println("DROP")
	// TODO: implement this!
	kvs.queries <- query{
		queryType: "dropped",
	}
	ans := <-kvs.droppedChan
	//fmt.Println(ans)
	return ans
}

// TODO: add additional methods/functions below!
