// Implementation of a KeyValueServer. Students should write their code in this file.

package p0partA

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
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
	a           int
	d           int
	l           net.Listener
}
type client struct {
	connection     net.Conn
	responseBuffer chan string
	quit           chan int
}

type query struct {
	queryType   string
	key         string
	v1          []byte
	v2          []byte
	clientIndex int
	queryClient client
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
		a:           0,
		d:           0,
		l:           nil,
	}
	return KeyValueServer(server)
}
func (kvs *keyValueServer) writeClientBuffer(currClient client) {
	// fmt.Println("START WRITE")
	// fmt.Println(currClient)
	for {
		select {
		case clientMessage := <-currClient.responseBuffer:
			fmt.Println("GOT FROM CLIENT BUFFER")
			fmt.Println([]byte(clientMessage))
			_, err := currClient.connection.Write([]byte(clientMessage))
			if err != nil {
				fmt.Println("ERROR IN READING OFF CLIENT BUFFER FOR CONNECTION:" + currClient.connection.LocalAddr().String())
				return
			}
			fmt.Println("DONE WRITING")

		}

	}
}
func (kvs *keyValueServer) bgClientRead(currClient client) {
	// fmt.Println("START READ")
	// fmt.Println(currClient)
	reader := bufio.NewReader(currClient.connection)
	for {
		select {
		case <-currClient.quit:
			fmt.Println("EXIT READ")
			return
		default:
			msg, e := reader.ReadBytes('\n')
			if e == io.EOF {
				kvs.queries <- query{
					queryType:   "closeConn",
					queryClient: currClient,
				}
				return

			} else if e != nil {
				return
			} else if e == nil {
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
					// fmt.Println("GETTTT")
					kvs.queries <- query{
						key:       key,
						queryType: "get",
					}
					// fmt.Println("WAIT FOR GETVALUE")
					// fmt.Println(kvs.dict)
					// fmt.Println(key)
					getValue := <-kvs.getValue
					// fmt.Println("GOT GET VALUE")
					// fmt.Println(len(getValue))
					for _, returnValue := range getValue {

						kvs.messages <- message{
							messClient: currClient,
							content:    key + ":" + string(returnValue) + "\n",
						}
					}
					// fmt.Println("DONE PUTTING IN MESSAGES")

				}
			}
		}

	}
}
func (kvs *keyValueServer) bgRoutine() {
	//fmt.Println("START BACKGROUND")
	for {
		select {
		case query := <-kvs.queries:
			if query.queryType == "active" {
				kvs.activeChan <- kvs.a
			} else if query.queryType == "dropped" {
				kvs.droppedChan <- kvs.d
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
				// fmt.Println("BEFORE PUT RETURN IN GETVALUE CHANNEL")
				kvs.getValue <- returnedValues
			} else if query.queryType == "closeConn" {
				// fmt.Println("closeConn")
				// fmt.Println(len(kvs.active))
				// fmt.Println(query.queryClient)
				// fmt.Println(kvs.d)
				kvs.a--
				kvs.d++
				query.queryClient.connection.Close()
				// fmt.Println(kvs.d)
				// fmt.Println(len(kvs.active))
				// fmt.Println("FINISH CLOSE CONNECTION")
			}
		case conn := <-kvs.connections:
			if conn != nil {
				newClient := client{connection: conn, responseBuffer: make(chan string, 500)}
				kvs.a++
				go kvs.writeClientBuffer(newClient)
				go kvs.bgClientRead(newClient)
			}
		case msg := <-kvs.messages:
			// fmt.Println("BEFORE PUT IN CLIENT BUFFER")
			msg.messClient.responseBuffer <- msg.content
			// fmt.Println("AFTER PUT IN CLIENT BUFFER")
		}
	}
}
func (kvs *keyValueServer) Start(port int) error {
	// TODO: implement this!
	fmt.Println(port)
	l, err := net.Listen("tcp", "localhost"+":"+strconv.Itoa(port))
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		return err
	}
	// Close the listener when the application closes.

	fmt.Println("Listening on " + "localhost" + ":" + strconv.Itoa(port))
	kvs.l = l
	//Start background routine
	go kvs.bgRoutine()
	go func() {
		for {
			conn, err := kvs.l.Accept()
			if err != nil {
				fmt.Println("Error accepting: ", err.Error())
			}
			kvs.connections <- conn
		}
	}()

	return nil
}

func (kvs *keyValueServer) Close() {
	fmt.Println("CLOSE")
	// defer kvs.l.Close()
	// for _, currClient := range kvs.active {
	// 	kvs.queries <- query{
	// 		queryType:   "closeConn",
	// 		queryClient: currClient,
	// 	}
	// 	currClient.connection.Close()
	// }
	// fmt.Println(kvs.active)
	// fmt.Println("FINISH CLOSE")
	// TODO: implement this!
}

func (kvs *keyValueServer) CountActive() int {
	// TODO: implement this!
	// fmt.Println("ACTIVE")
	//fmt.Println(kvs.active)
	kvs.queries <- query{
		queryType: "active",
	}
	ans := <-kvs.activeChan
	// fmt.Println(ans)
	return ans
}

func (kvs *keyValueServer) CountDropped() int {
	// fmt.Println("DROP")
	// TODO: implement this!
	kvs.queries <- query{
		queryType: "dropped",
	}
	ans := <-kvs.droppedChan
	// fmt.Println(ans)
	return ans
}

// TODO: add additional methods/functions below!
