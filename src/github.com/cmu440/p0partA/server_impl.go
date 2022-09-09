// Implementation of a KeyValueServer. Students should write their code in this file.

package p0partA

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strconv"

	"github.com/cmu440/p0partA/kvstore"
)

type keyValueServer struct {
	// TODO: implement this!
	req     chan []byte
	active  chan int
	dropped chan int
	dict    kvstore.KVStore
}

// New creates and returns (but does not start) a new KeyValueServer.
func New(store kvstore.KVStore) *keyValueServer {
	// TODO: implement this!
	fmt.Println("////////////////////////////////////////////////////////////////////////////////////////////////////////")
	return &keyValueServer{
		req:     make(chan []byte),
		active:  make(chan int),
		dropped: make(chan int),
		dict:    store,
	}
}

func (kvs *keyValueServer) requestRoutine() {
	fmt.Println("BACKGROUND")
	for {
		select {
		case currRequest := <-kvs.req:
			splitRequest := bytes.Split(currRequest, []byte(":"))
			if bytes.Equal(splitRequest[0], []byte{'P', 'u', 't'}) {
				fmt.Println("Put")
				kvs.dict.Put(string(splitRequest[1]), splitRequest[2])
			} else if bytes.Equal(splitRequest[0], []byte{'G', 'e', 't'}) {
				fmt.Println("Get")
				returnedVals := kvs.dict.Get(string(splitRequest[1]))
				for _, v := range returnedVals {
					fmt.Println(v)
					// buffer := splitRequest[2]

				}
			} else if bytes.Equal(splitRequest[0], []byte{'D', 'e', 'l', 'e', 't', 'e'}) {
				fmt.Println("Delete")
				kvs.dict.Delete(string(splitRequest[1]))
			} else if bytes.Equal(splitRequest[0], []byte{'U', 'p', 'd', 'a', 't', 'e'}) {
				fmt.Println("Update")
				kvs.dict.Update(string(splitRequest[1]), splitRequest[2], splitRequest[3])
			}

		}
	}
}

func (kvs *keyValueServer) Start(port int) error {
	// TODO: implement this!
	l, err := net.Listen("tcp", "localhost"+":"+strconv.Itoa(port))
	if err != nil {
		fmt.Println("Error listening:", err.Error())
	}
	// Close the listener when the application closes.
	// defer l.Close()
	fmt.Println("Listening on " + "localhost" + ":" + strconv.Itoa(port))

	//Start background routine
	go kvs.requestRoutine()
	fmt.Println("AFTER BACKGROUND")
	go checkConn(l, kvs)

	return err
}
func checkConn(l net.Listener, kvs *keyValueServer) {
	for {
		fmt.Println("IN LOOP")
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
		}
		fmt.Println("SBIDB")
		// Handle connections in a new goroutine.
		go handleRequest(conn, kvs)
	}
}

func handleRequest(conn net.Conn, kvs *keyValueServer) {
	fmt.Println("HANDLE")
	// Make a buffer to hold incoming data.
	// buf := make([]byte, 5000)
	fmt.Println(len(kvs.active))

	// fmt.Println(len(kvs.active))
	// Read the incoming connection into the buffer.
	fmt.Println("BEFORE READ")
	reader := bufio.NewReader(conn)
	fmt.Println(reader)
	fmt.Println("AFTER READ")
	// clientWriteChannel := make(chan []byte)
	// buf = append(buf, []byte(":"+(&clientWriteChannel)...))
	//Put message onto request channel
	// kvs.req <- buf
	kvs.active <- 0
	fmt.Println(len(kvs.active))
	fmt.Println("ENDDD")
	// Send a response back to person contacting us.
	// conn.Write([]byte("Message received.\n"))
	// Close the connection when you're done with it.
	// conn.Close()
}

func (kvs *keyValueServer) Close() {
	fmt.Println("CLOSE")
	// TODO: implement this!
}

func (kvs *keyValueServer) CountActive() int {
	// TODO: implement this!
	fmt.Println("ACTIVE")
	fmt.Println(len(kvs.active))
	return len(kvs.active)
}

func (kvs *keyValueServer) CountDropped() int {
	fmt.Println("DROP")
	// TODO: implement this!
	return len(kvs.dropped)
}

// TODO: add additional methods/functions below!
