package tcppool

import (
	"os"
	"testing"
	"net"
	"sync"
	"fmt"
	"encoding/json"
)

import (
	lctx "github.com/moonshadowmobile/logctx"
)

const tlpath = "test/testlogconfig.xml"

var pool *TCPConnectionPool
var listener *net.TCPListener

func TestMain(m *testing.M) {
	// set up some misc stuff needed for logging
	lc := lctx.NewLoggingContext()
	defer lc.Flush()
	err := lc.AddLogger("workflow", tlpath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go startListener(&wg)
	wg.Wait()
	// create the pool
	remote_addr, err := net.ResolveTCPAddr("tcp", "localhost:8988")
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	p, err := NewTCPConnectionPool(lc, true, 5, 10, "tcp", nil, remote_addr, true, "30s", "10s", "5s", 1028)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	err = p.Open()
	if err != nil {
		fmt.Println(err)	
		os.Exit(1)
	}
	pool = p
	os.Exit(m.Run())
}

func TestSendAndReceiveJSON(t *testing.T) {
	resp := make(map[string]interface{})
	err := pool.SendAndReceiveJSON(&resp, []byte(`{"query":"test"}`))
	if err != nil {
		t.Error(err)
	}
	fmt.Println("got resp", resp)
}

func TestSendAndReceiveJSONPiped(t *testing.T) {
	// stubbed
}

func TestSendAndReceiveBinaryPiped(t *testing.T) {
	// stubbed
}

func TestAcquire(t *testing.T) {

}

func TestReleaseUnusable(t *testing.T) {
	// stubbed
}

func TestReleaseReusable(t *testing.T) {
	// stubbed
}

func TestReapLRU(t *testing.T) {
	// stubbed
}

// Tests that sending a stop signal to the goroutine reaper causes it to terminate nearly immediately, even with a low interval set.
func TestStopReapLRU(t *testing.T) {
	// stubbed
}

func startListener(wg *sync.WaitGroup) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:8988")
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	// start up the tcplistener to test against
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	listener = l
	wg.Done()
	for {
		conn, err := l.AcceptTCP()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		go handleConn(conn)
	}
}

func handleConn(conn *net.TCPConn) {
	fmt.Println("new conn", conn)
	d := json.NewDecoder(conn)
	for {
		msg := make(map[string]interface{})
		err := d.Decode(&msg)
		if err != nil {
			if err.Error() == "EOF" {
				fmt.Println("connection closed by client")
				return
			}
			fmt.Println("error decoding query:", err.Error())
		}
		fmt.Println("decoded msg", msg)
		b, err := json.Marshal(msg)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
		conn.Write(b)
	}
}