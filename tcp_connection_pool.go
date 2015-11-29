package tcppool

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

import (
	lctx "github.com/moonshadowmobile/logctx"
)

const cls_tcpconnpool = "TCPConnectionPool"

type TCPConnection struct {
	Connection *net.TCPConn
	last_used  time.Time
	in_use     bool
}
type TCPConnectionPool struct {
	Cls        string
	Usable     bool
	Configured bool
	lc         *lctx.LoggingContext
	//-
	connections      map[uint64]*TCPConnection
	connections_lock sync.RWMutex
	//-
	buf_size          uint16
	release_sub       chan uint64
	stop              chan bool
	stop_autoreap     chan bool
	autoreap_interval *time.Duration
	//-
	// whether or not to attempt to reuse existing connections when they are released back to the pool
	reuse_connections bool
	// Minimum number of connections that should be in the pool. When the pool is Open()ed, this number of connections are preallocated. The autoreaper task will honor this parameter and never reap more connections than is required by min_conns.
	min_conns uint64
	// The maximum number of possible connections in the pool. Attempts to Acquire() a connection that would normally result in an expand() of the pool will block until a connection is Release()d by another connection borrower.
	max_conns uint64
	// How long to allow an unused connection to live before being eligible for auto-reaping.
	max_lru_age_ns int64
	//-
	net_type         string
	local_addr       *net.TCPAddr
	remote_addr      *net.TCPAddr
	keepalive        bool
	keepalive_period *time.Duration
}

func (self *TCPConnectionPool) Acquire() (uint64, *net.TCPConn, error) {
	if self.Usable {
		self.connections_lock.Lock()
		defer self.connections_lock.Unlock()
		num_conns := len(self.connections)
		// this pool contains persistent connections, try to use a preallocated connection
		if self.reuse_connections {
			self.lc.Debugf("workflow", "[%s] Retreiving available connection from pool of size %d", self.Cls, num_conns)
			for k, c := range self.connections {
				if !c.in_use {
					c.in_use = true
					c.last_used = time.Now()
					self.lc.Debugf("workflow", "[%s] Returning first available connection %d, last used %d (unix nano timestamp)", self.Cls, k, c.last_used.UnixNano())
					return k, c.Connection, nil
				}
			}
		}
		// all the connections were in use (or reuse_connections was false and we're doing setup on every call to acquire), if we haven't hit the maximum number of allowed connections, expand the pool and return that new connection
		if uint64(num_conns) < self.max_conns {
			self.lc.Debugf("workflow", "[%s] No available existing connections, attempting to expand the pool to size %d", self.Cls, num_conns+1)
			conn_id, conn, err := self.expand()
			if err != nil {
				return 0, nil, err
			}
			return conn_id, conn.Connection, nil
		} else {
			// we're maxed out on connections, the best we can do is wait for a while for a connection to free up (receive message on a channel maybe?), and then return an error if we reach the limit
			self.lc.Debugf("workflow", "[%s] Pool is at capacity (%d), and cannot grow. Waiting for existing connections to be released.", self.Cls)
			self.release_sub = make(chan uint64)
			conn_id := <-self.release_sub
			self.lc.Debugf("workflow", "[%s] Connection %d released, disbursing as requested.", self.Cls)
			return conn_id, self.connections[conn_id].Connection, nil
		}
	} else {
		return 0, nil, errors.New("Not usable, did you Open() the pool after creating it?")
	}
}

// Release the lock on the connection with the given id
func (self *TCPConnectionPool) Release(conn_id uint64, reusable bool) error {
	if self.Usable {
		self.connections_lock.Lock()
		defer self.connections_lock.Unlock()
		if !self.reuse_connections {
			self.lc.Debugf("workflow", "[%s] Connection keepalive is disabled, reaping connection with id %d", self.Cls, conn_id)
			// not attempting connection reuse, so just close it and remove the reference in the connections map
			err := self.connections[conn_id].Connection.Close()
			if err != nil {
				self.lc.Errorf("workflow", "[%s] Error closing existing connection %d, error: %s", self.Cls, conn_id, err.Error())
				delete(self.connections, conn_id)
				return err
			}
			delete(self.connections, conn_id)
		} else {
			// keepalive is enabled, and we're interested in the state of the connection.. if it has been closed by the remote host, we want to replace the connection with a new one so as to not reuse defunct connections.
			if !reusable {
				self.lc.Debugf("workflow", "[%s] Connection %d indicated to be unusable, reaping and replacing with a fresh one.", self.Cls, conn_id)
				err := self.connections[conn_id].Connection.Close()
				if err != nil {
					self.lc.Errorf("workflow", "[%s] Error closing existing connection %d, error: %s", self.Cls, conn_id, err.Error())
					return err
				}
				delete(self.connections, conn_id)
				tcp_conn, err := self.new_tcp_connection()
				if err != nil {
					return err
				}
				self.connections[conn_id] = tcp_conn
			} else {
				self.connections[conn_id].in_use = false
				self.lc.Debugf("workflow", "[%s] Connection %d indicated to be reusable, will not reap and replace.", self.Cls, conn_id)
			}
		}
		if self.release_sub != nil {
			// an Acquire() call is blocked, let it know we've released a connection for it
			self.lc.Debugf("workflow", "[%s] Notifying Acquire() caller of released connection %d.", self.Cls, conn_id)
			self.release_sub <- conn_id
			close(self.release_sub)
			self.release_sub = nil
		}
		return nil
	} else {
		return errors.New("Not usable, did you Open() the pool after creating it?")
	}
}

// Optionally pre-allocate the pool and begin reaping task.
func (self *TCPConnectionPool) Open() error {
	if self.Configured {
		// only pre-allocate connections if we're using persistent connections
		min_conns := self.min_conns
		for min_conns > 0 {
			tcp_conn, err := self.new_tcp_connection()
			if err != nil {
				return err
			}
			self.connections[min_conns] = tcp_conn
			min_conns--
		}
		//go self.reap_lru()
		self.Usable = true
		return nil
	}
	return errors.New("Not configured, use NewTCPConnectionPool")
}

// Shuts down the pool, closing all connections and stopping the reapr
func (self *TCPConnectionPool) Close() error {
	if self.Usable {
		//self.stop_autoreap <- true
		self.connections_lock.Lock()
		defer self.connections_lock.Unlock()
		self.lc.Infof("workflow", "[%s] Closing %d connection(s).", self.Cls, len(self.connections))
		for k, c := range self.connections {
			err := c.Connection.Close()
			if err != nil {
				self.lc.Errorf("workflow", "[%s] Error closing existing connection %d, error: %s", self.Cls, k, err.Error())
				return err
			}
		}
		self.lc.Infof("workflow", "[%s] %d connection(s) closed.", self.Cls, len(self.connections))
	}
	return nil
}

// adds a new connection to the pool
// *IMPORTANT* always Lock() the connection_lock before calling this, and Unlock() after
func (self *TCPConnectionPool) expand() (uint64, *TCPConnection, error) {
	tcp_conn, err := self.new_tcp_connection()
	if err != nil {
		return 0, nil, err
	}
	conn_id := uint64(len(self.connections))
	self.connections[conn_id] = tcp_conn
	return conn_id, tcp_conn, nil
}

// run as a goroutine, periodically remove old, unused connections from the pool
func (self *TCPConnectionPool) reap_lru() {
	for {
		select {
		case <-self.stop_autoreap:
			self.lc.Infof("workflow", "[%s] [autoreaper] Connection auto-reaper received stop signal, shutting down task.", self.Cls)
			return
		default:
			self.connections_lock.Lock()
			to_reap := []uint64{}
			num_conns := uint64(len(self.connections))
			for k, c := range self.connections {
				if c.in_use == false && time.Since(c.last_used).Nanoseconds() > self.max_lru_age_ns {
					// the max number we're allowed to reap is the total - the minimum number required in the pool
					if uint64(len(to_reap)) < (num_conns - self.min_conns) {
						to_reap = append(to_reap, k)
					}
				}
			}
			for _, conn_id := range to_reap {
				c := self.connections[conn_id]
				self.lc.Debugf("workflow", "[%s] [autoreaper] Reaping unused connection %d with age %d (maximum age %d) from pool.", self.Cls, conn_id, time.Since(c.last_used).Nanoseconds(), self.max_lru_age_ns)
				err := c.Connection.Close()
				if err != nil {
					self.lc.Errorf("workflow", "[%s] [autoreaper] Error closing existing connection %d, error: %s", self.Cls, conn_id, err.Error())
				}
				delete(self.connections, conn_id)
			}
			self.connections_lock.Unlock()
			self.lc.Debugf("workflow", "[%s] [autoreaper] Reaped %d old, unused connections, next pass in %s.", self.Cls, len(to_reap), self.autoreap_interval.String())
			time.Sleep(*self.autoreap_interval)
		}
	}
}

func (self *TCPConnectionPool) new_tcp_connection() (*TCPConnection, error) {
	conn, err := net.DialTCP(self.net_type, self.local_addr, self.remote_addr)
	if err != nil {
		self.lc.Errorf("workflow", "[%s] Error dialing connection, error: %s", self.Cls, err.Error())
		return nil, err
	}
	if self.keepalive {
		err := conn.SetKeepAlive(self.keepalive)
		if err != nil {
			self.lc.Errorf("workflow", "[%s] Error setting keepalive, error: %s", self.Cls, err.Error())
			return nil, err
		}
		if self.keepalive_period != nil {
			err = conn.SetKeepAlivePeriod(*self.keepalive_period)
			if err != nil {
				self.lc.Errorf("workflow", "[%s] Error setting connection keepalive period %s, error: %s", self.Cls, self.keepalive_period.String(), err.Error())
				return nil, err
			}
		}
	}
	if self.remote_addr != nil {
		self.lc.Debugf("workflow", "[%s] Connection created to remote address %s.", self.Cls, self.remote_addr)
	} else {
		self.lc.Debugf("workflow", "[%s] Connection created to local address %s.", self.Cls, self.local_addr)
	}
	tcp_conn := new(TCPConnection)
	tcp_conn.Connection = conn
	tcp_conn.last_used = time.Now()
	return tcp_conn, nil
}

// Acquire a connection reference and attempt to write the data. One failure, try several release/acquire cycles to get a good connections we can send data with, and then return that connection for later use (like with a read).
func (self *TCPConnectionPool) attemptWrite(query []byte) (uint64, *net.TCPConn, error) {
	var write_tries uint8 = 3
	var last_err error
	var conn_id uint64
	var conn *net.TCPConn
	for write_tries > 1 {
		conn_id, conn, last_err = self.Acquire()
		if last_err != nil {
			return 0, nil, last_err
		}
		conn.SetWriteDeadline(time.Now().Add(time.Duration(3) * time.Second))
		num_written, err := conn.Write(query)
		if err != nil {
			write_tries--
			err2 := self.Release(conn_id, false)
			if err2 != nil {
				return 0, nil, err2
			}
			last_err = err
			continue
		}
		qlen := len(query)
		if num_written != qlen {
			write_tries--
			// a bad deal; this means we couldn't write the entire query to ephemeris
			self.lc.Errorf("workflow", "[%s] Mismatch between the number of bytes in the provided query and number of bytes written to Ephemeris, num_query_bytes: %d, num_written: %d", self.Cls, qlen, num_written)
			err := self.Release(conn_id, true)
			if err != nil {
				return 0, nil, err
			}
			last_err = errors.New(fmt.Sprintf("the entire provided query was not sent!, query length: %d, sent: %d", qlen, num_written))
			continue
		} else {
			break
		}
	}
	// after several attempts on different connections, we failed to write the query
	if last_err != nil {
		return 0, nil, last_err
	}
	return conn_id, conn, nil
}

// Acquires a TCP connection, writes the provided byte slice, and buffers the entire JSON-encoded, UTF-8 response before unmarshaling it into the provided value. Do not use this function for queries that could return arbitrarily large responses, as you could blow up the process's memory. Use of this function assumes we can continue to use this connection in the future without rebuilding it - this is specific to how the Ephemeris TCP semantics are set up.
func (self *TCPConnectionPool) SendAndReceiveJSON(destination_ptr interface{}, query []byte) error {
	// newline signifies a complete query
	query = append(query, '\u000A')

	conn_id, conn, err := self.attemptWrite(query)
	if err != nil {
		return err
	}

	conn.SetReadDeadline(time.Now().Add(time.Duration(3) * time.Second))

	// we know this response is json, so we can use a decoder to handle the read bytes
	d := json.NewDecoder(conn)
	err = d.Decode(destination_ptr)
	if err != nil {
		err2 := self.Release(conn_id, false)
		if err2 != nil {
			return err2
		}
		return err
	}
	err = self.Release(conn_id, true)
	if err != nil {
		return err
	}
	return nil
}

// Acquires a TCP connection, writes the provided byte slice, and receives a JSON response in chunks. These chunks are piped directly to the provided destination. The destination must process each chunk in order for the next chunk to be read from the connection to Ephemeris. Use of this function assumes we can continue to use this connection in the future without rebuilding it - this is specific to how the Ephemeris TCP semantics are set up.
func (self *TCPConnectionPool) SendAndReceiveJSONPiped(destination net.Conn, query []byte) error {
	// newline signifies a complete query
	query = append(query, '\u000A')

	conn_id, conn, err := self.attemptWrite(query)
	if err != nil {
		return err
	}

	var num_open_objs uint64
	var num_closed_objs uint64
	var num_open_arrays uint64
	var num_closed_arrays uint64

	// These deadlines may be problematic for later calls to Read() and Write() for large responses.. if so, we can move this down into the loop or think of another way to handle it.
	conn.SetReadDeadline(time.Now().Add(time.Duration(3) * time.Second))
	destination.SetWriteDeadline(time.Now().Add(time.Duration(3) * time.Second))
	buf := make([]byte, self.buf_size)
	for {
		num_read, err := conn.Read(buf[0:])
		if err != nil {
			if err != nil {
				self.lc.Errorf("workflow", "[%s] Error reading from Ephemeris connection, error: %s", self.Cls, err.Error())
				err2 := self.Release(conn_id, true)
				if err2 != nil {
					return err2
				}
				return err
			}
		}
		if uint16(num_read) == self.buf_size {
			// a complete chunk was recieved, we know that there may be need to continue reading; we also know we can write the entire buffer without fear that we're sending data from a previous iteration.
			for _, b := range buf {
				switch b {
				case '\u007B':
					num_open_objs++
				case '\u007D':
					num_closed_objs++
				case '\u005B':
					num_open_arrays++
				case '\u005D':
					num_closed_arrays++
				}
			}
			num_written, err := destination.Write(buf)
			if err != nil {
				self.lc.Errorf("workflow", "[%s] Error writing Ephemeris response (full chunk) to destination connection, error: %s", self.Cls, err.Error())
				err2 := self.Release(conn_id, true)
				if err2 != nil {
					return err2
				}
				return err
			}
			if num_written != num_read {
				// a bad deal; this means some of the data we received wasn't sent to the destination
				self.lc.Errorf("workflow", "[%s] Mismatch between the number of bytes received from Ephemeris and the number of bytes written to the destination, num_received: %d, num_read: %d", self.Cls, num_read, num_written)
				err := self.Release(conn_id, true)
				if err != nil {
					return err
				}
				return errors.New(fmt.Sprintf("some data received was not sent!, recieved: %d, sent: %d", num_read, num_written))
			}
		} else {
			// only data from the beginning of the buffer up to the bytes read is valid; anything after that is either empty (small response) or invalid (data from a previous iteration)
			for _, b := range buf[0:num_read] {
				switch b {
				case '\u007B':
					num_open_objs++
				case '\u007D':
					num_closed_objs++
				case '\u005B':
					num_open_arrays++
				case '\u005D':
					num_closed_arrays++
				}
			}
			num_written, err := destination.Write(buf[0:num_read])
			if err != nil {
				self.lc.Errorf("workflow", "[%s] Error writing Ephemeris response (partial chunk) to destination connection, error: %s", self.Cls, err.Error())
				err2 := self.Release(conn_id, true)
				if err2 != nil {
					return err2
				}
				return err
			}
			if num_written != num_read {
				// a bad deal; this means some of the data we received wasn't sent to the destination
				self.lc.Errorf("workflow", "[%s] Mismatch between the number of bytes received from Ephemeris and the number of bytes written to the destination, num_received: %d, num_read: %d", self.Cls, num_read, num_written)
				err := self.Release(conn_id, true)
				if err != nil {
					return err
				}
				return errors.New(fmt.Sprintf("some data received was not sent!, recieved: %d, sent: %d", num_read, num_written))
			}
			// Now we decide if we're done yet... the critical assumption made here is that if we ever aren't able to fill up the self.buf_sizebuffer, there's nothing else going to be sent to us. Our final sanity check is if the final character in the buffer turns out to be a '}' or ']'... then we say this is the end of the data. Otherwise, we set a new read timeout and hope that ephemeris fills the buffer in time and error with an io read timeout if it doesn't.
			// } = 007D hex
			// ] = 005D hex
			last := buf[num_read-1]
			if last == '\u007D' || last == '\u005D' {
				self.lc.Debugf("workflow", "[%s] Received final byte sequence ']' or '}', checking object delimiter counts.", self.Cls)
				if num_open_arrays == num_closed_arrays && num_open_objs == num_closed_objs {
					self.lc.Debugf("workflow", "[%s] Delimiter counts match, terminating read loop.", self.Cls)
					break
				}
				self.lc.Warnf("workflow", "[%s] Mismatched array or object delimiters, setting deadline for receipt of next chunk.", self.Cls)
				conn.SetReadDeadline(time.Now().Add(time.Duration(3) * time.Second))
			} else {
				self.lc.Warnf("workflow", "[%s] Last byte received in partial chunk was not a ']' or '}', setting deadline for receipt of next chunk.", self.Cls)
				conn.SetReadDeadline(time.Now().Add(time.Duration(3) * time.Second))
			}
		}
	}
	err = self.Release(conn_id, true)
	if err != nil {
		return err
	}
	return nil
}

// Acquires a TCP connection, writes the provided byte slice, and receives a binary response in chunks. These chunks are piped directly to the provided destination. The destination must process each chunk in order for the next chunk to be read from the connection to Ephemeris. marking the acquired connection from the pool as unusable when finished - this is specific to how the Ephemeris TCP semantics are set up.
func (self *TCPConnectionPool) SendAndReceiveBinaryPiped(destination net.Conn, query []byte) error {
	// newline signifies a complete query
	query = append(query, '\u000A')

	conn_id, conn, err := self.attemptWrite(query)
	if err != nil {
		return err
	}

	conn.SetReadDeadline(time.Now().Add(time.Duration(3) * time.Second))
	// todo; read into a reusable buffer and write to destination, and then release with reusable = false

	err = self.Release(conn_id, true)
	if err != nil {
		return err
	}
	return nil
}

// Create a configured connection pool object. Use this instead of new() or a struct literal instantiation.
func NewTCPConnectionPool(lc *lctx.LoggingContext, reuse_connections bool, min_conns, max_conns uint64, net_type string, local_addr *net.TCPAddr, remote_addr *net.TCPAddr, keepalive bool, keepalive_period string, max_lru_age string, autoreap_interval string, buf_size uint16) (*TCPConnectionPool, error) {
	pool := new(TCPConnectionPool)
	pool.Cls = cls_tcpconnpool
	pool.lc = lc
	pool.buf_size = buf_size
	pool.connections = make(map[uint64]*TCPConnection)
	pool.stop = make(chan bool)
	pool.stop_autoreap = make(chan bool)
	autoreap_interval_dur, err := time.ParseDuration(autoreap_interval)
	if err != nil {
		return nil, err
	}
	pool.autoreap_interval = &autoreap_interval_dur
	pool.reuse_connections = reuse_connections
	pool.max_conns = max_conns
	pool.min_conns = min_conns
	dur, err := time.ParseDuration(max_lru_age)
	if err != nil {
		return nil, err
	}
	pool.max_lru_age_ns = dur.Nanoseconds()
	pool.net_type = net_type
	pool.local_addr = local_addr
	pool.remote_addr = remote_addr
	pool.keepalive = keepalive
	keepalive_period_dur, err := time.ParseDuration(keepalive_period)
	if err != nil {
		return nil, err
	}
	pool.keepalive_period = &keepalive_period_dur
	if !(max_conns > min_conns) {
		return nil, errors.New("max_conns must be greater than min_conns")
		lc.Errorf("workflow", "The pool requires that max_conns be greater than min_conns. Provided max=%d and provided min=%d", max_conns, min_conns)
	}
	if max_conns == 0 || min_conns == 0 {
		return nil, errors.New("")
		lc.Errorf("workflow", "At least 1 connection is required to create a pool, provided max_conns=%d and min_conns=%d", max_conns, min_conns)
	}
	pool.Configured = true
	return pool, nil
}
