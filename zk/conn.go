// Package zk is a native Go client library for the ZooKeeper orchestration service.
package zk

/*
TODO:
* make sure a ping response comes back in a reasonable time

Possible watcher events:
* Event{Type: EventNotWatching, State: StateDisconnected, Path: path, Err: err}
*/

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrNoServer indicates that an operation cannot be completed
// because attempts to connect to all servers in the list failed.
var ErrNoServer = errors.New("zk: could not connect to a server")

// ErrInvalidPath indicates that an operation was being attempted on
// an invalid path. (e.g. empty path)
var ErrInvalidPath = errors.New("zk: invalid path")

// DefaultLogger uses the stdlib log package for logging.
var DefaultLogger Logger = defaultLogger{}

const (
	bufferSize      = 1536 * 1024
	eventChanSize   = 6
	sendChanSize    = 16
	protectedPrefix = "_c_"
)

type watchType int

const (
	watchTypeData = iota
	watchTypeExist
	watchTypeChild
)

// watcher 的 key ，由 path 和 type 唯一确定
type watchPathType struct {
	path  string
	wType watchType
}

type Dialer func(network, address string, timeout time.Duration) (net.Conn, error)

// Logger is an interface that can be implemented to provide custom log output.
type Logger interface {
	Printf(string, ...interface{})
}

type authCreds struct {
	scheme string
	auth   []byte
}


// Zxid: 为 zookeeper 维持的 id
// xid: 为本地的 id
//
//
//
type Conn struct {

	lastZxid         int64

	sessionID        int64
	state            State 		// must be 32-bit aligned




	xid              uint32


	sessionTimeoutMs int32 		// session timeout in milliseconds

	passwd           []byte

	dialer         Dialer
	hostProvider   HostProvider

	serverMu       sync.Mutex 	// protects server
	server         string     	// remember the address/port of the current server


	conn           net.Conn


	eventChan      chan Event
	eventCallback  EventCallback // may be nil

	shouldQuit     chan struct{}

	pingInterval   time.Duration
	recvTimeout    time.Duration
	connectTimeout time.Duration

	maxBufferSize  int

	creds   []authCreds
	credsMu sync.Mutex // protects server


	// 待发送的请求
	sendChan     chan *request

	// 等待发送的请求缓存
	requests     map[int32]*request // Xid -> pending request
	requestsLock sync.Mutex

	// 正在处理的 watchers
	watchers     map[watchPathType][]chan Event
	watchersLock sync.Mutex


	closeChan    chan struct{} // channel to tell send loop stop

	// Debug (used by unit tests)
	reconnectLatch   chan struct{}


	setWatchLimit    int
	setWatchCallback func([]*setWatchesRequest)


	// Debug (for recurring re-auth hang)
	debugCloseRecvLoop bool
	debugReauthDone    chan struct{}


	logger  Logger
	logInfo bool // true if information messages are logged; false if only errors are logged


	buf []byte
}





// connOption represents a connection option.
type connOption func(c *Conn)



type request struct {

	xid        int32
	opcode     int32
	pkt        interface{}
	recvStruct interface{}
	recvChan   chan response

	// Because sending and receiving happen in separate go routines,
	// there's a possible race condition when creating watches from outside the read loop.
	//
	// We must ensure that a watcher gets added to the list synchronously
	// with the response from the server on any request that creates a watch.
	// In order to not hard code the watch logic for each opcode in the recv
	// loop the caller can use recvFunc to insert some synchronously code after a response.
	recvFunc func(*request, *responseHeader, error)
}


type response struct {
	zxid int64
	err  error
}



type Event struct {
	// 事件类型
	Type   EventType
	// 状态
	State  State
	// 路径
	Path   string // For non-session events, the path of the watched node.
	// 错误信息
	Err    error
	// 服务信息
	Server string // For connection events
}


// HostProvider is used to represent a set of hosts a ZooKeeper client should connect to.
// HostProvider 被用于表示 zk client 应该连接到的一组 zk servers 主机地址。
//
// It is an analog of the Java equivalent:
// http://svn.apache.org/viewvc/zookeeper/trunk/src/java/main/org/apache/zookeeper/client/HostProvider.java?view=markup
type HostProvider interface {

	// Init is called first, with the servers specified in the connection string.
	// 首先调用 Init 函数，使用 `服务字符串(数组)` 作为参数。
	Init(servers []string) error

	// Len returns the number of servers.
	Len() int

	// Next returns the next server to connect to.
	// Next 返回下一个要连接的服务器地址。
	//
	// retryStart will be true if we've looped through all known servers without Connected() being called.
	// 如果我们遍历完所有已知服务器而未调用 Connected() 标识任何节点可用，则 retryStart 将为 true 。
	Next() (server string, retryStart bool)

	// Notify the HostProvider of a successful connection.
	// 通知 HostProvider 一个成功建立的连接。
	Connected()
}


// ConnectWithDialer establishes a new connection to a pool of zookeeper servers using a custom Dialer.
//
//
// See Connect for further information about session timeout.
//
// This method is deprecated and provided for compatibility: use the WithDialer option instead.
func ConnectWithDialer(servers []string, sessionTimeout time.Duration, dialer Dialer) (*Conn, <-chan Event, error) {


	return Connect(servers, sessionTimeout, WithDialer(dialer))
}





// Connect establishes a new connection to a pool of zookeeper servers.
//
//
//
// The provided session timeout sets the amount of time for which
// a session is considered valid after losing connection to a server.
//
// Within the session timeout it's possible to reestablish a connection to a different server and keep the same session.
// This is means any ephemeral nodes and watches are maintained.


// Connect 大体逻辑如下：

// 1. 混淆 servers 顺序，避免出现请求热点
// 2. DNSHostProvider 实现 DNS 查询， 把 host 转换成 address(ip 地址)
// 3. 开启 goroutine 循环
//
//	a. 建立网络连接
//		(1) 使用轮询的方式，选取ip地址
//		(2) 如果建联成功， 会打印 2018/03/03 11:26:53 Connected to 127.0.0.1:2181 类似的提示
//
//	b. 调用 authenticate， 同 zk-server 协调生成 SessionID ，日志 "2018/03/03 11:26:53 Authenticated: id=99617148944318466，timeout=4000" 中
//     的 id 就是 sessionId，timeout 是服务端返回的 session 超时时间。根据服务端返回的 timeout 数据设置 pingInterval。
//
//	c. 开启两个 goroutine， 一个 sendLoop 循环, 一个 recvLoop 循环
//
//	d. sendLoop 循环， 如果有 send 数据则发送数据，否则，根据 pingInterval 时间发送心跳包。
//
//	e. recvLoop 循环接收网络数据， 前4个字节是包的长度，根据长度创建 buf 大小，存储收到的数据。
// 	   返回的头文件中有 Xid 字段，这个是请求传过去的，然后原样返回。
// 	   所有的 request 会放到一个 map 中，Xid 就是其中的 key 。
// 	   如果接受到的数据是 watcher 数据，Xid 为 固定值-1，recvLoop 会根据 path 和 watcher 类型查找全局 map 中对应的数据，把 event事件放到对应的channel中。
// 	   监听事件只生效一次，然后会关闭 channel ，删除 wacher 对象。


func Connect(servers []string, sessionTimeout time.Duration, options ...connOption) (*Conn, <-chan Event, error) {

	// 参数检查
	if len(servers) == 0 {
		return nil, nil, errors.New("zk: server list must not be empty")
	}

	// 参数校正，确保 addr = ip + ":" + port
	srvs := make([]string, len(servers))
	for i, addr := range servers {
		if strings.Contains(addr, ":") {
			srvs[i] = addr
		} else {
			srvs[i] = addr + ":" + strconv.Itoa(DefaultPort)
		}
	}

	// 混淆 srvs 顺序，避免出现请求热点
	stringShuffle(srvs) 	// Randomize the order of the servers to avoid creating hotspots

	// 定义事件管道，该管道会赋值给 conn.eventChan 以接收 zk-server 返回的 event，这个管道作为 `connect()` 的返回值，
	// 使得 zk-server 返回的 event 能够直接送达给调用者。
	ec := make(chan Event, eventChanSize)

	conn := &Conn{
		dialer:         net.DialTimeout,					// Dial connects to the address on the named network.
		hostProvider:   &DNSHostProvider{},					//
		conn:           nil,
		state:          StateDisconnected,
		eventChan:      ec,
		shouldQuit:     make(chan struct{}),
		connectTimeout: 1 * time.Second,
		sendChan:       make(chan *request, sendChanSize),
		requests:       make(map[int32]*request),
		watchers:       make(map[watchPathType][]chan Event),
		passwd:         emptyPassword,
		logger:         DefaultLogger,
		logInfo:        true, // default is true for backwards compatability
		buf:            make([]byte, bufferSize),
	}

	// Set provided options.
	for _, option := range options {
		option(conn)
	}


	// 初始化 hostProvider，它被用来维护 zk-client 应该连接到的一组 zk servers 主机地址。
	if err := conn.hostProvider.Init(srvs); err != nil {
		return nil, nil, err
	}

	// 设置 `网络连接超时` 为 `会话超时`
	conn.setTimeouts(int32(sessionTimeout / time.Millisecond))


	// 启动主 goroutine
	go func() {

		// 阻塞式调用
		conn.loop()

		// 将已经发送到 zk 的请求处理掉，直接报错: ErrClosing(Close之后, Conn就完事，程序也该关闭了)
		conn.flushRequests(ErrClosing)

		// 所有的 Watcher 也会收到 ErrClosing 消息
		conn.invalidateWatches(ErrClosing)

		// 发送关闭信号
		close(conn.eventChan)
	}()


	// 这里把 ec 返回给调用者，使调用者能够接收到 zk-server 返回的 zk-event。
	return conn, ec, nil
}





// WithDialer returns a connection option specifying a non-default Dialer.
func WithDialer(dialer Dialer) connOption {
	return func(c *Conn) {
		c.dialer = dialer
	}
}

// WithHostProvider returns a connection option specifying a non-default HostProvider.
func WithHostProvider(hostProvider HostProvider) connOption {
	return func(c *Conn) {
		c.hostProvider = hostProvider
	}
}

// WithLogger returns a connection option specifying a non-default Logger
func WithLogger(logger Logger) connOption {
	return func(c *Conn) {
		c.logger = logger
	}
}

// WithLogInfo returns a connection option specifying whether or not information messages
// shoud be logged.
func WithLogInfo(logInfo bool) connOption {
	return func(c *Conn) {
		c.logInfo = logInfo
	}
}

// EventCallback is a function that is called when an Event occurs.
type EventCallback func(Event)

// WithEventCallback returns a connection option that specifies an event
// callback.
// The callback must not block - doing so would delay the ZK go routines.
func WithEventCallback(cb EventCallback) connOption {
	return func(c *Conn) {
		c.eventCallback = cb
	}
}

// WithMaxBufferSize sets the maximum buffer size used to read and decode
// packets received from the Zookeeper server. The standard Zookeeper client for
// Java defaults to a limit of 1mb. For backwards compatibility, this Go client
// defaults to unbounded unless overridden via this option. A value that is zero
// or negative indicates that no limit is enforced.
//
// This is meant to prevent resource exhaustion in the face of potentially
// malicious data in ZK. It should generally match the server setting (which
// also defaults ot 1mb) so that clients and servers agree on the limits for
// things like the size of data in an individual znode and the total size of a
// transaction.
//
// For production systems, this should be set to a reasonable value (ideally
// that matches the server configuration). For ops tooling, it is handy to use a
// much larger limit, in order to do things like clean-up problematic state in
// the ZK tree. For example, if a single znode has a huge number of children, it
// is possible for the response to a "list children" operation to exceed this
// buffer size and cause errors in clients. The only way to subsequently clean
// up the tree (by removing superfluous children) is to use a client configured
// with a larger buffer size that can successfully query for all of the child
// names and then remove them. (Note there are other tools that can list all of
// the child names without an increased buffer size in the client, but they work
// by inspecting the servers' transaction logs to enumerate children instead of
// sending an online request to a server.
func WithMaxBufferSize(maxBufferSize int) connOption {
	return func(c *Conn) {
		c.maxBufferSize = maxBufferSize
	}
}

// WithMaxConnBufferSize sets maximum buffer size used to send and encode
// packets to Zookeeper server. The standard Zookeepeer client for java defaults
// to a limit of 1mb. This option should be used for non-standard server setup
// where znode is bigger than default 1mb.
func WithMaxConnBufferSize(maxBufferSize int) connOption {
	return func(c *Conn) {
		c.buf = make([]byte, maxBufferSize)
	}
}



func (c *Conn) Close() {
	close(c.shouldQuit)

	select {
	case <-c.queueRequest(opClose, &closeRequest{}, &closeResponse{}, nil):
	case <-time.After(time.Second):
	}
}

// State returns the current state of the connection.
func (c *Conn) State() State {
	return State(atomic.LoadInt32((*int32)(&c.state)))
}

// SessionID returns the current session id of the connection.
func (c *Conn) SessionID() int64 {
	return atomic.LoadInt64(&c.sessionID)
}

// SetLogger sets the logger to be used for printing errors.
// Logger is an interface provided by this package.
func (c *Conn) SetLogger(l Logger) {
	c.logger = l
}

func (c *Conn) setTimeouts(sessionTimeoutMs int32) {
	c.sessionTimeoutMs = sessionTimeoutMs
	sessionTimeout := time.Duration(sessionTimeoutMs) * time.Millisecond
	c.recvTimeout = sessionTimeout * 2 / 3
	c.pingInterval = c.recvTimeout / 2
}

func (c *Conn) setState(state State) {
	atomic.StoreInt32((*int32)(&c.state), int32(state))
	c.sendEvent(Event{Type: EventSession, State: state, Server: c.Server()})
}

func (c *Conn) sendEvent(evt Event) {

	// 如果指定了事件回调，在发送 event 之前先调用事件回调函数，这个回调函数需要是非阻塞的。
	if c.eventCallback != nil {
		c.eventCallback(evt)
	}

	// 发送 evt 到事件管道，如果发送阻塞则 ignore
	select {
	case c.eventChan <- evt:
	default:
		// panic("zk: event channel full - it must be monitored and never allowed to be full")
	}
}




func (c *Conn) connect() error {


	var retryStart bool
	for {

		// Next() 返回下一个要连接的服务器 Server 。
		c.serverMu.Lock()
		c.server, retryStart = c.hostProvider.Next()
		c.serverMu.Unlock()

		// 设置状态为 `连接中`
		c.setState(StateConnecting)

		// 如果 retryStart 为 true ，
		if retryStart {

			//
			c.flushUnsentRequests(ErrNoServer)

			select {
			// 超时 1 秒
			case <-time.After(time.Second):
				// pass
			//
			case <-c.shouldQuit:
				// 设置状态为 `断连`
				c.setState(StateDisconnected)
				//
				c.flushUnsentRequests(ErrClosing)
				//
				return ErrClosing
			}
		}

		// 尝试建立网络连接
		//（1）若成功，则设置 c.conn 且设置状态为 `已连接`，打印日志并返回
		//（2）若失败，则打印错误日志，然后继续 for 循环尝试下一次连接
		zkConn, err := c.dialer("tcp", c.Server(), c.connectTimeout)
		if err == nil {
			c.conn = zkConn
			c.setState(StateConnected)
			if c.logInfo {
				// 打印 Info 日志
				c.logger.Printf("Connected to %s", c.Server())
			}
			return nil
		}

		// 打印 Error 日志
		c.logger.Printf("Failed to connect to %s: %+v", c.Server(), err)
	}


}

func (c *Conn) resendZkAuth(reauthReadyChan chan struct{}) {


	shouldCancel := func() bool {
		select {
		case <-c.shouldQuit:
			return true
		case <-c.closeChan:
			return true
		default:
			return false
		}
	}


	c.credsMu.Lock()
	defer c.credsMu.Unlock()

	defer close(reauthReadyChan)

	if c.logInfo {
		c.logger.Printf("re-submitting `%d` credentials after reconnect", len(c.creds))
	}



	for _, cred := range c.creds {
		if shouldCancel() {
			return
		}
		resChan, err := c.sendRequest(
			opSetAuth,
			&setAuthRequest{Type: 0,
				Scheme: cred.scheme,
				Auth:   cred.auth,
			},
			&setAuthResponse{},
			nil)

		if err != nil {
			c.logger.Printf("call to sendRequest failed during credential resubmit: %s", err)
			// FIXME(prozlach): lets ignore errors for now
			continue
		}

		var res response
		select {
		case res = <-resChan:
		case <-c.closeChan:
			c.logger.Printf("recv closed, cancel re-submitting credentials")
			return
		case <-c.shouldQuit:
			c.logger.Printf("should quit, cancel re-submitting credentials")
			return
		}
		if res.err != nil {
			c.logger.Printf("credential re-submit failed: %s", res.err)
			// FIXME(prozlach): lets ignore errors for now
			continue
		}
	}


}

func (c *Conn) sendRequest(
	opcode int32,
	req interface{},
	res interface{},
	recvFunc func(*request, *responseHeader, error),
) (
	<-chan response,
	error,
) {


	//
	rq := &request{
		xid:        c.nextXid(),
		opcode:     opcode,
		pkt:        req,
		recvStruct: res,
		recvChan:   make(chan response, 1),
		recvFunc:   recvFunc,
	}


	if err := c.sendData(rq); err != nil {
		return nil, err
	}

	return rq.recvChan, nil
}



func (c *Conn) loop() {

	// 阻塞式调用
	for {

		// 1. 建立同 zk-server 的连接
		if err := c.connect(); err != nil {
			// c.Close() was called
			return
		}

		// 2. 发送 connectRequest{} 给 zk-server ，接收 connectResponse{} 响应，并根据响应更新 c *Conn 的成员变量。
		err := c.authenticate()

		switch {
		// 会话过期，需要清理过期会话的 watchers
		case err == ErrSessionExpired:
			c.logger.Printf("authentication failed: %s", err)
			c.invalidateWatches(err)
		// 对于其它错误，则关闭 conn
		case err != nil && c.conn != nil:
			c.logger.Printf("authentication failed: %s", err)
			c.conn.Close()

		// 连接正常
		case err == nil:

			// 打印日志
			if c.logInfo {
				c.logger.Printf("authenticated: id=%d, timeout=%d", c.SessionID(), c.sessionTimeoutMs)
			}

			// 标识连接成功
			c.hostProvider.Connected()        // mark success


			c.closeChan = make(chan struct{}) // channel to tell send loop stop
			reauthChan := make(chan struct{}) // channel to tell send loop that authdata has been resubmitted


			// 启动两个 loop ，分别为 c.sendLoop() 和 c.recvLoop(c.conn)

			var wg sync.WaitGroup
			wg.Add(2)

			// 开启 sendLoop
			go func() {

				// 先阻塞在管道上，等待启动信号
				<-reauthChan

				if c.debugCloseRecvLoop {
					close(c.debugReauthDone)
				}

				err := c.sendLoop()
				if err != nil || c.logInfo {
					c.logger.Printf("send loop terminated: err=%v", err)
				}

				c.conn.Close() // causes recv loop to EOF/exit
				wg.Done()

			}()


			// 开启 recvLoop
			go func() {

				var err error
				if c.debugCloseRecvLoop {
					err = errors.New("DEBUG: close recv loop")
				} else {
					err = c.recvLoop(c.conn)
				}


				if err != io.EOF || c.logInfo {
					c.logger.Printf("recv loop terminated: err=%v", err)
				}

				if err == nil {
					panic("zk: recvLoop should never return nil error")
				}


				close(c.closeChan) // tell send loop to exit
				wg.Done()
			}()


			c.resendZkAuth(reauthChan)

			c.sendSetWatches()
			wg.Wait()
		}




		// 4. 设置连接状态为 `断连`
		c.setState(StateDisconnected)

		// 5. 如果收到 shouldQuit, 则准备退出，否则继续 for 循环处理
		select {
		case <-c.shouldQuit:
			c.flushRequests(ErrClosing)
			return
		default:
		}

		if err != ErrSessionExpired {
			err = ErrConnectionClosed
		}
		c.flushRequests(err)

		if c.reconnectLatch != nil {
			select {
			case <-c.shouldQuit:
				return
			case <-c.reconnectLatch:
			}
		}
	}
}




// 处理未发送的 Requests
//
// 和 flushRequests 的区别在于: request 来自于 c.sendChan 的 buffer

func (c *Conn) flushUnsentRequests(err error) {
	for {
		select {
		default:
			return
		case req := <-c.sendChan:
			req.recvChan <- response{-1, err}
		}
	}
}

// Send error to all pending requests and clear request map
func (c *Conn) flushRequests(err error) {
	c.requestsLock.Lock()

	// c.requests 存储了所有待发送的请求，这里遍历 c.requests 中每个请求，
	// 直接为每个 request 返回一个错误响应 response{-1, err}
	for _, req := range c.requests {
		req.recvChan <- response{-1, err}
	}

	// 清空 c.requests
	c.requests = make(map[int32]*request)

	c.requestsLock.Unlock()
}

// Send error to all watchers and clear watchers map
func (c *Conn) invalidateWatches(err error) {
	c.watchersLock.Lock()
	defer c.watchersLock.Unlock()
	// 如果存在 watchers 就逐个处理
	if len(c.watchers) >= 0 {
		// 遍历 c.watchers
		for pathType, watchers := range c.watchers {

			// 构造 StateDisconnected 事件
			ev := Event{
				Type: EventNotWatching,
				State: StateDisconnected,
				Path: pathType.path,
				Err: err,
			}

			// watchers 是一组事件管道，这里以 StateDisconnected 事件通知管道接收者，然后关闭这个管道。
			for _, ch := range watchers {
				ch <- ev	// 通知管道接收者
				close(ch)	// 关闭管道
			}

		}

		// 清空所有 c.watchers
		c.watchers = make(map[watchPathType][]chan Event)
	}
}

func (c *Conn) sendSetWatches() {
	c.watchersLock.Lock()
	defer c.watchersLock.Unlock()



	if len(c.watchers) == 0 {
		return
	}


	// NB: A ZK server, by default, rejects packets >1mb.
	//
	// So, if we have too many watches to reset,
	// we need to break this up into multiple packets to avoid hitting that limit.
	//
	// Mirroring the Java client behavior:
	//
	// we are conservative in that we limit requests to 128kb (since server limit is actually
	// configurable and could conceivably be configured smaller than default of 1mb).
	//
	limit := 128 * 1024
	if c.setWatchLimit > 0 {
		limit = c.setWatchLimit
	}



	var reqs []*setWatchesRequest
	var req *setWatchesRequest
	var sizeSoFar int

	n := 0
	for pathType, watchers := range c.watchers {

		if len(watchers) == 0 {
			continue
		}

		addlLen := 4 + len(pathType.path)

		if req == nil || sizeSoFar+addlLen > limit {

			if req != nil {
				// add to set of requests that we'll send
				reqs = append(reqs, req)
			}

			sizeSoFar = 28 // fixed overhead of a set-watches packet

			req = &setWatchesRequest{
				RelativeZxid: c.lastZxid,
				DataWatches:  make([]string, 0),
				ExistWatches: make([]string, 0),
				ChildWatches: make([]string, 0),
			}

		}


		sizeSoFar += addlLen
		switch pathType.wType {
		case watchTypeData:
			req.DataWatches  = append(req.DataWatches,  pathType.path)
		case watchTypeExist:
			req.ExistWatches = append(req.ExistWatches, pathType.path)
		case watchTypeChild:
			req.ChildWatches = append(req.ChildWatches, pathType.path)
		}
		n++
	}


	if n == 0 {
		return
	}


	if req != nil { // don't forget any trailing packet we were building
		reqs = append(reqs, req)
	}


	if c.setWatchCallback != nil {
		c.setWatchCallback(reqs)
	}


	go func() {
		res := &setWatchesResponse{}

		// TODO: Pipeline these so queue all of them up before waiting on any response.
		// That will require some investigation to make sure there aren't failure modes
		// where a blocking write to the channel of requests could hang indefinitely and
		// cause this goroutine to leak...
		for _, req := range reqs {
			_, err := c.request(opSetWatches, req, res, nil)
			if err != nil {
				c.logger.Printf("Failed to set previous watches: %s", err.Error())
				break
			}
		}
	}()
}


// 发送 connectRequest{} 给 zk-server ，接收 connectResponse{} 响应，并根据响应更新 c *Conn 的成员变量。

func (c *Conn) authenticate() error {

	// packet := len(body) + body
	buf := make([]byte, 256)

	///// 写

	// Encode and send a connect request.
	// 序列化 connectRequest{} 对象
	n, err := encodePacket(buf[4:], &connectRequest{
		ProtocolVersion: protocolVersion,		// 协议版本号
		LastZxidSeen:    c.lastZxid,			//
		TimeOut:         c.sessionTimeoutMs, 	// 设置 `会话超时`
		SessionID:       c.SessionID(), 		// 填入旧的 `会话ID` ，以维持状态的统一
		Passwd:          c.passwd,				// 填入连接 `密码`
	})

	if err != nil {
		return err
	}

	// packet := len(body) + body
	binary.BigEndian.PutUint32(buf[:4], uint32(n))

	// 设置写超时
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.recvTimeout * 10)); err != nil {
		return err
	}
	// 执行写操作
	_, err = c.conn.Write(buf[:n+4])
	if err != nil {
		return err
	}
	// 重置写超时
	if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
		return err
	}


	///// 读

	// Receive and decode a connect response.
	// 设置读超时
	if err := c.conn.SetReadDeadline(time.Now().Add(c.recvTimeout * 10)); err != nil {
		return err
	}
	// 执行读操作，先读四个字节的 len(body)
	_, err = io.ReadFull(c.conn, buf[:4])
	if err != nil {
		return err
	}
	// 重置读超时
	if err := c.conn.SetReadDeadline(time.Time{}); err != nil {
		return err
	}

	// 解析 len(body)，检查 buf 是够足够，不够则扩容
	blen := int(binary.BigEndian.Uint32(buf[:4]))
	if cap(buf) < blen {
		buf = make([]byte, blen)
	}

	// 执行读操作，读取 body
	_, err = io.ReadFull(c.conn, buf[:blen])
	if err != nil {
		return err
	}

	// 反序列化为 connectResponse{} 对象
	r := connectResponse{}
	_, err = decodePacket(buf[:blen], &r)
	if err != nil {
		return err
	}

	// 若 session 过期，则重置 c 的成员变量，同时报错返回
	if r.SessionID == 0 {
		atomic.StoreInt64(&c.sessionID, int64(0))
		c.passwd = emptyPassword
		c.lastZxid = 0
		c.setState(StateExpired)
		return ErrSessionExpired
	}


	// 若 session 未过期，则更新 c 的成员变量：`sessionID`, `timeout`, `passwd`, `state`
	atomic.StoreInt64(&c.sessionID, r.SessionID)
	c.setTimeouts(r.TimeOut)
	c.passwd = r.Passwd
	c.setState(StateHasSession)

	return nil
}


// 1. 把 req 序列化为 reqPacket
// 2. 把 req 暂存在 c.requests[req.xid] 缓存中
// 3. 发送 reqPacket 到 zk-server
func (c *Conn) sendData(req *request) error {

	// packet := len(body) + body
	//
	// body := header + pkt



	// 构造 header
	header := &requestHeader{
		Xid: req.xid,
		Opcode: req.opcode,
	}

	// 写入 header 到 buf 中，c.buf[4...12] <= header(8B)
	n, err := encodePacket(c.buf[4:], header)
	if err != nil {
		req.recvChan <- response{-1, err}
		return nil
	}

	// 写入 pkt 到 buf 中，c.buf[12...] <= req.pkt
	n2, err := encodePacket(c.buf[4+n:], req.pkt)
	if err != nil {
		req.recvChan <- response{-1, err}
		return nil
	}

	// len(body) = n + n2
	n += n2

	// 写入 len(body) 到 buf 首部 4B 中，至此 buf[0, n+4] 中包含完整的请求包
	binary.BigEndian.PutUint32(c.buf[:4], uint32(n))


	// 把请求存放在待发送的请求缓存中
	c.requestsLock.Lock()
	select {
	case <-c.closeChan:
		req.recvChan <- response{-1, ErrConnectionClosed}
		c.requestsLock.Unlock()
		return ErrConnectionClosed
	default:
	}
	c.requests[req.xid] = req // 把请求存放在 pending 缓存中
	c.requestsLock.Unlock()


	// 设置写超时
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.recvTimeout)); err != nil {
		return err
	}
	// 执行写操作
	_, err = c.conn.Write(c.buf[:n+4])
	// 如果出错则直接写回错误信息到 req.recvChan 中，并关闭 conn
	if err != nil {
		req.recvChan <- response{-1, err}
		c.conn.Close()
		return err
	}
	// 重置写超时
	if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
		return err
	}

	return nil
}

func (c *Conn) sendLoop() error {
	pingTicker := time.NewTicker(c.pingInterval)
	defer pingTicker.Stop()

	for {
		select {

		//读取待发送的请求，并发送到 zk-server
		case req := <-c.sendChan:
			if err := c.sendData(req); err != nil {
				return err
			}

		// 向 zk-server 定时发送心跳包
		case <-pingTicker.C:
			// 构造心跳包
			n, err := encodePacket(c.buf[4:], &requestHeader{Xid: -2, Opcode: opPing})
			if err != nil {
				panic("zk: opPing should never fail to serialize")
			}
			binary.BigEndian.PutUint32(c.buf[:4], uint32(n))

			// 设置写超时
			if err := c.conn.SetWriteDeadline(time.Now().Add(c.recvTimeout)); err != nil {
				return err
			}
			// 执行写操作
			_, err = c.conn.Write(c.buf[:n+4])
			if err != nil {
				c.conn.Close()
				return err
			}
			// 重置写超时
			if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
				return err
			}

		// 退出写循环
		case <-c.closeChan:
			return nil
		}
	}
}

func (c *Conn) recvLoop(conn net.Conn) error {


	sz := bufferSize


	if c.maxBufferSize > 0 && sz > c.maxBufferSize {
		sz = c.maxBufferSize
	}


	buf := make([]byte, sz)

	for {

		// 设置读超时
		if err := conn.SetReadDeadline(time.Now().Add(c.recvTimeout)); err != nil {
			c.logger.Printf("failed to set connection deadline: %v", err)
		}

		// 读取 len(body)
		_, err := io.ReadFull(conn, buf[:4])
		if err != nil {
			return fmt.Errorf("failed to read from connection: %v", err)
		}

		// 解析 len(body)
		blen := int(binary.BigEndian.Uint32(buf[:4]))

		// 扩容 buf ，使之足以容纳待接收的 body 部分
		if cap(buf) < blen {
			if c.maxBufferSize > 0 && blen > c.maxBufferSize {
				return fmt.Errorf("received packet from server with length %d, which exceeds max buffer size %d", blen, c.maxBufferSize)
			}
			buf = make([]byte, blen)
		}

		// 读取 body，body = responseHeader + responseData
		_, err = io.ReadFull(conn, buf[:blen])
		if err != nil {
			return err
		}

		// 重置读超时
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			return err
		}

		// 先反序列化 responseHeader(16B)
		res := responseHeader{}
		_, err = decodePacket(buf[:16], &res)
		if err != nil {
			return err
		}

		// 在反序列化 responseData（如果有的话）....

		// Xid 为 -1 时，意味着 zk-server 返回 watcherEvent，需要根据 watcherEvent 类型触发对应的 watcher 。
		if res.Xid == -1 {

			// 反序列化 watcherEvent{}
			res := &watcherEvent{}
			_, err := decodePacket(buf[16:blen], res)
			if err != nil {
				return err
			}

			// 结构体转换 watcherEvent{} => Event{}
			ev := Event{
				Type:  res.Type,
				State: res.State,
				Path:  res.Path,
				Err:   nil,
			}

			// 发送事件 ev 到管道 c.eventChan 中，该管道会返回给 `connect()` 函数的调用者，使其能够接收到所有的这些事件。
			c.sendEvent(ev)


			// 事件类型转换：根据 zk-server 返回的 watcherEvent.Type 确定需要触发的 watchTypes 类型的 watchers
			wTypes := make([]watchType, 0, 2)
			switch res.Type {
			case EventNodeCreated:
				wTypes = append(wTypes, watchTypeExist)
			case EventNodeDeleted, EventNodeDataChanged:
				wTypes = append(wTypes, watchTypeExist, watchTypeData, watchTypeChild)
			case EventNodeChildrenChanged:
				wTypes = append(wTypes, watchTypeChild)
			}

			// 根据不同的 watchTypes 将事件推送到对应的 watchers 上
			c.watchersLock.Lock()
			for _, t := range wTypes {
				// 先判断是否存在 path + wtype 上的 watcher：若存在，则逐个触发，并删除该 wacher ；否则，continue 。
				wpt := watchPathType{res.Path, t}
				if watchers, ok := c.watchers[wpt]; ok {
					for _, ch := range watchers {
						ch <- ev
						close(ch)
					}
					delete(c.watchers, wpt)
				}
			}
			c.watchersLock.Unlock()


		} else if res.Xid == -2 {

			// Ping response. Ignore.

		} else if res.Xid < 0 {

			c.logger.Printf("Xid < 0 (%d) but not ping or watcher event", res.Xid)

		} else {


			if res.Zxid > 0 {
				c.lastZxid = res.Zxid
			}


			c.requestsLock.Lock()
			req, ok := c.requests[res.Xid]
			if ok {
				delete(c.requests, res.Xid)
			}
			c.requestsLock.Unlock()

			if !ok {
				c.logger.Printf("Response for unknown request with xid %d", res.Xid)
			} else {
				if res.Err != 0 {
					err = res.Err.toError()
				} else {
					_, err = decodePacket(buf[16:blen], req.recvStruct)
				}


				if req.recvFunc != nil {
					req.recvFunc(req, &res, err)
				}

				req.recvChan <- response{res.Zxid, err}
				if req.opcode == opClose {
					return io.EOF
				}
			}
		}



	}
}


func (c *Conn) nextXid() int32 {
	// 掩码 0x 7f ff ff ff 确保了 uint32 + 1 不会溢出 int32 取值范围。
	return int32(atomic.AddUint32(&c.xid, 1) & 0x7fffffff)
}

func (c *Conn) addWatcher(path string, watchType watchType) <-chan Event {
	c.watchersLock.Lock()
	defer c.watchersLock.Unlock()

	// 创建管道 ch ，它会作为 `事件通知管道` 返回给调用者，当事件发生时，会通过此管道把 event 发送给调用者。
	ch := make(chan Event, 1)

	// 构造 watcher 的 key ，是由 path 和 type 唯一确定的
	wpt := watchPathType{path, watchType}

	// 把管道 ch 绑定到 wpt 上，当 wpt 类型事件发生时，会遍历该类型所有绑定的管道，逐个将 ev 发送给它们。
	c.watchers[wpt] = append(c.watchers[wpt], ch)
	return ch
}


func (c *Conn) queueRequest(opcode int32, req interface{}, res interface{}, recvFunc func(*request, *responseHeader, error)) <-chan response {
	// 1. 构造请求
	rq := &request{
		xid:        c.nextXid(),
		opcode:     opcode,
		pkt:        req,
		recvStruct: res,					//
		recvChan:   make(chan response, 1), // 接收响应的管道
		recvFunc:   recvFunc,				//
	}
	// 2. 发送请求到 `待发送管道`
	c.sendChan <- rq
	// 3. 返回 `请求响应管道`，用于接收 rq 的响应
	return rq.recvChan
}




func (c *Conn) request(opcode int32, req interface{}, res interface{}, recvFunc func(*request, *responseHeader, error)) (int64, error) {
	// r := <-c.queueRequest(opcode, req, res, recvFunc)
	// 1. 发送请求，返回响应接收管道
	resCh := c.queueRequest(opcode, req, res, recvFunc)
	// 2. 从响应管道接收响应（阻塞式）
	r := <- resCh
	// 3. 返回请求 xid 和 错误信息
	return r.zxid, r.err
}

func (c *Conn) AddAuth(scheme string, auth []byte) error {



	_, err := c.request(opSetAuth, &setAuthRequest{Type: 0, Scheme: scheme, Auth: auth}, &setAuthResponse{}, nil)

	if err != nil {
		return err
	}

	// Remember authdata so that it can be re-submitted on reconnect
	//
	// FIXME(prozlach): For now we treat "userfoo:passbar" and "userfoo:passbar2"
	// as two different entries, which will be re-submitted on reconnet.
	//
	// Some research is needed on how ZK treats these cases and then maybe switch to
	// something like "map[username] = password" to allow only single password for given
	// user with users being unique.
	obj := authCreds{
		scheme: scheme,
		auth:   auth,
	}

	c.credsMu.Lock()
	c.creds = append(c.creds, obj)
	c.credsMu.Unlock()

	return nil
}

func (c *Conn) Children(path string) ([]string, *Stat, error) {
	// 在发送请求之前确保路径是有效的
	if err := validatePath(path, false); err != nil {
		return nil, nil, err
	}
	// 构造响应结构体
	res := &getChildren2Response{}
	// 发送请求，并阻塞式等待请求响应
	_, err := c.request(opGetChildren2, &getChildren2Request{Path: path, Watch: false}, res, nil)
	// 返回请求响应结果
	return res.Children, &res.Stat, err
}

func (c *Conn) ChildrenW(path string) ([]string, *Stat, <-chan Event, error) {

	// 在发送请求之前确保路径是有效的
	if err := validatePath(path, false); err != nil {
		return nil, nil, nil, err
	}

	var ech <-chan Event

	// 构造响应结构体
	res := &getChildren2Response{}

	// 发送请求，并阻塞式等待请求响应，这里设置了响应回调，当接收响应时会先调用该回调函数，这里利于闭包的特性，给 ech 赋值。
	_, err := c.request(opGetChildren2, &getChildren2Request{Path: path, Watch: true}, res, func(req *request, res *responseHeader, err error) {
		// 如果请求成功，则添加 watcher 来监听 path 上的 watchTypeChild 类型事件，返回一个事件通知管道 ech 。
		if err == nil {
			ech = c.addWatcher(path, watchTypeChild)
		}
	})

	if err != nil {
		return nil, nil, nil, err
	}

	return res.Children, &res.Stat, ech, err
}

func (c *Conn) Get(path string) ([]byte, *Stat, error) {
	if err := validatePath(path, false); err != nil {
		return nil, nil, err
	}

	res := &getDataResponse{}
	_, err := c.request(opGetData, &getDataRequest{Path: path, Watch: false}, res, nil)
	return res.Data, &res.Stat, err
}

// GetW returns the contents of a znode and sets a watch
func (c *Conn) GetW(path string) ([]byte, *Stat, <-chan Event, error) {
	if err := validatePath(path, false); err != nil {
		return nil, nil, nil, err
	}

	var ech <-chan Event
	res := &getDataResponse{}
	_, err := c.request(opGetData, &getDataRequest{Path: path, Watch: true}, res, func(req *request, res *responseHeader, err error) {
		if err == nil {
			ech = c.addWatcher(path, watchTypeData)
		}
	})
	if err != nil {
		return nil, nil, nil, err
	}
	return res.Data, &res.Stat, ech, err
}

func (c *Conn) Set(path string, data []byte, version int32) (*Stat, error) {
	if err := validatePath(path, false); err != nil {
		return nil, err
	}

	res := &setDataResponse{}
	_, err := c.request(opSetData, &SetDataRequest{path, data, version}, res, nil)
	return &res.Stat, err
}

func (c *Conn) Create(path string, data []byte, flags int32, acl []ACL) (string, error) {
	if err := validatePath(path, flags&FlagSequence == FlagSequence); err != nil {
		return "", err
	}

	res := &createResponse{}
	_, err := c.request(opCreate, &CreateRequest{path, data, acl, flags}, res, nil)
	return res.Path, err
}

// CreateProtectedEphemeralSequential fixes a race condition if the server crashes
// after it creates the node. On reconnect the session may still be valid so the
// ephemeral node still exists. Therefore, on reconnect we need to check if a node
// with a GUID generated on create exists.
func (c *Conn) CreateProtectedEphemeralSequential(path string, data []byte, acl []ACL) (string, error) {
	if err := validatePath(path, true); err != nil {
		return "", err
	}

	var guid [16]byte
	_, err := io.ReadFull(rand.Reader, guid[:16])
	if err != nil {
		return "", err
	}
	guidStr := fmt.Sprintf("%x", guid)

	parts := strings.Split(path, "/")
	parts[len(parts)-1] = fmt.Sprintf("%s%s-%s", protectedPrefix, guidStr, parts[len(parts)-1])
	rootPath := strings.Join(parts[:len(parts)-1], "/")
	protectedPath := strings.Join(parts, "/")

	var newPath string
	for i := 0; i < 3; i++ {
		newPath, err = c.Create(protectedPath, data, FlagEphemeral|FlagSequence, acl)
		switch err {
		case ErrSessionExpired:
			// No need to search for the node since it can't exist. Just try again.
		case ErrConnectionClosed:
			children, _, err := c.Children(rootPath)
			if err != nil {
				return "", err
			}
			for _, p := range children {
				parts := strings.Split(p, "/")
				if pth := parts[len(parts)-1]; strings.HasPrefix(pth, protectedPrefix) {
					if g := pth[len(protectedPrefix) : len(protectedPrefix)+32]; g == guidStr {
						return rootPath + "/" + p, nil
					}
				}
			}
		case nil:
			return newPath, nil
		default:
			return "", err
		}
	}
	return "", err
}

func (c *Conn) Delete(path string, version int32) error {
	if err := validatePath(path, false); err != nil {
		return err
	}

	_, err := c.request(opDelete, &DeleteRequest{path, version}, &deleteResponse{}, nil)
	return err
}

func (c *Conn) Exists(path string) (bool, *Stat, error) {
	if err := validatePath(path, false); err != nil {
		return false, nil, err
	}

	res := &existsResponse{}
	_, err := c.request(opExists, &existsRequest{Path: path, Watch: false}, res, nil)
	exists := true
	if err == ErrNoNode {
		exists = false
		err = nil
	}
	return exists, &res.Stat, err
}

func (c *Conn) ExistsW(path string) (bool, *Stat, <-chan Event, error) {
	if err := validatePath(path, false); err != nil {
		return false, nil, nil, err
	}

	var ech <-chan Event
	res := &existsResponse{}
	_, err := c.request(opExists, &existsRequest{Path: path, Watch: true}, res, func(req *request, res *responseHeader, err error) {
		if err == nil {
			ech = c.addWatcher(path, watchTypeData)
		} else if err == ErrNoNode {
			ech = c.addWatcher(path, watchTypeExist)
		}
	})
	exists := true
	if err == ErrNoNode {
		exists = false
		err = nil
	}
	if err != nil {
		return false, nil, nil, err
	}
	return exists, &res.Stat, ech, err
}

func (c *Conn) GetACL(path string) ([]ACL, *Stat, error) {
	if err := validatePath(path, false); err != nil {
		return nil, nil, err
	}

	res := &getAclResponse{}
	_, err := c.request(opGetAcl, &getAclRequest{Path: path}, res, nil)
	return res.Acl, &res.Stat, err
}
func (c *Conn) SetACL(path string, acl []ACL, version int32) (*Stat, error) {
	if err := validatePath(path, false); err != nil {
		return nil, err
	}

	res := &setAclResponse{}
	_, err := c.request(opSetAcl, &setAclRequest{Path: path, Acl: acl, Version: version}, res, nil)
	return &res.Stat, err
}

func (c *Conn) Sync(path string) (string, error) {
	if err := validatePath(path, false); err != nil {
		return "", err
	}

	res := &syncResponse{}
	_, err := c.request(opSync, &syncRequest{Path: path}, res, nil)
	return res.Path, err
}

type MultiResponse struct {
	Stat   *Stat
	String string
	Error  error
}

// Multi executes multiple ZooKeeper operations or none of them. The provided
// ops must be one of *CreateRequest, *DeleteRequest, *SetDataRequest, or
// *CheckVersionRequest.
func (c *Conn) Multi(ops ...interface{}) ([]MultiResponse, error) {
	req := &multiRequest{
		Ops:        make([]multiRequestOp, 0, len(ops)),
		DoneHeader: multiHeader{Type: -1, Done: true, Err: -1},
	}
	for _, op := range ops {
		var opCode int32
		switch op.(type) {
		case *CreateRequest:
			opCode = opCreate
		case *SetDataRequest:
			opCode = opSetData
		case *DeleteRequest:
			opCode = opDelete
		case *CheckVersionRequest:
			opCode = opCheck
		default:
			return nil, fmt.Errorf("unknown operation type %T", op)
		}
		req.Ops = append(req.Ops, multiRequestOp{multiHeader{opCode, false, -1}, op})
	}
	res := &multiResponse{}
	_, err := c.request(opMulti, req, res, nil)
	mr := make([]MultiResponse, len(res.Ops))
	for i, op := range res.Ops {
		mr[i] = MultiResponse{Stat: op.Stat, String: op.String, Error: op.Err.toError()}
	}
	return mr, err
}

// IncrementalReconfig is the zookeeper reconfiguration api that allows adding and removing servers
// by lists of members.
// Return the new configuration stats.
func (c *Conn) IncrementalReconfig(joining, leaving []string, version int64) (*Stat, error) {
	// TODO: validate the shape of the member string to give early feedback.
	request := &reconfigRequest{
		JoiningServers: []byte(strings.Join(joining, ",")),
		LeavingServers: []byte(strings.Join(leaving, ",")),
		CurConfigId:    version,
	}

	return c.internalReconfig(request)
}

// Reconfig is the non-incremental update functionality for Zookeeper where the list preovided
// is the entire new member list.
// the optional version allows for conditional reconfigurations, -1 ignores the condition.
func (c *Conn) Reconfig(members []string, version int64) (*Stat, error) {
	request := &reconfigRequest{
		NewMembers:  []byte(strings.Join(members, ",")),
		CurConfigId: version,
	}

	return c.internalReconfig(request)
}

func (c *Conn) internalReconfig(request *reconfigRequest) (*Stat, error) {
	response := &reconfigReponse{}
	_, err := c.request(opReconfig, request, response, nil)
	return &response.Stat, err
}

// Server returns the current or last-connected server name.
func (c *Conn) Server() string {
	c.serverMu.Lock()
	defer c.serverMu.Unlock()
	return c.server
}
