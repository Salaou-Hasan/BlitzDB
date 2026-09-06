package blitzdb

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"sync/atomic"
	"time"
)

// Tunables mirror the Rust SDK.
const (
	// DefaultTimeout bounds one call (queue + service).
	DefaultTimeout = 5 * time.Second
	// ClientVersion mirrors the SDK release (§35 messages).
	ClientVersion = "0.2.0"
	// MinServerVersion is the floor this SDK speaks to (§35).
	MinServerVersion = "0.1.0"
	// ProtocolVersionGo must match the server exactly.
	ProtocolVersionGo = 2
	// MaxFlushOps caps one flush frame (protocol bound).
	MaxFlushOps = 4096
	// FlushChunk bounds head-of-line wait inside a frame.
	FlushChunk = 16
)

// CallResult is a procedure outcome: output values plus per-write globals.
type CallResult struct {
	Values  map[string]any
	Applied []AppliedWrite
}

// AppliedWrite is one committed write, in buffer order.
type AppliedWrite struct {
	Table string
	ID    uint64
}

type pending struct {
	req       Request
	retrySafe bool
	enqueued  time.Time
	respCh    chan pendingResult
}

type pendingResult struct {
	resp Response
	err  error
}

type cmd struct {
	direct  bool
	pending *pending
}

// Client is a single pooled TCP connection with a drain-driven batcher.
// Share one Client across goroutines: every method looks like a single op
// while the worker flushes drains as shared frames (singles go as SINGLE
// frames so the Get/Scan fast paths stay hot).
//
// Retry contract (v1, honest): a flush that fails before any response byte
// is retried ONCE after reconnect iff every op is a read or an _idem
// insert (auto-stamped on every Insert). Updates/deletes/calls are never
// auto-retried: retry them yourself (updates are effect-idempotent; treat
// delete's "not found" as success; design procedures around an app _idem).
type Client struct {
	conn    net.Conn
	host    string
	port    int
	timeout time.Duration

	cmds   chan cmd
	closed atomic.Bool

	nextID   atomic.Uint64
	idemPref string
	idemNext atomic.Uint64
}

// Connect dials host:port (eager; fails fast).
func Connect(host string, port int) (*Client, error) {
	return ConnectTimeout(host, port, DefaultTimeout)
}

// ConnectTimeout dials with an explicit timeout.
func ConnectTimeout(host string, port int, timeout time.Duration) (*Client, error) {
	d := net.Dialer{Timeout: timeout}
	conn, err := d.Dial("tcp", net.JoinHostPort(host, fmt.Sprint(port)))
	if err != nil {
		return nil, newErr(ErrTransport, fmt.Sprintf("connect %s:%d: %v", host, port, err))
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	var pref [16]byte
	if _, err := rand.Read(pref[:]); err != nil {
		conn.Close()
		return nil, newErr(ErrTransport, "idem entropy: "+err.Error())
	}
	c := &Client{
		conn:     conn,
		host:     host,
		port:     port,
		timeout:  timeout,
		cmds:     make(chan cmd, 8192),
		idemPref: hex.EncodeToString(pref[:]),
	}
	c.nextID.Store(1)
	c.idemNext.Store(1)
	// Compatibility handshake first (§35): one RTT that turns version
	// skew into a readable error instead of later garbage.
	if err := c.checkServerVersion(); err != nil {
		conn.Close()
		return nil, err
	}
	go c.worker()
	return c, nil
}

// Close shuts the client down (fails in-flight ops).
func (c *Client) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) allocID() uint64   { return c.nextID.Add(1) }
func (c *Client) allocIDEM() string { return fmt.Sprintf("%s-%d", c.idemPref, c.idemNext.Add(1)) }

func (c *Client) exec(req Request, retrySafe, direct bool) (Response, error) {
	if c.closed.Load() {
		return Response{}, newErr(ErrClosed, "client closed")
	}
	p := &pending{
		req:       req,
		retrySafe: retrySafe,
		enqueued:  time.Now(),
		respCh:    make(chan pendingResult, 1),
	}
	select {
	case c.cmds <- cmd{direct: direct, pending: p}:
	case <-time.After(c.timeout):
		return Response{}, newErr(ErrTimeout, "enqueue timed out")
	}
	// No per-op timer: the worker enforces the deadline as a queue bound
	// plus per-flush socket deadlines, and always settles every pending.
	r := <-p.respCh
	return r.resp, r.err
}

// parseVersion parses major.minor.patch (ignores pre-release/build).
func parseVersion(v string) ([3]uint64, bool) {
	var out [3]uint64
	core := v
	if j := indexByte(core, '-'); j >= 0 {
		core = core[:j]
	} else if j := indexByte(core, '+'); j >= 0 {
		core = core[:j]
	}
	parts := splitDots(core)
	if len(parts) < 3 {
		return out, false
	}
	for i := 0; i < 3; i++ {
		n, ok := parseUintDec(parts[i])
		if !ok {
			return out, false
		}
		out[i] = n
	}
	return out, true
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

func splitDots(s string) []string {
	var parts []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == '.' {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return parts
}

func parseUintDec(s string) (uint64, bool) {
	if s == "" {
		return 0, false
	}
	var n uint64
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		n = n*10 + uint64(s[i]-'0')
	}
	return n, true
}

func cmpVersion(a, b [3]uint64) int {
	for i := 0; i < 3; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

// CheckCompat is the §35 verdict: exact protocol match plus server floor.
func CheckCompat(server string, protocol int) error {
	if protocol != ProtocolVersionGo {
		return newErr(ErrServer, fmt.Sprintf(
			"BlitzDB client v%s (protocol %d) requires server protocol %d (server v%s speaks v%d)",
			ClientVersion, ProtocolVersionGo, ProtocolVersionGo, server, protocol))
	}
	floor, _ := parseVersion(MinServerVersion)
	got, ok := parseVersion(server)
	if !ok || cmpVersion(got, floor) < 0 {
		return newErr(ErrServer, fmt.Sprintf(
			"BlitzDB client v%s requires BlitzDB server >= %s (found %s)",
			ClientVersion, MinServerVersion, server))
	}
	return nil
}

// checkServerVersion performs the one-shot pre-worker handshake.
func (c *Client) checkServerVersion() error {
	codec := NewFrameCodec()
	frame, err := codec.EncodeRequest(Request{ID: 0, Op: OpVersion, Table: ""})
	if err != nil {
		return newErr(ErrTransport, "version encode: "+err.Error())
	}
	_ = c.conn.SetDeadline(time.Now().Add(c.timeout))
	if _, err := c.conn.Write(frame); err != nil {
		return newErr(ErrTransport, "version write: "+err.Error())
	}
	tmp := make([]byte, 4096)
	for {
		n, err := c.conn.Read(tmp)
		if err != nil {
			return newErr(ErrTransport, "version read: "+err.Error())
		}
		if n == 0 {
			return newErr(ErrTransport, "closed during version check")
		}
		payloads, err := codec.Feed(tmp[:n])
		if err != nil {
			return newErr(ErrTransport, "version framing: "+err.Error())
		}
		if len(payloads) == 0 {
			continue
		}
		resp, err := codec.DecodeResponse(payloads[0])
		if err != nil {
			return newErr(ErrTransport, "version decode: "+err.Error())
		}
		if !resp.OK {
			return newErr(ErrTransport, "version refused: "+resp.Error)
		}
		if len(resp.Rows) == 0 {
			return newErr(ErrTransport, "empty version response")
		}
		server, _ := resp.Rows[0].Values["server"].(string)
		var protocol int
		switch v := resp.Rows[0].Values["protocol"].(type) {
		case int64:
			protocol = int(v)
		case uint64:
			protocol = int(v)
		case int32:
			protocol = int(v)
		default:
			return newErr(ErrTransport, "malformed version response")
		}
		if server == "" {
			return newErr(ErrTransport, "malformed version response")
		}
		return CheckCompat(server, protocol)
	}
}

func (c *Client) okRows(resp Response) ([]Row, error) {
	if resp.OK {
		return resp.Rows, nil
	}
	msg := resp.Error
	if msg == "" {
		msg = "unknown error"
	}
	return nil, MapServerError(msg)
}

// -- worker ---------------------------------------------------------------

// worker owns the socket: drains queued commands into ordered, chunked
// frames. Direct commands (ping/auth) always fly alone.
func (c *Client) worker() {
	codec := NewFrameCodec()
	for {
		first, ok := <-c.cmds
		if !ok || c.closed.Load() {
			return
		}
		// Drain everything already queued (the batch under load).
		batch := []cmd{first}
	drain:
		for len(batch) < MaxFlushOps {
			select {
			case cm := <-c.cmds:
				batch = append(batch, cm)
			default:
				break drain
			}
		}
		// Segment in order: Batch runs share frames, Directs fly alone.
		i := 0
		for i < len(batch) {
			if batch[i].direct {
				c.flush(codec, []*pending{batch[i].pending})
				i++
				continue
			}
			var run []*pending
			for i < len(batch) && !batch[i].direct && len(run) < FlushChunk {
				run = append(run, batch[i].pending)
				i++
			}
			c.flush(codec, run)
			if c.closed.Load() {
				return
			}
		}
	}
}

func (c *Client) flush(codec *FrameCodec, run []*pending) {
	if len(run) == 0 || c.closed.Load() {
		for _, p := range run {
			p.respCh <- pendingResult{err: newErr(ErrClosed, "client closed")}
		}
		return
	}
	// Queue deadline: already-expired ops fail without touching the socket.
	now := time.Now()
	fresh := run[:0]
	for _, p := range run {
		if now.Sub(p.enqueued) >= c.timeout {
			p.respCh <- pendingResult{err: newErr(ErrTimeout, "timeout")}
		} else {
			fresh = append(fresh, p)
		}
	}
	if len(fresh) == 0 {
		return
	}
	retrySafe := true
	reqs := make([]Request, 0, len(fresh))
	for _, p := range fresh {
		retrySafe = retrySafe && p.retrySafe
		reqs = append(reqs, p.req)
	}
	resps, err := c.roundtrip(codec, reqs, retrySafe)
	if err != nil {
		for _, p := range fresh {
			p.respCh <- pendingResult{err: err}
		}
		return
	}
	for i, p := range fresh {
		p.respCh <- pendingResult{resp: resps[i]}
	}
}

func (c *Client) roundtrip(codec *FrameCodec, reqs []Request, retrySafe bool) ([]Response, error) {
	var frame []byte
	var err error
	if len(reqs) == 1 {
		frame, err = codec.EncodeRequest(reqs[0])
	} else {
		frame, err = codec.EncodeBatch(reqs[0].ID, reqs, false)
	}
	if err != nil {
		return nil, newErr(ErrTransport, err.Error())
	}
	resps, err := c.writeRead(codec, frame, len(reqs) == 1)
	if err != nil {
		if !retrySafe {
			return nil, newErr(ErrTransport, "flush failed (not retry-safe; retry manually)")
		}
		if err := c.reconnect(); err != nil {
			return nil, err
		}
		resps, err = c.writeRead(codec, frame, len(reqs) == 1)
		if err != nil {
			return nil, newErr(ErrTransport, "flush failed after reconnect")
		}
	}
	return resps, nil
}

func (c *Client) writeRead(codec *FrameCodec, frame []byte, single bool) ([]Response, error) {
	_ = c.conn.SetDeadline(time.Now().Add(c.timeout))
	if _, err := c.conn.Write(frame); err != nil {
		return nil, err
	}
	tmp := make([]byte, 65536)
	for {
		n, err := c.conn.Read(tmp)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				return nil, newErr(ErrTimeout, "read timed out")
			}
			return nil, err
		}
		if n == 0 {
			return nil, newErr(ErrTransport, "connection closed mid-flush")
		}
		// The codec stashes internally; each feed returns newly completed
		// payloads (usually exactly one: our response frame).
		payloads, err := codec.Feed(tmp[:n])
		if err != nil {
			return nil, newErr(ErrTransport, err.Error())
		}
		if len(payloads) == 0 {
			continue
		}
		if single {
			resp, err := codec.DecodeResponse(payloads[0])
			if err != nil {
				return nil, newErr(ErrTransport, err.Error())
			}
			return []Response{resp}, nil
		}
		b, err := codec.DecodeBatchResponse(payloads[0])
		if err != nil {
			return nil, newErr(ErrTransport, err.Error())
		}
		return b.Results, nil
	}
}

// drainCodec pulls already-completed payloads without new bytes.
func drainCodec(codec *FrameCodec) ([][]byte, error) {
	return codec.Feed(nil)
}

func (c *Client) reconnect() error {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(c.host, fmt.Sprint(c.port)), c.timeout)
	if err != nil {
		return newErr(ErrTransport, "reconnect failed: "+err.Error())
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	old := c.conn
	c.conn = conn
	_ = old.Close()
	return nil
}

// reconnect races with in-flight writeRead on the old socket. The old
// socket is only replaced here (worker thread), so no lock is needed:
// all IO happens on the worker.

// -- primitive ops ----------------------------------------------------------

func (c *Client) req(op, table string) Request {
	return Request{ID: c.allocID(), Op: op, Table: table}
}

// Authenticate handshakes this connection (identity persists).
func (c *Client) Authenticate(token string) error {
	r := c.req(OpPing, "")
	r.Values = map[string]any{"_auth": token}
	resp, err := c.exec(r, true, true)
	if err != nil {
		return err
	}
	_, err = c.okRows(resp)
	return err
}

// Ping probes liveness (direct frame, no batching delay).
func (c *Client) Ping() error {
	resp, err := c.exec(c.req(OpPing, ""), true, true)
	if err != nil {
		return err
	}
	_, err = c.okRows(resp)
	return err
}

// Insert stamps auto _idem (safe reconnect-replay inside the window).
func (c *Client) Insert(table string, values map[string]any) (Row, error) {
	vals := make(map[string]any, len(values)+1)
	for k, v := range values {
		vals[k] = v
	}
	vals["_idem"] = c.allocIDEM()
	r := c.req(OpInsert, table)
	r.Values = vals
	resp, err := c.exec(r, true, false)
	if err != nil {
		return Row{}, err
	}
	rows, err := c.okRows(resp)
	if err != nil {
		return Row{}, err
	}
	if len(rows) == 0 {
		return Row{}, newErr(ErrServer, "insert returned no rows")
	}
	return rows[len(rows)-1], nil
}

// InsertFast skips _idem (expert path, bench wire parity). At-most-once:
// never auto-retried — retry manually with the same values.
func (c *Client) InsertFast(table string, values map[string]any) (Row, error) {
	r := c.req(OpInsert, table)
	r.Values = values
	resp, err := c.exec(r, false, false)
	if err != nil {
		return Row{}, err
	}
	rows, err := c.okRows(resp)
	if err != nil {
		return Row{}, err
	}
	if len(rows) == 0 {
		return Row{}, newErr(ErrServer, "insert returned no rows")
	}
	return rows[len(rows)-1], nil
}

// Get returns nil, nil on row miss (table miss stays an error).
func (c *Client) Get(table string, id uint64) (*Row, error) {
	r := c.req(OpGet, table)
	r.RowID = &id
	resp, err := c.exec(r, true, false)
	if err != nil {
		return nil, err
	}
	if resp.OK {
		if len(resp.Rows) == 0 {
			return nil, nil
		}
		row := resp.Rows[0]
		return &row, nil
	}
	msg := resp.Error
	if msg == "" {
		msg = "unknown error"
	}
	if len(msg) >= 13 && msg[:13] == "row not found" {
		return nil, nil
	}
	return nil, MapServerError(msg)
}

// Update is not auto-retried: retry manually on transport error.
func (c *Client) Update(table string, id uint64, values map[string]any) (Row, error) {
	r := c.req(OpUpdate, table)
	r.RowID = &id
	r.Values = values
	resp, err := c.exec(r, false, false)
	if err != nil {
		return Row{}, err
	}
	rows, err := c.okRows(resp)
	if err != nil {
		return Row{}, err
	}
	if len(rows) == 0 {
		return Row{}, newErr(ErrServer, "update returned no rows")
	}
	return rows[len(rows)-1], nil
}

// Delete is not auto-retried: on transport error, re-Get first; treat a
// retry's "not found" as success (already deleted).
func (c *Client) Delete(table string, id uint64) error {
	r := c.req(OpDelete, table)
	r.RowID = &id
	resp, err := c.exec(r, false, false)
	if err != nil {
		return err
	}
	_, err = c.okRows(resp)
	return err
}

// Scan pages with limit/cursor/desc.
func (c *Client) Scan(table string, limit int, cursor *uint64, desc bool) ([]Row, error) {
	values := map[string]any{"_limit": int64(limit)}
	if desc {
		values["_order"] = "desc"
	}
	if cursor != nil {
		values["_cursor"] = *cursor
	}
	r := c.req(OpScan, table)
	r.Values = values
	resp, err := c.exec(r, true, false)
	if err != nil {
		return nil, err
	}
	return c.okRows(resp)
}

// Find by unique column (nil, nil on miss).
func (c *Client) Find(table, column string, value any) (*Row, error) {
	r := c.req(OpFind, table)
	r.Values = map[string]any{"_col": column, "_val": value}
	resp, err := c.exec(r, true, false)
	if err != nil {
		return nil, err
	}
	if resp.OK {
		if len(resp.Rows) == 0 {
			return nil, nil
		}
		row := resp.Rows[0]
		return &row, nil
	}
	msg := resp.Error
	if msg == "" {
		msg = "unknown error"
	}
	if msg == "not found" {
		return nil, nil
	}
	return nil, MapServerError(msg)
}

// Search runs a bounded exact-term query.
func (c *Client) Search(table, query string, limit int) ([]Row, error) {
	r := c.req(OpSearch, table)
	r.Values = map[string]any{"_q": query, "_limit": int64(limit)}
	resp, err := c.exec(r, true, false)
	if err != nil {
		return nil, err
	}
	return c.okRows(resp)
}

// CreateTable creates a table from a JSON schema map. Never
// auto-retried: a retried-after-success call honestly reports
// "already exists".
func (c *Client) CreateTable(schema map[string]any) (string, error) {
	r := c.req(OpTableCreate, "")
	r.Values = map[string]any{"schema": schema}
	resp, err := c.exec(r, false, false)
	if err != nil {
		return "", err
	}
	rows, err := c.okRows(resp)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", newErr(ErrServer, "table_create returned no rows")
	}
	name, _ := rows[len(rows)-1].Values["table"].(string)
	if name == "" {
		return "", newErr(ErrServer, "table_create response missing table")
	}
	return name, nil
}

// Call executes a registered procedure transactionally. Not auto-retried:
// design procedures around an application _idem argument instead.
func (c *Client) Call(fn string, args map[string]any) (CallResult, error) {
	var out CallResult
	r := c.req(OpCall, "fn:"+fn)
	r.Values = args
	resp, err := c.exec(r, false, false)
	if err != nil {
		return out, err
	}
	rows, err := c.okRows(resp)
	if err != nil {
		return out, err
	}
	if len(rows) == 0 {
		return out, newErr(ErrServer, "call returned no rows")
	}
	out.Values = map[string]any{}
	for k, v := range rows[len(rows)-1].Values {
		out.Values[k] = v
	}
	if raw, ok := out.Values["_applied"]; ok {
		delete(out.Values, "_applied")
		if arr, ok := raw.([]any); ok {
			for _, e := range arr {
				m, ok := e.(map[string]any)
				if !ok {
					continue
				}
				t, _ := m["table"].(string)
				var id uint64
				switch n := m["id"].(type) {
				case uint64:
					id = n
				case int64:
					if n >= 0 {
						id = uint64(n)
					}
				case float64:
					id = uint64(n)
				}
				if t != "" {
					out.Applied = append(out.Applied, AppliedWrite{Table: t, ID: id})
				}
			}
		}
	}
	return out, nil
}

// PollChanges reads recent writes (single long-poll, not a stream).
func (c *Client) PollChanges(table string, since uint64, limit int) ([]Row, error) {
	r := c.req(OpSubscribe, table)
	r.Values = map[string]any{"_since": since, "_limit": int64(limit)}
	resp, err := c.exec(r, true, false)
	if err != nil {
		return nil, err
	}
	return c.okRows(resp)
}
