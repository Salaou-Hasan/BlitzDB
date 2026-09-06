package blitzdb

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

// Wire constants mirror blitz-protocol/src/codec.rs exactly.
const (
	protocolVersion = 2
	headerLen       = 5
	defaultMaxFrame = 8 * 1024 * 1024

	kindRequest      = 0x01
	kindBatchRequest = 0x02
	kindAtomicBatch  = 0x03
	kindResponse     = 0x11
	kindBatchResp    = 0x12
)

// Op names used on the wire (tags 0-14).
const (
	OpPing      = "ping"
	OpInsert    = "insert"
	OpGet       = "get"
	OpUpdate    = "update"
	OpDelete    = "delete"
	OpScan      = "scan"
	OpSubscribe = "subscribe"
	OpFind      = "find"
	OpSearch    = "search"
	OpCall      = "call"
	OpJobSubmit = "job_submit"
	OpJobPoll   = "job_poll"
	OpProcDep   = "proc_deploy"
	OpProcList  = "proc_list"
	OpProcDrop  = "proc_drop"
	OpVersion   = "version"
	OpTableCreate = "table_create"
)

var opToTag = map[string]byte{
	OpPing: 0, OpInsert: 1, OpGet: 2, OpUpdate: 3, OpDelete: 4,
	OpScan: 5, OpSubscribe: 6, OpFind: 7, OpSearch: 8, OpCall: 9,
	OpJobSubmit: 10, OpJobPoll: 11, OpProcDep: 12, OpProcList: 13, OpProcDrop: 14,
	OpVersion: 15,
	OpTableCreate: 16,
}

var tagToOp = func() map[byte]string {
	m := make(map[byte]string, len(opToTag))
	for k, v := range opToTag {
		m[v] = k
	}
	return m
}()

// Request is one operation.
type Request struct {
	ID     uint64
	Op     string
	Table  string
	RowID  *uint64
	Values map[string]any
}

// Response is one per-op result.
type Response struct {
	ID    uint64
	OK    bool
	Rows  []Row
	Error string
}

// BatchResponse carries per-op results in order.
type BatchResponse struct {
	ID      uint64
	Results []Response
}

// DecodeError is a framing/decoding failure (caller bug or corrupt peer).
type DecodeError struct{ Msg string }

func (e *DecodeError) Error() string { return "decode: " + e.Msg }

func decErr(f string, a ...any) *DecodeError {
	return &DecodeError{Msg: fmt.Sprintf(f, a...)}
}

// -- writer ---------------------------------------------------------------

type writer struct{ buf []byte }

func (w *writer) u8(v byte)     { w.buf = append(w.buf, v) }
func (w *writer) u16(v uint16)  { w.buf = binary.LittleEndian.AppendUint16(w.buf, v) }
func (w *writer) u32(v uint32)  { w.buf = binary.LittleEndian.AppendUint32(w.buf, v) }
func (w *writer) i16(v int16)   { w.buf = binary.LittleEndian.AppendUint16(w.buf, uint16(v)) }
func (w *writer) i32(v int32)   { w.buf = binary.LittleEndian.AppendUint32(w.buf, uint32(v)) }
func (w *writer) u64(v uint64)  { w.buf = binary.LittleEndian.AppendUint64(w.buf, v) }
func (w *writer) i64(v int64)   { w.buf = binary.LittleEndian.AppendUint64(w.buf, uint64(v)) }
func (w *writer) f32(v float32) { w.buf = binary.LittleEndian.AppendUint32(w.buf, math.Float32bits(v)) }
func (w *writer) f64(v float64) { w.buf = binary.LittleEndian.AppendUint64(w.buf, math.Float64bits(v)) }
func (w *writer) str(s string)  { w.u32(uint32(len(s))); w.buf = append(w.buf, s...) }
func (w *writer) raw(b []byte)  { w.buf = append(w.buf, b...) }

func (w *writer) value(v any) error {
	switch t := v.(type) {
	case nil:
		w.u8(0x00)
	case bool:
		w.u8(0x01)
		if t {
			w.u8(1)
		} else {
			w.u8(0)
		}
	case int8:
		w.u8(0x02)
		w.u8(byte(t))
	case int16:
		w.u8(0x03)
		w.i16(t)
	case int32:
		w.u8(0x04)
		w.i32(t)
	case int64:
		w.u8(0x05)
		w.i64(t)
	case int:
		w.u8(0x05)
		w.i64(int64(t))
	case uint8:
		w.u8(0x06)
		w.u8(t)
	case uint16:
		w.u8(0x07)
		w.u16(t)
	case uint32:
		w.u8(0x08)
		w.u32(t)
	case uint64:
		w.u8(0x09)
		w.u64(t)
	case uint:
		w.u8(0x09)
		w.u64(uint64(t))
	case float32:
		w.u8(0x0A)
		w.f32(t)
	case float64:
		w.u8(0x0B)
		w.f64(t)
	case string:
		if IsUUIDShaped(t) {
			b, err := uuidToBytes(t)
			if err != nil {
				return err
			}
			w.u8(0x0F)
			w.raw(b[:])
		} else {
			w.u8(0x0D)
			w.str(t)
		}
	case []byte:
		w.u8(0x0E)
		w.u32(uint32(len(t)))
		w.raw(t)
	case time.Time:
		w.u8(0x10)
		w.i64(t.UTC().UnixMicro())
	case map[string]any:
		if len(t) == 1 {
			if n, ok := intOf(t, "$i32"); ok {
				if n < -2147483648 || n > 2147483647 {
					return decErr("$i32 out of range")
				}
				w.u8(0x04)
				w.i32(int32(n))
				return nil
			}
			if n, ok := uintOf(t, "$u32"); ok {
				if n > 4294967295 {
					return decErr("$u32 out of range")
				}
				w.u8(0x08)
				w.u32(uint32(n))
				return nil
			}
			if n, ok := intOf(t, "$i64"); ok {
				w.u8(0x05)
				w.i64(n)
				return nil
			}
			if n, ok := uintOf(t, "$u64"); ok {
				w.u8(0x09)
				w.u64(n)
				return nil
			}
			if f, ok := floatOf(t, "$f32"); ok {
				w.u8(0x0A)
				w.f32(float32(f))
				return nil
			}
			if s, ok := t["$decimal"].(string); ok {
				w.u8(0x0C)
				w.str(s)
				return nil
			}
			if s, ok := t["$date"].(string); ok {
				y, mo, d, err := parseYMD(s)
				if err != nil {
					return err
				}
				w.u8(0x11)
				w.i32(daysFromCivil(y, mo, d))
				w.u32(uint32(mo))
				w.u32(uint32(d))
				return nil
			}
			if s, ok := t["$uuid"].(string); ok {
				b, err := uuidToBytes(s)
				if err != nil {
					return err
				}
				w.u8(0x0F)
				w.raw(b[:])
				return nil
			}
			if s, ok := t["$bytes"].(string); ok {
				b, err := decodeBase64(s)
				if err != nil {
					return decErr("bad $bytes base64")
				}
				w.u8(0x0E)
				w.u32(uint32(len(b)))
				w.raw(b)
				return nil
			}
			if raw, ok := t["$ts"]; ok {
				var micros int64
				switch n := raw.(type) {
				case int64:
					micros = n
				case int:
					micros = int64(n)
				case float64:
					micros = int64(n)
				default:
					return decErr("bad $ts")
				}
				w.u8(0x10)
				w.i64(micros)
				return nil
			}
		}
		w.u8(0x12)
		return w.json(t)
	case []any:
		if len(t) > 65536 {
			return decErr("array too large: %d", len(t))
		}
		w.u8(0x13)
		w.u32(uint32(len(t)))
		for _, item := range t {
			if err := w.value(item); err != nil {
				return err
			}
		}
	default:
		return decErr("unencodable value %T", v)
	}
	return nil
}

func intOf(m map[string]any, k string) (int64, bool) {
	switch t := m[k].(type) {
	case int64:
		return t, true
	case int:
		return int64(t), true
	case int32:
		return int64(t), true
	case float64:
		if t == float64(int64(t)) {
			return int64(t), true
		}
	}
	return 0, false
}

func uintOf(m map[string]any, k string) (uint64, bool) {
	switch t := m[k].(type) {
	case uint64:
		return t, true
	case uint:
		return uint64(t), true
	case int:
		if t >= 0 {
			return uint64(t), true
		}
	}
	return 0, false
}

func floatOf(m map[string]any, k string) (float64, bool) {
	switch t := m[k].(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	}
	return 0, false
}

func parseYMD(s string) (int, int, int, error) {
	var y, mo, d int
	if _, err := fmt.Sscanf(s, "%04d-%02d-%02d", &y, &mo, &d); err != nil {
		return 0, 0, 0, decErr("bad $date: %s", s)
	}
	if len(s) != 10 {
		return 0, 0, 0, decErr("bad $date: %s", s)
	}
	return y, mo, d, nil
}

func (w *writer) json(v any) error {
	switch t := v.(type) {
	case nil:
		w.u8(0x00)
	case bool:
		w.u8(0x01)
		if t {
			w.u8(1)
		} else {
			w.u8(0)
		}
	case int64:
		w.u8(0x05)
		w.i64(t)
	case int:
		w.u8(0x05)
		w.i64(int64(t))
	case float64:
		w.u8(0x0B)
		w.f64(t)
	case string:
		w.u8(0x0D)
		w.str(t)
	case []any:
		w.u8(0x13)
		w.u32(uint32(len(t)))
		for _, item := range t {
			if err := w.json(item); err != nil {
				return err
			}
		}
	case map[string]any:
		w.u8(0x12)
		w.u32(uint32(len(t)))
		for k, item := range t {
			w.str(k)
			if err := w.json(item); err != nil {
				return err
			}
		}
	default:
		return decErr("unencodable json %T", v)
	}
	return nil
}

func (w *writer) valueMap(m map[string]any) error {
	w.u32(uint32(len(m)))
	for k, v := range m {
		w.str(k)
		if err := w.value(v); err != nil {
			return err
		}
	}
	return nil
}

func (w *writer) requestBody(r Request) error {
	tag, ok := opToTag[r.Op]
	if !ok {
		return decErr("unknown op: %s", r.Op)
	}
	w.u64(r.ID)
	w.u8(tag)
	w.str(r.Table)
	if r.RowID == nil {
		w.u8(0)
	} else {
		w.u8(1)
		w.u64(*r.RowID)
	}
	if r.Values == nil {
		w.u8(0)
	} else {
		w.u8(1)
		if err := w.valueMap(r.Values); err != nil {
			return err
		}
	}
	return nil
}

func (w *writer) responseBody(r Response) error {
	w.u64(r.ID)
	if r.OK {
		w.u8(1)
	} else {
		w.u8(0)
	}
	w.u32(uint32(len(r.Rows)))
	for _, row := range r.Rows {
		w.u64(row.ID)
		if err := w.valueMap(row.Values); err != nil {
			return err
		}
	}
	if r.Error == "" {
		w.u8(0)
	} else {
		w.u8(1)
		w.str(r.Error)
	}
	return nil
}

// -- reader ---------------------------------------------------------------

type reader struct {
	buf []byte
	off int
}

func (r *reader) need(n int, what string) error {
	if len(r.buf)-r.off < n {
		return decErr("truncated %s", what)
	}
	return nil
}

func (r *reader) u8() (byte, error) {
	if err := r.need(1, "frame"); err != nil {
		return 0, err
	}
	v := r.buf[r.off]
	r.off++
	return v, nil
}

func (r *reader) u16() (uint16, error) {
	if err := r.need(2, "frame"); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint16(r.buf[r.off:])
	r.off += 2
	return v, nil
}

func (r *reader) i16() (int16, error) {
	v, err := r.u16()
	return int16(v), err
}

func (r *reader) u32() (uint32, error) {
	if err := r.need(4, "frame"); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint32(r.buf[r.off:])
	r.off += 4
	return v, nil
}

func (r *reader) i32() (int32, error) {
	v, err := r.u32()
	return int32(v), err
}

func (r *reader) u64() (uint64, error) {
	if err := r.need(8, "frame"); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint64(r.buf[r.off:])
	r.off += 8
	return v, nil
}

func (r *reader) i64() (int64, error) {
	v, err := r.u64()
	return int64(v), err
}

func (r *reader) f32() (float32, error) {
	v, err := r.u32()
	return math.Float32frombits(v), err
}

func (r *reader) f64() (float64, error) {
	v, err := r.u64()
	return math.Float64frombits(v), err
}

func (r *reader) str() (string, error) {
	n, err := r.u32()
	if err != nil {
		return "", err
	}
	if err := r.need(int(n), "string"); err != nil {
		return "", err
	}
	s := string(r.buf[r.off : r.off+int(n)])
	r.off += int(n)
	return s, nil
}

func (r *reader) take(n int, what string) ([]byte, error) {
	if err := r.need(n, what); err != nil {
		return nil, err
	}
	b := r.buf[r.off : r.off+n]
	r.off += n
	return b, nil
}

func (r *reader) value() (any, error) {
	tag, err := r.u8()
	if err != nil {
		return nil, err
	}
	switch tag {
	case 0x00:
		return nil, nil
	case 0x01:
		b, err := r.u8()
		return b != 0, err
	case 0x02:
		b, err := r.u8()
		return int8(b), err
	case 0x03:
		return r.i16()
	case 0x04:
		return r.i32()
	case 0x05:
		return r.i64()
	case 0x06:
		b, err := r.u8()
		return b, err
	case 0x07:
		return r.u16()
	case 0x08:
		return r.u32()
	case 0x09:
		return r.u64()
	case 0x0A:
		return r.f32()
	case 0x0B:
		return r.f64()
	case 0x0C:
		s, err := r.str()
		return map[string]any{"$decimal": s}, err
	case 0x0D:
		return r.str()
	case 0x0E:
		n, err := r.u32()
		if err != nil {
			return nil, err
		}
		return r.take(int(n), "bytes")
	case 0x0F:
		b, err := r.take(16, "uuid")
		if err != nil {
			return nil, err
		}
		return bytesToUUID(b), nil
	case 0x10:
		micros, err := r.i64()
		if err != nil {
			return nil, err
		}
		return time.UnixMicro(micros).UTC(), nil
	case 0x11:
		days, err := r.i32()
		if err != nil {
			return nil, err
		}
		month, err := r.u32()
		if err != nil {
			return nil, err
		}
		day, err := r.u32()
		if err != nil {
			return nil, err
		}
		y, mo, d := civilFromDays(days)
		if mo != int(month) || d != int(day) {
			return nil, decErr("bad date")
		}
		return map[string]any{"$date": fmt.Sprintf("%04d-%02d-%02d", y, mo, d)}, nil
	case 0x12:
		return r.json()
	case 0x13:
		n, err := r.u32()
		if err != nil {
			return nil, err
		}
		if n > 65536 {
			return nil, decErr("array too large: %d", n)
		}
		out := make([]any, 0, n)
		for i := uint32(0); i < n; i++ {
			v, err := r.value()
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	}
	return nil, decErr("unknown value tag: %#x", tag)
}

func (r *reader) json() (any, error) {
	tag, err := r.u8()
	if err != nil {
		return nil, err
	}
	switch tag {
	case 0x00:
		return nil, nil
	case 0x01:
		b, err := r.u8()
		return b != 0, err
	case 0x05:
		return r.i64()
	case 0x09:
		return r.u64()
	case 0x0B:
		return r.f64()
	case 0x0D:
		return r.str()
	case 0x12:
		n, err := r.u32()
		if err != nil {
			return nil, err
		}
		if n > 4096 {
			return nil, decErr("json object too large: %d", n)
		}
		obj := make(map[string]any, n)
		for i := uint32(0); i < n; i++ {
			k, err := r.str()
			if err != nil {
				return nil, err
			}
			v, err := r.json()
			if err != nil {
				return nil, err
			}
			obj[k] = v
		}
		return obj, nil
	case 0x13:
		n, err := r.u32()
		if err != nil {
			return nil, err
		}
		if n > 65536 {
			return nil, decErr("json array too large: %d", n)
		}
		out := make([]any, 0, n)
		for i := uint32(0); i < n; i++ {
			v, err := r.json()
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	}
	return nil, decErr("bad json tag: %#x", tag)
}

func (r *reader) valueMap() (map[string]any, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	out := make(map[string]any, n)
	for i := uint32(0); i < n; i++ {
		k, err := r.str()
		if err != nil {
			return nil, err
		}
		v, err := r.value()
		if err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, nil
}

func (r *reader) requestBody() (Request, error) {
	var req Request
	id, err := r.u64()
	if err != nil {
		return req, err
	}
	t, err := r.u8()
	if err != nil {
		return req, err
	}
	op, ok := tagToOp[t]
	if !ok {
		return req, decErr("unknown op tag")
	}
	table, err := r.str()
	if err != nil {
		return req, err
	}
	hasRow, err := r.u8()
	if err != nil {
		return req, err
	}
	var rowID *uint64
	if hasRow != 0 {
		id, err := r.u64()
		if err != nil {
			return req, err
		}
		rowID = &id
	}
	hasVals, err := r.u8()
	if err != nil {
		return req, err
	}
	var values map[string]any
	if hasVals != 0 {
		values, err = r.valueMap()
		if err != nil {
			return req, err
		}
	}
	return Request{ID: id, Op: op, Table: table, RowID: rowID, Values: values}, nil
}

func (r *reader) rowView() (Row, error) {
	var row Row
	id, err := r.u64()
	if err != nil {
		return row, err
	}
	values, err := r.valueMap()
	if err != nil {
		return row, err
	}
	return Row{ID: id, Values: values}, nil
}

func (r *reader) responseBody() (Response, error) {
	var resp Response
	id, err := r.u64()
	if err != nil {
		return resp, err
	}
	ok, err := r.u8()
	if err != nil {
		return resp, err
	}
	n, err := r.u32()
	if err != nil {
		return resp, err
	}
	resp.ID = id
	resp.OK = ok != 0
	for i := uint32(0); i < n; i++ {
		row, err := r.rowView()
		if err != nil {
			return resp, err
		}
		resp.Rows = append(resp.Rows, row)
	}
	hasErr, err := r.u8()
	if err != nil {
		return resp, err
	}
	if hasErr != 0 {
		msg, err := r.str()
		if err != nil {
			return resp, err
		}
		resp.Error = msg
	}
	return resp, nil
}

func (r *reader) end() error {
	if len(r.buf)-r.off != 0 {
		return decErr("trailing bytes")
	}
	return nil
}

// -- FrameCodec -----------------------------------------------------------

// FrameCodec frames payloads and splits TCP streams (mirrors Rust).
type FrameCodec struct {
	MaxFrame int
	stash    []byte
}

// NewFrameCodec makes a codec with the default 8MiB frame budget.
func NewFrameCodec() *FrameCodec { return &FrameCodec{MaxFrame: defaultMaxFrame} }

func (c *FrameCodec) frame(payload []byte) []byte {
	head := make([]byte, headerLen)
	head[0] = protocolVersion
	binary.LittleEndian.PutUint32(head[1:], uint32(len(payload)))
	return append(head, payload...)
}

// Feed pushes bytes; returns all complete payloads.
func (c *FrameCodec) Feed(chunk []byte) ([][]byte, error) {
	c.stash = append(c.stash, chunk...)
	var out [][]byte
	for len(c.stash) >= headerLen {
		if c.stash[0] != protocolVersion {
			return nil, decErr("unsupported version: %d", c.stash[0])
		}
		ln := int(binary.LittleEndian.Uint32(c.stash[1:]))
		if ln > c.MaxFrame {
			return nil, decErr("frame too large: %d > %d", ln, c.MaxFrame)
		}
		if len(c.stash) < headerLen+ln {
			break
		}
		out = append(out, c.stash[headerLen:headerLen+ln])
		c.stash = append([]byte(nil), c.stash[headerLen+ln:]...)
	}
	return out, nil
}

// EncodeRequest frames one op.
func (c *FrameCodec) EncodeRequest(r Request) ([]byte, error) {
	w := &writer{}
	w.u8(kindRequest)
	if err := w.requestBody(r); err != nil {
		return nil, err
	}
	return c.frame(w.buf), nil
}

// EncodeBatch frames N ops (atomic selects kind 0x03).
func (c *FrameCodec) EncodeBatch(id uint64, ops []Request, atomic bool) ([]byte, error) {
	if len(ops) > 4096 {
		return nil, decErr("batch too large: %d", len(ops))
	}
	w := &writer{}
	if atomic {
		w.u8(kindAtomicBatch)
	} else {
		w.u8(kindBatchRequest)
	}
	w.u64(id)
	w.u32(uint32(len(ops)))
	for _, op := range ops {
		if err := w.requestBody(op); err != nil {
			return nil, err
		}
	}
	return c.frame(w.buf), nil
}

// Incoming is one decoded client frame.
type Incoming struct {
	Kind  string // "single", "batch", "atomic"
	Req   Request
	Batch []Request
	ID    uint64
}

// DecodeIncoming decodes one payload.
func (c *FrameCodec) DecodeIncoming(payload []byte) (Incoming, error) {
	var in Incoming
	r := &reader{buf: payload}
	kind, err := r.u8()
	if err != nil {
		return in, err
	}
	switch kind {
	case kindRequest:
		req, err := r.requestBody()
		if err != nil {
			return in, err
		}
		if err := r.end(); err != nil {
			return in, err
		}
		return Incoming{Kind: "single", Req: req}, nil
	case kindBatchRequest, kindAtomicBatch:
		id, err := r.u64()
		if err != nil {
			return in, err
		}
		n, err := r.u32()
		if err != nil {
			return in, err
		}
		if n > 4096 {
			return in, decErr("batch too large: %d", n)
		}
		ops := make([]Request, 0, n)
		for i := uint32(0); i < n; i++ {
			op, err := r.requestBody()
			if err != nil {
				return in, err
			}
			ops = append(ops, op)
		}
		if err := r.end(); err != nil {
			return in, err
		}
		k := "batch"
		if kind == kindAtomicBatch {
			k = "atomic"
		}
		return Incoming{Kind: k, Batch: ops, ID: id}, nil
	}
	return in, decErr("unknown incoming kind: %#x", kind)
}

// DecodeResponse decodes one response payload.
func (c *FrameCodec) DecodeResponse(payload []byte) (Response, error) {
	var resp Response
	r := &reader{buf: payload}
	kind, err := r.u8()
	if err != nil {
		return resp, err
	}
	if kind != kindResponse {
		return resp, decErr("wrong payload kind: %#x", kind)
	}
	resp, err = r.responseBody()
	if err != nil {
		return resp, err
	}
	if err := r.end(); err != nil {
		return resp, err
	}
	return resp, nil
}

// DecodeBatchResponse decodes one batch response payload.
func (c *FrameCodec) DecodeBatchResponse(payload []byte) (BatchResponse, error) {
	var b BatchResponse
	r := &reader{buf: payload}
	kind, err := r.u8()
	if err != nil {
		return b, err
	}
	if kind != kindBatchResp {
		return b, decErr("wrong payload kind: %#x", kind)
	}
	id, err := r.u64()
	if err != nil {
		return b, err
	}
	n, err := r.u32()
	if err != nil {
		return b, err
	}
	if n > 4096 {
		return b, decErr("batch response too large: %d", n)
	}
	b.ID = id
	for i := uint32(0); i < n; i++ {
		resp, err := r.responseBody()
		if err != nil {
			return b, err
		}
		b.Results = append(b.Results, resp)
	}
	if err := r.end(); err != nil {
		return b, err
	}
	return b, nil
}

// EncodeBatchResponse frames a batch response (tests/tools).
func (c *FrameCodec) EncodeBatchResponse(b BatchResponse) ([]byte, error) {
	w := &writer{}
	w.u8(kindBatchResp)
	w.u64(b.ID)
	w.u32(uint32(len(b.Results)))
	for _, r := range b.Results {
		if err := w.responseBody(r); err != nil {
			return nil, err
		}
	}
	return c.frame(w.buf), nil
}
