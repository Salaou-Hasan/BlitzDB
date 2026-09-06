package blitzdb

import (
	"bytes"
	"testing"
	"time"
)

func roundValue(t *testing.T, v any) any {
	t.Helper()
	c := NewFrameCodec()
	req := Request{ID: 1, Op: OpInsert, Table: "t", Values: map[string]any{"v": v}}
	f, err := c.EncodeRequest(req)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	payloads, err := c.Feed(f)
	if err != nil || len(payloads) != 1 {
		t.Fatalf("feed: %v %d", err, len(payloads))
	}
	back, err := c.DecodeIncoming(payloads[0])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if back.Kind != "single" {
		t.Fatalf("kind: %s", back.Kind)
	}
	return back.Req.Values["v"]
}

func TestValueScalars(t *testing.T) {
	if got := roundValue(t, nil); got != nil {
		t.Fatalf("null: %v", got)
	}
	if got := roundValue(t, true); got != true {
		t.Fatalf("bool: %v", got)
	}
	if got := roundValue(t, int64(42)); got != int64(42) {
		t.Fatalf("int64: %v", got)
	}
	if got := roundValue(t, 1.5); got != 1.5 {
		t.Fatalf("float: %v", got)
	}
	if got := roundValue(t, int32(-7)); got != int32(-7) {
		t.Fatalf("int32: %#v", got)
	}
	if got := roundValue(t, uint32(4000000000)); got != uint32(4000000000) {
		t.Fatalf("uint32: %#v", got)
	}
	if got := roundValue(t, uint64(1)<<63); got != uint64(1)<<63 {
		t.Fatalf("uint64: %#v", got)
	}
}

func TestValueStringsUUIDBytesDate(t *testing.T) {
	if got := roundValue(t, "hello"); got != "hello" {
		t.Fatalf("string: %v", got)
	}
	uid := "123e4567-e89b-12d3-a456-426614174000"
	if got := roundValue(t, uid); got != uid {
		t.Fatalf("uuid: %v", got)
	}
	if got := roundValue(t, map[string]any{"$uuid": uid}); got != uid {
		t.Fatalf("$uuid: %v", got)
	}
	if got := roundValue(t, []byte{1, 2, 250}); !bytes.Equal(got.([]byte), []byte{1, 2, 250}) {
		t.Fatalf("bytes: %v", got)
	}
	if got := roundValue(t, map[string]any{"$decimal": "12.50"}); !mapsEqual(got, map[string]any{"$decimal": "12.50"}) {
		t.Fatalf("decimal: %v", got)
	}
	if got := roundValue(t, map[string]any{"$date": "2026-09-05"}); !mapsEqual(got, map[string]any{"$date": "2026-09-05"}) {
		t.Fatalf("date: %v", got)
	}
	ts := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	if got := roundValue(t, ts); !got.(time.Time).Equal(ts) {
		t.Fatalf("timestamp: %v", got)
	}
}

func mapsEqual(a, b any) bool {
	ma, ok1 := a.(map[string]any)
	mb, ok2 := b.(map[string]any)
	if !ok1 || !ok2 || len(ma) != len(mb) {
		return false
	}
	for k, v := range ma {
		if mb[k] != v {
			return false
		}
	}
	return true
}

func TestValueContainers(t *testing.T) {
	got := roundValue(t, []any{int64(1), "a", nil, true}).([]any)
	if len(got) != 4 || got[0] != int64(1) || got[1] != "a" || got[2] != nil || got[3] != true {
		t.Fatalf("array: %#v", got)
	}
	gotm := roundValue(t, map[string]any{"a": int64(1), "b": "x"}).(map[string]any)
	if gotm["a"] != int64(1) || gotm["b"] != "x" {
		t.Fatalf("json: %#v", gotm)
	}
}

func TestFrameKinds(t *testing.T) {
	c := NewFrameCodec()
	id := uint64(7)
	rid := uint64(42)
	req := Request{ID: id, Op: OpGet, Table: "users", RowID: &rid}
	f, err := c.EncodeRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	if f[5] != kindRequest {
		t.Fatalf("kind: %#x", f[5])
	}
	payloads, err := c.Feed(f)
	if err != nil || len(payloads) != 1 {
		t.Fatal("feed")
	}
	back, err := c.DecodeIncoming(payloads[0])
	if err != nil || back.Kind != "single" || back.Req.RowID == nil || *back.Req.RowID != 42 {
		t.Fatalf("back: %+v %v", back, err)
	}
	batch := []Request{req, {ID: 8, Op: OpGet, Table: "users"}}
	for _, atomic := range []bool{false, true} {
		bf, err := c.EncodeBatch(9, batch, atomic)
		if err != nil {
			t.Fatal(err)
		}
		want := byte(kindBatchRequest)
		if atomic {
			want = kindAtomicBatch
		}
		if bf[5] != want {
			t.Fatalf("batch kind: %#x", bf[5])
		}
		pl, err := c.Feed(bf)
		if err != nil || len(pl) != 1 {
			t.Fatal("batch feed")
		}
		bb, err := c.DecodeIncoming(pl[0])
		if err != nil {
			t.Fatal(err)
		}
		wantKind := "batch"
		if atomic {
			wantKind = "atomic"
		}
		if bb.Kind != wantKind || len(bb.Batch) != 2 {
			t.Fatalf("batch back: %+v", bb)
		}
	}
}

func TestFeedSplitAndVersion(t *testing.T) {
	c := NewFrameCodec()
	f, err := c.EncodeRequest(Request{ID: 1, Op: OpPing})
	if err != nil {
		t.Fatal(err)
	}
	if pl, err := c.Feed(f[:3]); err != nil || len(pl) != 0 {
		t.Fatalf("split: %v %d", err, len(pl))
	}
	if pl, err := c.Feed(f[3:]); err != nil || len(pl) != 1 {
		t.Fatalf("rest: %v %d", err, len(pl))
	}
	bad := append([]byte(nil), f...)
	bad[0] = 0x7F
	if _, err := NewFrameCodec().Feed(bad); err == nil {
		t.Fatal("version accepted")
	}
}

func TestBatchResponse(t *testing.T) {
	c := NewFrameCodec()
	in := BatchResponse{ID: 99, Results: []Response{
		{ID: 1, OK: true, Rows: []Row{{ID: 5, Values: map[string]any{"name": "Ada"}}}},
		{ID: 2, OK: false, Error: "gone"},
	}}
	f, err := c.EncodeBatchResponse(in)
	if err != nil {
		t.Fatal(err)
	}
	if f[5] != kindBatchResp {
		t.Fatalf("kind: %#x", f[5])
	}
	pl, err := c.Feed(f)
	if err != nil || len(pl) != 1 {
		t.Fatal("feed")
	}
	back, err := c.DecodeBatchResponse(pl[0])
	if err != nil {
		t.Fatal(err)
	}
	if back.ID != 99 || len(back.Results) != 2 || !back.Results[0].OK || back.Results[1].OK {
		t.Fatalf("back: %+v", back)
	}
	if back.Results[0].Rows[0].Values["name"] != "Ada" {
		t.Fatalf("row: %+v", back.Results[0].Rows[0])
	}
}

func TestCompatMatrix(t *testing.T) {
	if err := CheckCompat("0.1.0", 2); err != nil {
		t.Fatalf("happy: %v", err)
	}
	if err := CheckCompat("1.4.2", 2); err != nil {
		t.Fatalf("happy: %v", err)
	}
	if err := CheckCompat("0.1.0", 3); err == nil {
		t.Fatal("protocol skew must fail")
	} else if se, ok := err.(*SdkError); !ok || se.Kind != ErrServer {
		t.Fatalf("kind: %v", err)
	}
	if err := CheckCompat("0.0.9", 2); err == nil {
		t.Fatal("old server must fail")
	}
	if err := CheckCompat("banana", 2); err == nil {
		t.Fatal("garbage must fail")
	}
	if v, ok := parseVersion("1.2.3"); !ok || v != [3]uint64{1, 2, 3} {
		t.Fatalf("parse: %v %v", v, ok)
	}
}
