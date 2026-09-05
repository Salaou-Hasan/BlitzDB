// Package blitzdb is the reference Go SDK for BlitzDB (stdlib only).
//
// Value model: Go's native integers map 1:1 to wire tags (int8→0x02 …
// uint64→0x09); plain int becomes Int64, float64 becomes Float64.
// UUID-shaped strings encode as Uuid and decode back identically.
// time.Time ⇄ Timestamp (ms precision); {"$date":"YYYY-MM-DD"} ⇄ Date;
// {"$decimal":s} ⇄ Decimal; {"$uuid":s} ⇄ Uuid; {"$bytes":"<base64>"} ⇄
// Bytes; {"$ts":micros} ⇄ Timestamp; {"$i32"/"$u32"/"$i64"/"$u64"/"$f32"}
// force exact widths (the server validates column types strictly).
// Any other map[string]any is Json; []any is Array.
package blitzdb

import (
	"encoding/base64"
	"fmt"
	"regexp"
)

// Row is one result row with its (possibly shard-routed) global id.
type Row struct {
	ID     uint64
	Values map[string]any
}

var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// IsUUIDShaped reports whether s encodes as Uuid on the wire.
func IsUUIDShaped(s string) bool { return uuidRe.MatchString(s) }

func uuidToBytes(s string) ([16]byte, error) {
	var out [16]byte
	hex := make([]byte, 0, 32)
	for i := 0; i < len(s); i++ {
		if s[i] == '-' {
			continue
		}
		hex = append(hex, s[i])
	}
	if len(hex) != 32 {
		return out, fmt.Errorf("bad uuid: %s", s)
	}
	for i := 0; i < 16; i++ {
		hi := fromHex(hex[2*i])
		lo := fromHex(hex[2*i+1])
		if hi > 15 || lo > 15 {
			return out, fmt.Errorf("bad uuid: %s", s)
		}
		out[i] = hi<<4 | lo
	}
	return out, nil
}

func fromHex(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	}
	return 255
}

func bytesToUUID(b []byte) string {
	const hexd = "0123456789abcdef"
	var dst [36]byte
	pos := 0
	for i, c := range b {
		if i == 4 || i == 6 || i == 8 || i == 10 {
			dst[pos] = '-'
			pos++
		}
		dst[pos] = hexd[c>>4]
		dst[pos+1] = hexd[c&15]
		pos += 2
	}
	return string(dst[:])
}

// daysFromCivil matches chrono's num_days_from_ce (proleptic Gregorian).
func daysFromCivil(y, m, d int) int32 {
	yAdj := y
	if m <= 2 {
		yAdj--
	}
	era := yAdj / 400
	yoe := yAdj - era*400
	mp := (m + 9) % 12
	doy := (153*mp+2)/5 + d - 1
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	return int32(era*146097 + doe - 719468 + 719162)
}

func civilFromDays(z int32) (y, m, d int) {
	z2 := int(z) - 719162 + 719468
	era := z2 / 146097
	doe := z2 - era*146097
	yoe := (doe - doe/1460 + doe/36524 - doe/146096) / 365
	y = yoe + era*400
	doy := doe - (365*yoe + yoe/4 - yoe/100)
	mp := (5*doy + 2) / 153
	d = doy - (153*mp+2)/5 + 1
	if mp < 10 {
		m = mp + 3
	} else {
		m = mp - 9
	}
	if m <= 2 {
		y++
	}
	return y, m, d
}

func decodeBase64(s string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(s)
}

func encodeBase64(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}
