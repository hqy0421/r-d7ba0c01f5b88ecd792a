// Copyright 2026 LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package clean

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"sort"
	"time"
)

// The canonical value encoding below is deterministic: equal native values
// produce identical byte sequences, and the exact Go dynamic type survives a
// round trip. Reproducibility matters here because (a) a source row whose
// content is byte-identical on redelivery must keep its source version
// identity, and (b) revalidation from raw must produce exactly the same rows.
const (
	tagNil byte = iota
	tagBool
	tagInt
	tagInt32
	tagInt64
	tagFloat32
	tagFloat64
	tagString
	tagBytes
	tagArray
	tagMap
	tagTime
	tagRat
)

var errUnsupportedValue = fmt.Errorf("clean: cannot store value")

// encodeValue renders a native Go value decoded by goavro into a canonical,
// deterministic binary representation.
func encodeValue(v interface{}) ([]byte, error) {
	buf := make([]byte, 0, 64)
	return encodeValueTo(buf, v)
}

func encodeValueTo(buf []byte, v interface{}) ([]byte, error) {
	switch t := v.(type) {
	case nil:
		buf = append(buf, tagNil)
	case bool:
		buf = append(buf, tagBool)
		if t {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	case int:
		buf = append(buf, tagInt)
		buf = appendInt64(buf, int64(t))
	case int32:
		buf = append(buf, tagInt32)
		buf = appendInt64(buf, int64(t))
	case int64:
		buf = append(buf, tagInt64)
		buf = appendInt64(buf, t)
	case float32:
		buf = append(buf, tagFloat32)
		buf = appendUint32(buf, math.Float32bits(t))
	case float64:
		buf = append(buf, tagFloat64)
		buf = appendUint64(buf, math.Float64bits(t))
	case string:
		buf = append(buf, tagString)
		buf = appendUvarint(buf, uint64(len(t)))
		buf = append(buf, t...)
	case []byte:
		buf = append(buf, tagBytes)
		buf = appendUvarint(buf, uint64(len(t)))
		buf = append(buf, t...)
	case time.Time:
		buf = append(buf, tagTime)
		u := t.UTC()
		buf = appendInt64(buf, u.Unix())
		buf = appendInt64(buf, int64(u.Nanosecond()))
	case *big.Rat:
		if t == nil {
			buf = append(buf, tagNil)
			break
		}
		buf = append(buf, tagRat)
		num := t.Num().String()
		den := t.Denom().String()
		buf = appendUvarint(buf, uint64(len(num)))
		buf = append(buf, num...)
		buf = appendUvarint(buf, uint64(len(den)))
		buf = append(buf, den...)
	case []interface{}:
		buf = append(buf, tagArray)
		buf = appendUvarint(buf, uint64(len(t)))
		var err error
		for _, item := range t {
			if buf, err = encodeValueTo(buf, item); err != nil {
				return nil, err
			}
		}
	case map[string]interface{}:
		buf = append(buf, tagMap)
		buf = appendUvarint(buf, uint64(len(t)))
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var err error
		for _, k := range keys {
			buf = appendUvarint(buf, uint64(len(k)))
			buf = append(buf, k...)
			if buf, err = encodeValueTo(buf, t[k]); err != nil {
				return nil, err
			}
		}
	default:
		// goavro yields []interface{} and map[string]interface{}, but callers
		// may hand typed slices and maps to a repair. Support those without
		// changing the on-disk format.
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			buf = append(buf, tagArray)
			buf = appendUvarint(buf, uint64(rv.Len()))
			var err error
			for i := 0; i < rv.Len(); i++ {
				if buf, err = encodeValueTo(buf, rv.Index(i).Interface()); err != nil {
					return nil, err
				}
			}
		case reflect.Map:
			if rv.Type().Key().Kind() != reflect.String {
				return nil, fmt.Errorf("%w of type %T", errUnsupportedValue, v)
			}
			buf = append(buf, tagMap)
			buf = appendUvarint(buf, uint64(rv.Len()))
			keys := make([]string, 0, rv.Len())
			for _, k := range rv.MapKeys() {
				keys = append(keys, k.String())
			}
			sort.Strings(keys)
			var err error
			for _, k := range keys {
				buf = appendUvarint(buf, uint64(len(k)))
				buf = append(buf, k...)
				if buf, err = encodeValueTo(buf, rv.MapIndex(reflect.ValueOf(k)).Interface()); err != nil {
					return nil, err
				}
			}
		default:
			return nil, fmt.Errorf("%w of type %T", errUnsupportedValue, v)
		}
	}
	return buf, nil
}

// decodeValue parses one canonical value starting at b and reports how many
// bytes it consumed.
func decodeValue(b []byte) (interface{}, int, error) {
	if len(b) == 0 {
		return nil, 0, fmt.Errorf("clean: truncated stored value")
	}
	tag := b[0]
	rest := b[1:]
	switch tag {
	case tagNil:
		return nil, 1, nil
	case tagBool:
		if len(rest) < 1 {
			return nil, 0, fmt.Errorf("clean: truncated stored bool")
		}
		return rest[0] != 0, 2, nil
	case tagInt:
		v, n, err := readInt64(rest)
		return int(v), 1 + n, err
	case tagInt32:
		v, n, err := readInt64(rest)
		return int32(v), 1 + n, err
	case tagInt64:
		v, n, err := readInt64(rest)
		return v, 1 + n, err
	case tagFloat32:
		v, n, err := readUint32(rest)
		return math.Float32frombits(v), 1 + n, err
	case tagFloat64:
		v, n, err := readUint64(rest)
		return math.Float64frombits(v), 1 + n, err
	case tagString:
		s, n, err := readBlob(rest)
		if err != nil {
			return nil, 0, err
		}
		return string(s), 1 + n, nil
	case tagBytes:
		s, n, err := readBlob(rest)
		if err != nil {
			return nil, 0, err
		}
		return s, 1 + n, nil
	case tagTime:
		sec, n1, err := readInt64(rest)
		if err != nil {
			return nil, 0, err
		}
		nsec, n2, err := readInt64(rest[n1:])
		if err != nil {
			return nil, 0, err
		}
		return time.Unix(sec, nsec).UTC(), 1 + n1 + n2, nil
	case tagRat:
		numBytes, n1, err := readBlob(rest)
		if err != nil {
			return nil, 0, err
		}
		denBytes, n2, err := readBlob(rest[n1:])
		if err != nil {
			return nil, 0, err
		}
		num, ok := new(big.Int).SetString(string(numBytes), 10)
		if !ok {
			return nil, 0, fmt.Errorf("clean: corrupt stored rational numerator")
		}
		den, ok := new(big.Int).SetString(string(denBytes), 10)
		if !ok || den.Sign() == 0 {
			return nil, 0, fmt.Errorf("clean: corrupt stored rational denominator")
		}
		return new(big.Rat).SetFrac(num, den), 1 + n1 + n2, nil
	case tagArray:
		count, n := binary.Uvarint(rest)
		if n <= 0 || count > uint64(len(rest)) {
			return nil, 0, fmt.Errorf("clean: corrupt stored array")
		}
		off := n
		items := make([]interface{}, 0, count)
		for i := uint64(0); i < count; i++ {
			item, used, err := decodeValue(rest[off:])
			if err != nil {
				return nil, 0, err
			}
			items = append(items, item)
			off += used
		}
		return items, 1 + off, nil
	case tagMap:
		count, n := binary.Uvarint(rest)
		if n <= 0 || count > uint64(len(rest)) {
			return nil, 0, fmt.Errorf("clean: corrupt stored map")
		}
		off := n
		out := make(map[string]interface{}, count)
		for i := uint64(0); i < count; i++ {
			rawKey, used, err := readBlob(rest[off:])
			if err != nil {
				return nil, 0, err
			}
			off += used
			value, used, err := decodeValue(rest[off:])
			if err != nil {
				return nil, 0, err
			}
			off += used
			out[string(rawKey)] = value
		}
		return out, 1 + off, nil
	default:
		return nil, 0, fmt.Errorf("clean: unknown stored value tag %d", tag)
	}
}

// decodeStoredValue decodes a complete canonical value and rejects trailing
// bytes.
func decodeStoredValue(b []byte) (interface{}, error) {
	v, n, err := decodeValue(b)
	if err != nil {
		return nil, err
	}
	if n != len(b) {
		return nil, fmt.Errorf("clean: stored value has %d trailing bytes", len(b)-n)
	}
	return v, nil
}

// mergeOverrides overlays patch fields on top of a decoded record.
func mergeOverrides(base interface{}, overrides map[string]interface{}) (interface{}, error) {
	record, ok := base.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("%w: source value is %T, not a record", ErrRepairValue, base)
	}
	merged := make(map[string]interface{}, len(record)+len(overrides))
	for k, v := range record {
		merged[k] = v
	}
	for k, v := range overrides {
		merged[k] = v
	}
	return merged, nil
}

func appendInt64(b []byte, v int64) []byte {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], uint64(v))
	return append(b, tmp[:]...)
}

func appendUint32(b []byte, v uint32) []byte {
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], v)
	return append(b, tmp[:]...)
}

func appendUint64(b []byte, v uint64) []byte {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v)
	return append(b, tmp[:]...)
}

func appendUvarint(b []byte, v uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(b, tmp[:n]...)
}

func readInt64(b []byte) (int64, int, error) {
	if len(b) < 8 {
		return 0, 0, fmt.Errorf("clean: truncated stored integer")
	}
	return int64(binary.LittleEndian.Uint64(b[:8])), 8, nil
}

func readUint32(b []byte) (uint32, int, error) {
	if len(b) < 4 {
		return 0, 0, fmt.Errorf("clean: truncated stored float")
	}
	return binary.LittleEndian.Uint32(b[:4]), 4, nil
}

func readUint64(b []byte) (uint64, int, error) {
	if len(b) < 8 {
		return 0, 0, fmt.Errorf("clean: truncated stored float")
	}
	return binary.LittleEndian.Uint64(b[:8]), 8, nil
}

func readBlob(b []byte) ([]byte, int, error) {
	n, used := binary.Uvarint(b)
	if used <= 0 || n > uint64(len(b)-used) {
		return nil, 0, fmt.Errorf("clean: truncated stored blob")
	}
	return b[used : used+int(n)], used + int(n), nil
}
