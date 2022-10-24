package utils

import (
	"encoding"
	"fmt"
	"net"
	"reflect"
	"time"
)

// Scan parses bytes `b` to `v` with appropriate type.

//nolint:gocyclo
func Scan(b []byte, v interface{}) (interface{}, error) {

	var err error
	var val interface{}

	switch v := v.(type) {
	case nil:
		err = fmt.Errorf("redis: Scan(nil)")
	case string:
		val = BytesToString(b)
	case []byte:
		val = b
	case int:
		val, err = Atoi(b)
	case int8:
		val, err = ParseInt(b, 10, 8)
	case int16:
		val, err = ParseInt(b, 10, 16)
	case int32:
		val, err = ParseInt(b, 10, 32)
	case int64:
		val, err = ParseInt(b, 10, 64)
	case uint:
		var n uint64
		n, err = ParseUint(b, 10, 64)
		val = uint(n)
	case uint8:
		var n uint64
		n, err = ParseUint(b, 10, 8)
		val = uint8(n)
	case uint16:
		var n uint64
		n, err = ParseUint(b, 10, 16)
		val = uint16(n)
	case uint32:
		var n uint64
		n, err = ParseUint(b, 10, 32)
		val = uint32(n)
	case uint64:
		var n uint64
		n, err = ParseUint(b, 10, 64)
		val = uint64(n)
	case float32:
		var n float64
		n, err = ParseFloat(b, 32)
		val = float32(n)
	case float64:
		var n float64
		n, err = ParseFloat(b, 64)
		val = float64(n)
	case bool:
		val = len(b) == 1 && b[0] == '1'
	case time.Time:
		val, err = time.Parse(time.RFC3339Nano, BytesToString(b))
	case time.Duration:
		var n int64
		n, err = ParseInt(b, 10, 64)
		val = time.Duration(n)
	case encoding.BinaryUnmarshaler:
		err = v.UnmarshalBinary(b)
		val = v
	case net.IP:
		val = b
	default:
		err = fmt.Errorf(
			"redis: can't unmarshal %T (consider implementing BinaryUnmarshaler)", v)
	}
	return val, err
}

func ToNumber(v interface{}) (int64, bool) {
	switch v := v.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return int64(v), true
	case uint:
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		return int64(v), true
	case float32:
		return int64(v), true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}

var numKindMap = map[reflect.Kind]struct{}{
	reflect.Int:     {},
	reflect.Int8:    {},
	reflect.Int16:   {},
	reflect.Int32:   {},
	reflect.Int64:   {},
	reflect.Uint:    {},
	reflect.Uint8:   {},
	reflect.Uint16:  {},
	reflect.Uint32:  {},
	reflect.Uint64:  {},
	reflect.Float32: {},
	reflect.Float64: {},
}

func IsNumber(v interface{}) bool {
	typ := reflect.TypeOf(v)

	kind := typ.Kind()
	_, ok := numKindMap[kind]
	return ok
}
