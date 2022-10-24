package utils

import (
	"encoding"
	"net"
	"strconv"
	"time"
)

func WriteArg(v interface{}) []byte {
	switch v := v.(type) {
	case nil:
		return _string("")
	case string:
		return _string(v)
	case []byte:
		return v
	case int:
		return _int(int64(v))
	case int8:
		return _int(int64(v))
	case int16:
		return _int(int64(v))
	case int32:
		return _int(int64(v))
	case int64:
		return _int(v)
	case uint:
		return _uint(uint64(v))
	case uint8:
		return _uint(uint64(v))
	case uint16:
		return _uint(uint64(v))
	case uint32:
		return _uint(uint64(v))
	case uint64:
		return _uint(v)
	case float32:
		return _float(float64(v))
	case float64:
		return _float(v)
	case bool:
		if v {
			return _int(1)
		}
		return _int(0)
	case time.Time:
		return v.AppendFormat([]byte{}, time.RFC3339Nano)
	case time.Duration:
		return _int(v.Nanoseconds())
	case encoding.BinaryMarshaler:
		b, err := v.MarshalBinary()
		if err != nil {
			return nil
		}
		return b
	case net.IP:
		return v
	default:
		return nil
	}
}

func _string(s string) []byte {
	return StringToBytes(s)
}

func _uint(n uint64) []byte {
	return strconv.AppendUint([]byte{}, n, 10)
}

func _int(n int64) []byte {
	return strconv.AppendInt([]byte{}, n, 10)
}

func _float(f float64) []byte {
	return strconv.AppendFloat([]byte{}, f, 'f', -1, 64)
}
