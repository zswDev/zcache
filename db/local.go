package db

import (
	"unsafe"
)

const (
	__total     = (1 << 4) - 1
	__chunkSize = 8
	__chunkLen  = 2
	__lenSize   = 2
)

type Local struct {
	chunk     []byte
	indexs    []byte //1k
	chunkSize int
	chunkLen  int
	total     int
	lenSize   int
	//threadLocal map[uint64][]byte
}

//node
//1024byte=1kb
//max 4m

// k=4b
// v=1024b
// value max length = 4k

// TODO 后续加入idle索引每个空chunk, index索引用了那些chunk,做个最高上线
// TODO 先分配一个 4k page, 然后把4k page, 拆分成 8bytes, 16bytes的 slab

func NewLocal() *Local {

	chunk := make([]byte, __total*__chunkSize*__chunkLen)
	indexs := make([]byte, __total*__chunkSize*1)

	return &Local{
		chunk:     chunk,
		indexs:    indexs,
		chunkSize: __chunkSize,
		chunkLen:  __chunkLen,
		total:     __total,
		lenSize:   __lenSize,
	}
}

func hash(key string) int {
	hash := 0
	for i := 0; i < len(key); i++ {
		hash = 31*hash + int(key[i])
	}
	return hash
}

func (l *Local) existsKey(key string) (int, bool) {

	klen := len(key)
	if klen+l.lenSize > l.chunkSize {
		return 0, false
	}

	offset := hash(key) % l.total
	thatKlen := bytes2int(l.indexs, offset)

	keyStart := offset*l.chunkSize + l.lenSize
	keyEnd := keyStart + klen
	thatKey := BytesToString(l.indexs[keyStart:keyEnd])

	//fmt.Println(thatKey, thatKlen, keyStart, keyEnd)
	if thatKlen != 0 && thatKey != key { //已有了, 不做冲突处理
		return 0, false
	}
	return offset, true
}

func int2bytes(bytes []byte, offset, v int) {
	bytes[offset] = byte((v >> 8) & 0xff) //高位
	offset++
	bytes[offset] = byte(v & 0xff) //低位
}

func bytes2int(bytes []byte, offset int) int {
	height := int(bytes[offset]) << 8 //高位
	offset++
	low := int(bytes[offset]) //低位
	v := height + low
	return v
}

type callback = func(start, end, chunkStart, chunkEnd int)

func (l *Local) next(offset, vlen int, cb callback) {

	start := 0
	end := l.chunkSize - l.lenSize
	chunkStart := offset*l.chunkSize + l.lenSize
	chunkEnd := (offset + 1) * l.chunkSize

	//fmt.Println(start, end, 1111)
	//fmt.Println(chuankStart, chuankEnd, 2222)

	for i := 0; i < l.chunkLen; i++ {

		if start >= vlen {
			break
		}
		if start < vlen && end >= vlen {
			end = vlen
		}

		//fmt.Println("vlen", start, end, value[start:end])
		//fmt.Println("chunk", chuankStart, chuankEnd, l.chunk[chuankStart:chuankEnd])
		//copy(l.chunk[chunkStart:chunkEnd], value[start:end])
		cb(start, end, chunkStart, chunkEnd)

		start = end
		end = start + l.chunkSize
		chunkStart = chunkEnd
		chunkEnd = chunkStart + l.chunkSize
	}
}

func (l *Local) Set(key string, value []byte) {
	//gid := l.getGid()

	offset, ok := l.existsKey(key)
	if !ok {
		return
	}
	//fmt.Println("key", offset, ok)

	vlen := len(value)
	if vlen+2 > l.chunkLen*l.chunkSize {
		return
	}

	klen := len(key)
	int2bytes(l.indexs, offset*l.chunkSize, klen)
	l.next(offset, klen, func(start, end, chunkStart, chunkEnd int) {
		copy(l.indexs[chunkStart:chunkEnd], key[start:end])
	})

	int2bytes(l.chunk, offset*l.chunkSize, vlen)

	// fmt.Println("vlen", offset*l.chunkLen, offset)
	// fmt.Println("v", l.chunk)

	l.next(offset, vlen, func(start, end, chunkStart, chunkEnd int) {
		//fmt.Println("get value", start, end)
		//fmt.Println("set chunk", chunkStart, chunkEnd)
		copy(l.chunk[chunkStart:chunkEnd], value[start:end])
	})

}

func (l *Local) Get(key string) []byte {
	//gid := l.getGid()

	offset, ok := l.existsKey(key)
	if !ok {
		return nil
	}
	//fmt.Println(offset, ok)
	vlen := 0
	vlen = bytes2int(l.chunk, offset*l.chunkSize)

	//fmt.Println(vlen)

	value := make([]byte, vlen)

	l.next(offset, vlen, func(start, end, chunkStart, chunkEnd int) {
		//fmt.Println("set value", start, end)
		//fmt.Println("get chunk", chunkStart, chunkEnd)
		copy(value[start:end], l.chunk[chunkStart:chunkEnd])
	})

	return value
}

func (l *Local) Del(key string) {
	offset, ok := l.existsKey(key)
	if !ok {
		return
	}
	//fmt.Println(offset, ok)
	vlen := 0
	vlen = bytes2int(l.chunk, offset*l.chunkSize)

	int2bytes(l.chunk, offset, 0)

	//fmt.Println(vlen)

	l.next(offset, vlen, func(start, end, chunkStart, chunkEnd int) {
		//fmt.Println("set value", start, end)
		//fmt.Println("get chunk", chunkStart, chunkEnd)
		for i := chunkStart; i < chunkStart; i++ {
			l.chunk[chunkStart] = 0
		}
	})
}

// BytesToString converts byte slice to string.
func BytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
