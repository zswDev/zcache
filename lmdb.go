package storage

import (
	"fmt"
	"strings"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/savsgio/gotils/strconv"
)

const (
	__size       = 1024 * 1024 * 128 //128m
	__maxDBs     = 16
	__maxReaders = 1024 * __maxDBs
	__flags      = lmdb.NoMetaSync | lmdb.NoSync | lmdb.MapAsync | lmdb.WriteMap
	__mode       = 0600
	__key        = "key"
	__field      = "field"
)

type Ldb struct {
	tmpDir   string
	rootKey  string
	size     int64
	env      *lmdb.Env
	fieldDbi lmdb.DBI
	keyDbi   lmdb.DBI
}

func NewLdb(rootKey, tmpDir string, size int64) *Ldb {

	if size <= 0 {
		size = __size
	}
	ldb := &Ldb{
		rootKey: rootKey,
		tmpDir:  tmpDir,
		size:    size,
	}
	ldb._init()
	return ldb
}

func (ld *Ldb) _init() {

	var err error
	ld.env, err = lmdb.NewEnv()
	if err != nil {
		panic("lmdb_init_err	" + err.Error())
	}
	err = ld.env.SetMaxDBs(__maxDBs)
	if err != nil {
		panic("lmdb_init_err	" + err.Error())
	}
	err = ld.env.SetMapSize(ld.size)
	if err != nil {
		panic("lmdb_init_err	" + err.Error())
	}
	ld.env.SetMaxReaders(__maxReaders)

	err = ld.env.Open(ld.tmpDir, __flags, __mode)
	if err != nil {
		panic("lmdb_init_err	" + err.Error())
	}

	err = ld.env.Sync(false)
	if err != nil {
		panic("lmdb_init_err	" + err.Error())
	}

	err = ld.env.Update(func(txn *lmdb.Txn) error {
		ld.keyDbi, err = txn.OpenDBI(Sprintf(__key, "/", ld.rootKey), lmdb.Create)
		if err != nil {
			return err
		}
		ld.fieldDbi, err = txn.OpenDBI(Sprintf(__field, "/", ld.rootKey), lmdb.Create)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic("lmdb_init_err	" + err.Error())
	}
}

func (ld *Ldb) GetAllKey() []string {

	allKey := []string{}
	ld.env.View(func(txn *lmdb.Txn) error {

		cursor, err := txn.OpenCursor(ld.keyDbi)
		if err != nil {
			return nil
		}
		defer cursor.Close()

		for {
			k, _, err := cursor.Get(nil, nil, lmdb.Next)
			if lmdb.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
			key := BytesToString(k)
			allKey = append(allKey, key)
		}
	})
	return allKey
}

func (ld *Ldb) _get(txn *lmdb.Txn, key, field string, value interface{}) (interface{}, error) {

	tag := strconv.S2B(Sprintf(key, "/", field))
	bytes, err := txn.Get(ld.fieldDbi, tag)
	if err != nil {
		return nil, err
	}
	if len(bytes) == 0 {
		return nil, fmt.Errorf("not data")
	}
	return Scan(bytes, value)
}

func (ld *Ldb) _set(txn *lmdb.Txn, key, field string, value interface{}) error {

	bytes := WriteArg(value)
	tag := strconv.S2B(Sprintf(key, "/", field))
	return txn.Put(ld.fieldDbi, tag, bytes, 0)
}

func (ld *Ldb) _del(txn *lmdb.Txn, key, field string) error {

	tag := strconv.S2B(Sprintf(key, "/", field))
	return txn.Del(ld.fieldDbi, tag, nil)
}

func (ld *Ldb) _getFieldMap(txn *lmdb.Txn, key string) (map[string]struct{}, error) {

	fieldMap := map[string]struct{}{}
	keyBit := strconv.S2B(key)
	fieldsBit, err := txn.Get(ld.keyDbi, keyBit)
	if !lmdb.IsNotFound(err) {
		fields := strings.Split(BytesToString(fieldsBit), ",")
		for _, field := range fields {
			fieldMap[field] = struct{}{}
		}
	}
	return fieldMap, nil
}

func (ld *Ldb) _setFieldMap(txn *lmdb.Txn, key string, fieldMap map[string]struct{}) error {

	keyBit := StringToBytes(key)
	fields := []string{}
	for field := range fieldMap {
		fields = append(fields, field)
	}
	fieldsStr := strings.Join(fields, ",")
	return txn.Put(ld.keyDbi, keyBit, StringToBytes(fieldsStr), 0)
}

func (ld *Ldb) _delFieldMap(txn *lmdb.Txn, key string) error {

	keyBit := strconv.S2B(key)
	return txn.Del(ld.keyDbi, keyBit, nil)
}

func (ld *Ldb) Set(key string, out map[string]interface{}) (err error) {

	return ld.env.Update(func(txn *lmdb.Txn) error {

		fieldMap, err := ld._getFieldMap(txn, key)
		if err != nil {
			return err
		}
		for field, value := range out {
			err := ld._set(txn, key, field, value)
			if err != nil {
				return err
			}
			fieldMap[field] = struct{}{}
		}
		return ld._setFieldMap(txn, key, fieldMap)
	})
}

func (ld *Ldb) Get(key string, info interface{}, out map[string]interface{}) error {

	return ld.env.View(func(txn *lmdb.Txn) error {
		txn.RawRead = true
		for field, value := range out {
			_value, _err := ld._get(txn, key, field, value)
			if _err != nil {
				return _err
			}
			out[field] = _value
		}
		return Map2Struct("redis", out, info)
	})
}

func (ld *Ldb) Del(key string) (err error) {

	return ld.env.Update(func(txn *lmdb.Txn) error {

		fieldMap, err := ld._getFieldMap(txn, key)
		if err != nil {
			return err
		}
		for field := range fieldMap {
			err := ld._del(txn, key, field)
			if err != nil {
				return err
			}
		}
		err = ld._delFieldMap(txn, key)
		if err != nil {
			return err
		}
		return nil
	})
}

// func (ld *Ldb) IncrBy(key, field string, incr int64) (ret int64, err error) {

// 	err = ld.env.Update(func(txn *lmdb.Txn) error {

// 		fieldMap, err := ld._getFieldMap(txn, key)
// 		if err != nil {
// 			return err
// 		}

// 		_value, err := ld._get(txn, key, field, incr)
// 		if err != nil {
// 			return err
// 		}
// 		number, ok := ToNumber(_value)
// 		if !ok {
// 			return fmt.Errorf("not number")
// 		}
// 		number += incr
// 		err = ld._set(txn, key, field, number)
// 		if err != nil {
// 			return err
// 		}
// 		ret = number

// 		fieldMap[field] = struct{}{}
// 		return ld._setFieldMap(txn, key, fieldMap)
// 	})
// 	return ret, err
// }

// func (ld *Ldb) Getset(key, field string, value interface{}) (ret interface{}, err error) {

// 	err = ld.env.Update(func(txn *lmdb.Txn) error {

// 		fieldMap, err := ld._getFieldMap(txn, key)
// 		if err != nil {
// 			return err
// 		}
// 		ret, err = ld._get(txn, key, field, value)
// 		if err != nil {
// 			return err
// 		}
// 		err = ld._set(txn, key, field, value)
// 		if err != nil {
// 			return err
// 		}
// 		fieldMap[field] = struct{}{}
// 		return ld._setFieldMap(txn, key, fieldMap)
// 	})
// 	return ret, err
// }

// func (ld *Ldb) DelField(key string, out map[string]interface{}) (err error) {

// 	err = ld.env.Update(func(txn *lmdb.Txn) error {

// 		fieldMap, err := ld._getFieldMap(txn, key)
// 		if err != nil {
// 			return err
// 		}
// 		for field := range out {
// 			err := ld._del(txn, key, field)
// 			if err != nil {
// 				return err
// 			}
// 			delete(fieldMap, field)
// 		}
// 		return ld._setFieldMap(txn, key, fieldMap)
// 	})
// 	return err
// }
