package db

import (
	"fmt"
	"strings"
	"zcache/utils"

	"github.com/bmatsuo/lmdb-go/lmdb"
	"github.com/savsgio/gotils/strconv"
)

const (
	__size       = 1024 * 1024 * 128 //128m
	__maxDBs     = 16
	__maxReaders = 1024 * __maxDBs
	__flags      = lmdb.NoMetaSync | lmdb.NoSync | lmdb.MapAsync | lmdb.WriteMap
	__mode       = 0600
)

type Ldb struct {
	tmpDir  string
	rootKey string
	size    int64
	env     *lmdb.Env
	dbi     lmdb.DBI
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
		ld.dbi, err = txn.OpenDBI(ld.rootKey, lmdb.Create)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic("lmdb_init_err	" + err.Error())
	}
}

func (ld *Ldb) GetAllKeys() [][]string {

	allKeys := [][]string{}
	ld.env.View(func(txn *lmdb.Txn) error {

		cursor, err := txn.OpenCursor(ld.dbi)
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
			data := utils.BytesToString(k)
			keys := strings.Split(data, "/")
			allKeys = append(allKeys, []string{keys[0], keys[1]})
		}
	})
	return allKeys
}

func (ld *Ldb) _get(txn *lmdb.Txn, key, field string, value interface{}) (interface{}, error) {

	tag := strconv.S2B(utils.Sprintf(key, "/", field))
	bytes, err := txn.Get(ld.dbi, tag)
	if err != nil {
		return nil, err
	}
	if len(bytes) == 0 {
		return nil, fmt.Errorf("not data")
	}
	return utils.Scan(bytes, value)
}

func (ld *Ldb) _set(txn *lmdb.Txn, key, field string, value interface{}) error {

	bytes := utils.WriteArg(value)
	tag := strconv.S2B(utils.Sprintf(key, "/", field))
	return txn.Put(ld.dbi, tag, bytes, 0)
}

func (ld *Ldb) _del(txn *lmdb.Txn, key, field string) error {

	tag := strconv.S2B(utils.Sprintf(key, "/", field))
	return txn.Del(ld.dbi, tag, nil)
}

func (ld *Ldb) Set(key string, out map[string]interface{}) (err error) {

	return ld.env.Update(func(txn *lmdb.Txn) error {
		for field, value := range out {
			err := ld._set(txn, key, field, value)
			if err != nil {
				return err
			}
		}
		return nil
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
		return utils.Map2Struct("redis", out, info)
	})
}

func (ld *Ldb) MatchAllKeys(key string) [][]byte {

	allKeys := [][]byte{}
	ld.env.View(func(txn *lmdb.Txn) error {

		cursor, err := txn.OpenCursor(ld.dbi)
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
			data := utils.BytesToString(k)
			if strings.HasPrefix(data, key) {
				allKeys = append(allKeys, k)
			}
		}
	})
	return allKeys
}

func (ld *Ldb) Del(key string, field string) (err error) {

	if field != "" {
		return ld.env.Update(func(txn *lmdb.Txn) error {
			err := ld._del(txn, key, field)
			if err != nil {
				return err
			}
			return nil
		})
	}
	allkeys := ld.MatchAllKeys(key)
	return ld.env.Update(func(txn *lmdb.Txn) error {
		for _, key := range allkeys {
			err := txn.Del(ld.dbi, key, nil)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (ld *Ldb) IncrBy(key, field string, incr int64) (ret int64, err error) {

	err = ld.env.Update(func(txn *lmdb.Txn) error {
		_value, err := ld._get(txn, key, field, incr)
		if err != nil {
			return err
		}
		number, ok := utils.ToNumber(_value)
		if !ok {
			return fmt.Errorf("not number")
		}
		number += incr
		err = ld._set(txn, key, field, number)
		if err != nil {
			return err
		}
		ret = number
		return nil
	})
	return ret, err
}

func (ld *Ldb) Getset(key, field string, value interface{}) (ret interface{}, err error) {

	err = ld.env.Update(func(txn *lmdb.Txn) error {
		ret, err = ld._get(txn, key, field, value)
		if err != nil {
			return err
		}
		err = ld._set(txn, key, field, value)
		if err != nil {
			return err
		}
		return nil
	})
	return ret, err
}

func (ld *Ldb) DelField(key string, out map[string]interface{}) (err error) {

	err = ld.env.Update(func(txn *lmdb.Txn) error {
		for field := range out {
			err := ld._del(txn, key, field)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}
