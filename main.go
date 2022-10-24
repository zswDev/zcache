package storage

import (
	"zcache/db"
	"zcache/utils"

	"github.com/go-redis/redis/v8"
	"github.com/qiniu/qmgo"
)

type Obj3Cache struct {
	rootKey string
	rdb     *db.Rdb
	mdb     *db.Mdb
	ldb     *db.Ldb
}

var opMap = map[string]struct{}{
	"expired": {},
	"del":     {},
	"hset":    {},
	"hdel":    {},
	"hincrby": {},
}

//
// cpu   => heap  => local => sharedMemory => remoteMemory => remoteDisk
// 0.001%=> 0.01% => 0.1 % => 1%           => 20%          => 80%

func NewObj3Cache(
	rootKey string,
	mgo *qmgo.Client,
	client *redis.ClusterClient,
) *Obj3Cache {

	tmpDir := utils.GetTempDir()

	obj3Cache := &Obj3Cache{
		mdb: db.NewMdb(rootKey, mgo),
		rdb: db.NewRdb(rootKey, client),
		ldb: db.NewLdb(rootKey, tmpDir, 0),
	}

	obj3Cache._initSync()
	obj3Cache._onSync()

	return obj3Cache
}

func (oc *Obj3Cache) _onSync() {

	oc.rdb.OnChange(func(op, key string) {
		_, ok := opMap[key]
		if !ok {
			return
		}
		oc.ldb.Del(key)
	})
}

func (oc *Obj3Cache) _initSync() error {

	allKey := oc.ldb.GetAllKey()
	allExists, err := oc.rdb.Exists(allKey)
	if err != nil {
		return err
	}
	for i := range allKey {
		ok := allExists[i]
		if ok == 0 {
			oc.ldb.Del(allKey[i])
		}
	}
	return nil
}

func (oc *Obj3Cache) _getInfo(info interface{}) (string, map[string]interface{}, error) {

	out, err := utils.Struct2Map("redis", info)
	if err != nil {
		return "", nil, err
	}
	id, _ := out["id"].(string)
	delete(out, "id")

	return id, out, nil
}

func (oc *Obj3Cache) _getKey(id string, info interface{}) (string, string) {

	table := utils.GetTable(info)
	key := utils.Sprintf(oc.rootKey, "/", table, "/", id)
	return table, key
}

func (oc Obj3Cache) Set(info struct{}) error {

	id, out, err := oc._getInfo(info)
	if err != nil {
		return err
	}
	key, table := oc._getKey(id, info)

	err = oc.mdb.Set(table, id, out)
	if err != nil {
		return err
	}
	oc.rdb.Del(key)

	return nil
}

func (oc *Obj3Cache) Get(info interface{}) error {

	id, out, err := oc._getInfo(info)
	if err != nil {
		return err
	}
	key, table := oc._getKey(id, info)

	err = oc.ldb.Get(key, info, out)
	if err == nil {
		return nil
	}
	err = oc.rdb.Get(key, info)
	if err == nil {
		oc.ldb.Set(key, out)
		return nil
	}
	err = oc.mdb.Get(table, id, info)
	if err != nil {
		return nil
	}
	oc.rdb.Set(key, out)

	return nil
}

func (oc *Obj3Cache) Del(info interface{}) error {

	id, _, err := oc._getInfo(info)
	if err != nil {
		return err
	}
	key, table := oc._getKey(id, info)

	err = oc.mdb.Del(table, id)
	if err != nil {
		return nil
	}

	oc.rdb.Del(key)

	return err
}

func (oc *Obj3Cache) IncrBy(info interface{}) error {

	id, out, err := oc._getInfo(info)
	if err != nil {
		return err
	}
	key, table := oc._getKey(id, info)

	newOut := map[string]interface{}{}
	for field, value := range out {
		ok := utils.IsNumber(value)
		if !ok {
			continue
		}
		newOut[field] = value
	}

	err = oc.mdb.IncrBy(table, id, info, newOut)
	if err != nil {
		return nil
	}
	oc.rdb.Del(key)

	return nil
}

func (oc *Obj3Cache) Getset(info interface{}) error {

	id, out, err := oc._getInfo(info)
	if err != nil {
		return err
	}
	key, table := oc._getKey(id, info)

	err = oc.mdb.Getset(table, id, info, out)
	if err != nil {
		return err
	}
	oc.rdb.Del(key)

	return nil
}

func (oc *Obj3Cache) DelField(info interface{}) error {

	id, out, err := oc._getInfo(info)
	if err != nil {
		return err
	}
	key, table := oc._getKey(id, info)

	err = oc.mdb.DelField(table, id, out)
	if err != nil {
		return err
	}
	oc.rdb.Del(key)

	return nil
}
