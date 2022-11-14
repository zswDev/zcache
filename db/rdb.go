package db

import (
	"context"
	"log"
	"strings"
	"time"
	"zcache/utils"

	"github.com/go-redis/redis/v8"
)

const (
	__expire = time.Second * 60 * 60 * 24 * 10
	__topic  = "__keyspace@0__:"
	__notify = "notify-keyspace-events"
	__events = "eghKx"

	__getset_lua = `
local q_key = tostring(ARGV[1])
local q_field = tostring(ARGV[2])
local q_value = ARGV[3]

local value = redis.call('hget', q_key, q_field)
redis.call('hset', q_key, q_field, q_value)
return value`
)

type Rdb struct {
	db        *redis.ClusterClient
	getsetSha string
	topic     string
}

func NewRdb(
	rootKey string,
	client *redis.ClusterClient,
) *Rdb {

	rdb := &Rdb{
		db:    client,
		topic: utils.Sprintf(__topic, rootKey, "*"),
	}
	rdb.getsetSha, _ = rdb._loadLua(__getset_lua)

	return rdb
}

func (r *Rdb) _loadLua(code string) (string, error) {

	script := redis.NewScript(code)
	sha, err := script.Load(context.Background(), r.db).Result()
	if err != nil {
		log.Println("err_load", err)
		return "", nil
	}
	return sha, nil
}

func (r *Rdb) _evalLua(sha string, keys []string, args ...interface{}) interface{} {

	result, err := r.db.EvalSha(context.Background(), sha, keys, args...).Result()
	if err != nil {
		log.Println("err_eval	", err)
		return nil
	}
	return result
}

func (r *Rdb) OnChange(cb func(op, key string)) {

	r.db.ForEachMaster(context.Background(), func(ctx context.Context, client *redis.Client) error {

		_, err := client.ConfigSet(context.Background(), __notify, __events).Result()
		if err != nil {
			return err
		}
		sub := client.PSubscribe(context.Background(), r.topic)
		_, err = sub.Receive(context.Background())
		if err != nil {
			log.Println("Receive	", err)
			return err
		}

		for msg := range sub.Channel() {

			keys := strings.Split(msg.Channel, __topic)
			if len(keys) != 2 {
				continue
			}
			key := keys[1]

			cb(msg.Payload, key)
		}
		return nil
	})
}

func (r *Rdb) Exists(allKeys [][]string) ([]int64, error) {

	ctx := context.Background()
	pipe := r.db.Pipeline()
	for _, keys := range allKeys {
		pipe.HExists(ctx, keys[0], keys[1])
	}
	cmds, err := pipe.Exec(context.Background())
	if err != nil {
		return nil, err
	}
	allExists := make([]int64, len(allKeys))
	for i, cmd := range cmds {
		ret, ok := cmd.(*redis.IntCmd)
		if !ok {
			continue
		}
		exists, err := ret.Result()
		if err != nil {
			return nil, err
		}
		allExists[i] = exists
	}
	return allExists, nil
}

func (r *Rdb) Set(key string, out map[string]interface{}) error {

	ctx := context.Background()
	pipe := r.db.Pipeline()
	pipe.HSet(ctx, key, out)
	pipe.Expire(ctx, key, __expire)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (r *Rdb) Get(key string, info interface{}) error {

	ret := r.db.HGetAll(context.Background(), key)
	data, err := ret.Result()
	if len(data) == 0 || (err != nil && err != redis.Nil) {
		return err
	}
	err = ret.Scan(info)
	if err != nil {
		return err
	}
	return nil
}

func (r *Rdb) Del(key string) error {

	_, err := r.db.Del(context.Background(), key).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	return nil
}

func (r *Rdb) DelField(key string, info interface{}, out map[string]interface{}) error {

	ctx := context.Background()
	pipe := r.db.Pipeline()
	for field := range out {
		pipe.HDel(ctx, key, field)
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		log.Println("obj_set	", err)
		return err
	}
	return nil
}

//只对一个int64字段 原子加
func (r *Rdb) IncrBy(key string, out map[string]interface{}) (int64, error) {

	for field, value := range out {

		number, ok := utils.ToNumber(value)
		if !ok {
			continue
		}
		ret, err := r.db.HIncrBy(context.Background(), key, field, number).Result()
		if err != nil {
			log.Println("obj_incr	", err)
			return 0, err
		}
		return ret, nil
	}
	return 0, nil
}

func (r *Rdb) GetSet(key string, info interface{}, out map[string]interface{}) (interface{}, error) {

	for field, value := range out {

		ret := r._evalLua(
			r.getsetSha,
			nil,
			key,
			field,
			value)
		return ret, nil
	}
	return nil, nil
}
