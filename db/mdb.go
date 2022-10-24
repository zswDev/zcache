package db

import (
	"context"
	"log"
	"time"

	"github.com/qiniu/qmgo"
	"github.com/qiniu/qmgo/operator"
	opts "github.com/qiniu/qmgo/options"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Mdb struct {
	db *qmgo.Database
}

func NewMdb(
	rootKey string,
	client *qmgo.Client,
) *Mdb {

	return &Mdb{
		db: client.Database(rootKey),
	}
}

func (m *Mdb) InitIndex(table string) error {

	unique := true
	background := true
	opt := &options.IndexOptions{
		Unique:     &unique,
		Background: &background,
	}
	key := []string{"id"}
	err := m.db.Collection(table).
		CreateOneIndex(context.Background(),
			opts.IndexModel{
				Key:          key,
				IndexOptions: opt,
			})
	if err != nil {
		return err
	}
	return nil
}

func (m *Mdb) Set(table, id string, out map[string]interface{}) error {

	now := time.Now()
	_out := map[string]interface{}{}
	for k, v := range out {
		_out[k] = v
	}
	_out["updateAt"] = now

	_id := primitive.NewObjectID()
	out["_id"] = _id
	out["createAt"] = now
	out["updateAt"] = now

	err := m.db.Collection(table).
		Find(context.Background(),
			bson.M{
				"id": id,
			}).
		Apply(qmgo.Change{
			Upsert: true,
			Update: bson.M{
				operator.Set:         _out,
				operator.SetOnInsert: out,
			},
		}, nil)
	if err != nil {
		log.Println("obj_insert: ", err)
		return err
	}
	return nil
}

func (m *Mdb) Get(table, id string, info interface{}) error {

	err := m.db.Collection(table).
		Find(context.Background(),
			bson.M{
				"id": id,
			}).
		One(info)
	if err != nil {
		log.Println("obj_find: ", err)
		return err
	}
	return nil
}

func (m *Mdb) Del(table, id string) error {

	err := m.db.Collection(table).
		Remove(context.Background(),
			bson.M{
				"id": id,
			})
	if err != nil {
		log.Println("obj_find: ", err)
		return err
	}
	return nil
}

func (m *Mdb) IncrBy(table, id string, info interface{}, out map[string]interface{}) error {

	now := time.Now()

	err := m.db.Collection(table).
		Find(context.Background(),
			bson.M{
				"id": id,
			}).
		Apply(qmgo.Change{
			ReturnNew: true,
			Update: bson.M{
				operator.Inc: out,
				operator.Set: bson.M{
					"updateAt": now,
				},
			},
		}, info)
	if err != nil {
		log.Println("obj_insert: ", err)
		return err
	}
	return nil
}

func (m *Mdb) Getset(table, id string, info interface{}, out map[string]interface{}) error {

	now := time.Now()
	out["updateAt"] = now

	err := m.db.Collection(table).
		Find(context.Background(),
			bson.M{
				"id": id,
			}).
		Apply(qmgo.Change{
			ReturnNew: true,
			Update: bson.M{
				operator.Set: out,
			},
		}, info)
	if err != nil {
		log.Println("obj_insert: ", err)
		return err
	}
	return nil
}

func (m *Mdb) DelField(table, id string, out map[string]interface{}) error {

	err := m.db.Collection(table).
		UpdateOne(context.Background(),
			bson.M{
				"id": id,
			}, bson.M{
				operator.Unset: out,
			})
	if err != nil {
		log.Println("obj_insert: ", err)
		return err
	}
	return nil
}
