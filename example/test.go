package main

import (
	"github.com/qiniu/qmgo/field"
)

type SliceMock struct {
	field.DefaultField `redis:"-"`
	Len                int    `redis:"len"`
	Cap                int    `redis:"cap"`
	Id                 string `redis:"id"`
	Other              string `redis:"other"`
}

func testLdb() {

}

func main() {
	testLdb()
}
