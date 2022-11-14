package main

import (
	"fmt"
	"strings"

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
	strs := strings.SplitN("abc/abc", "/", 2)
	fmt.Println(strs)
	testLdb()
}
