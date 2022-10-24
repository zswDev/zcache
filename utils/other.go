package utils

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/mitchellh/mapstructure"
)

func Struct2Map(tag string, in interface{}) (map[string]interface{}, error) {

	out := map[string]interface{}{}
	decode, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Metadata: nil,
		Result:   &out,
		TagName:  tag,
	})
	err := decode.Decode(in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func Map2Struct(tag string, in map[string]interface{}, out interface{}) error {

	decode, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		Metadata: nil,
		Result:   out,
		TagName:  tag,
	})
	err := decode.Decode(in)
	if err != nil {
		return err
	}
	return nil
}

func GetTempDir() string {

	dir := os.TempDir()
	dir = filepath.Join(dir, "srpc")
	os.Mkdir(dir, 0644)
	return dir
}

func GetTable(dest interface{}) string {

	modelType := reflect.ValueOf(dest).Type()
	for modelType.Kind() == reflect.Slice || modelType.Kind() == reflect.Array || modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	if modelType.Kind() != reflect.Struct {
		var err error
		if modelType.PkgPath() == "" {
			err = fmt.Errorf("%s: %+v", "mgo", dest)
		} else {
			err = fmt.Errorf("%s: %s.%s", "mgo", modelType.PkgPath(), modelType.Name())
		}
		log.Println("get_table_name	", err)
		return ""
	}
	modelName := modelType.Name()
	return modelName
}

func Sprintf(strs ...string) string {

	var buf strings.Builder
	slen := len(strs)
	for i := 0; i < slen; i++ {
		buf.WriteString(strs[i])
	}
	return buf.String()
}
