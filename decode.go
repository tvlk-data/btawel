package btawel

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"

	"github.com/fatih/structs"
	"github.com/osamingo/boolconv"

	"cloud.google.com/go/bigtable"
)

// ReadRow converts bigtable.Row into a struct
func ReadRow(row bigtable.Row, s interface{}) (err error) {

	// create a map of bigtable readItem
	// to make data lookup faster
	rowMap := map[string]bigtable.ReadItem{}
	var items map[string][]bigtable.ReadItem
	items = row

	for _, v := range items {
		for _, item := range v {
			rowMap[item.Column] = item
		}
	}

	st := structs.New(s)
	fs := st.Fields()

	if err = parseVal(row, rowMap, fs); err != nil {
		return
	}
	return
}

// recursively parse data for all fields of struct based on the tag the field has
func parseVal(row bigtable.Row, rowMap map[string]bigtable.ReadItem, fs []*structs.Field) (err error) {

	if len(fs) == 0 {
		return
	}

	for _, f := range fs {

		t := f.Tag(BigtableTagName)

		ti := GetBigtableTagInfo(t)
		if ti.RowKey {
			if err = setValue(f, []byte(row.Key())); err != nil {
				return
			}
			continue
		}


		if f.Kind() == reflect.Struct {
			parseVal(row, rowMap, f.Fields())
		} else {
			if rowMap[ti.Column].Value == nil {
				continue
			}

			if err = setValue(f, rowMap[ti.Column].Value); err != nil {
				return
			}
			continue
		}
	}
	return
}

// ReadColumnQualifier returns column qualifiers.
func ReadColumnQualifier(ris []bigtable.ReadItem) (cqs []string) {

	for i := range ris {
		cs := strings.Split(ris[i].Column, ColumnQualifierDelimiter)
		cqs = append(cqs, cs[len(cs)-1])
	}

	return
}

// ReadItems converts Mutation into Struct.
func ReadItems(ris []bigtable.ReadItem, s interface{}) (err error) {

	if len(ris) == 0 || s == nil {
		return
	}

	fs := structs.New(s).Fields()
	if len(fs) == 0 {
		return
	}

	for i := range ris {

		for _, f := range fs {

			t := f.Tag(BigtableTagName)
			if t == "" {
				continue
			}

			ti := GetBigtableTagInfo(t)
			if ti.RowKey {
				if err = setValue(f, []byte(ris[i].Row)); err != nil {
					return
				}

				continue
			}

			cs := strings.Split(ris[i].Column, ColumnQualifierDelimiter)
			if cs[len(cs)-1] == ti.Column {
				if err = setValue(f, ris[i].Value); err != nil {
					return
				}

				continue
			}
		}
	}
	return
}

func setPointerValue(f *structs.Field, val []byte) (err error) {

	aType := reflect.TypeOf(f.Value())
	ptr := reflect.New(aType)

	switch reflect.ValueOf(f.Value()).Type().Elem().Kind() {
	case reflect.String:

		s := string(val)

		ptr.Elem().Set(reflect.ValueOf(&s))
		f.Set(ptr.Elem().Interface())

	case reflect.Int:

		var n int64
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)

		v := int(n)
		ptr.Elem().Set(reflect.ValueOf(&v))
		f.Set(ptr.Elem().Interface())

	case reflect.Bool:
		b := boolconv.BtoB(val).Tob()

		ptr.Elem().Set(reflect.ValueOf(&b))
		f.Set(ptr.Elem().Interface())


	case reflect.Uint:
		var n uint64
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)

		v := uint(n)
		ptr.Elem().Set(reflect.ValueOf(&v))
		f.Set(ptr.Elem().Interface())

	case reflect.Int8:
		var n int8
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)

		v := int8(n)
		ptr.Elem().Set(reflect.ValueOf(&v))
		f.Set(ptr.Elem().Interface())

	case reflect.Uint8:
		var n uint8
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)

		ptr.Elem().Set(reflect.ValueOf(&n))
		f.Set(ptr.Elem().Interface())

	case reflect.Int16:
		var n int16
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)

		ptr.Elem().Set(reflect.ValueOf(&n))
		f.Set(ptr.Elem().Interface())

	case reflect.Uint16:
		var n uint16
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)

		ptr.Elem().Set(reflect.ValueOf(&n))
		f.Set(ptr.Elem().Interface())

	case reflect.Int32:
		var n int32
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)

		ptr.Elem().Set(reflect.ValueOf(&n))
		f.Set(ptr.Elem().Interface())

	case reflect.Uint32:
		var n uint32
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)

		ptr.Elem().Set(reflect.ValueOf(&n))
		f.Set(ptr.Elem().Interface())

	case reflect.Int64:
		var n int64
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)

		ptr.Elem().Set(reflect.ValueOf(&n))
		f.Set(ptr.Elem().Interface())

	case reflect.Uint64:
		var n uint64
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)

		ptr.Elem().Set(reflect.ValueOf(&n))
		f.Set(ptr.Elem().Interface())

	case reflect.Float32:
		var n float32
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)

		ptr.Elem().Set(reflect.ValueOf(&n))
		f.Set(ptr.Elem().Interface())

	case reflect.Float64:
		var n float64
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)

		ptr.Elem().Set(reflect.ValueOf(&n))
		f.Set(ptr.Elem().Interface())

	default:
		err = fmt.Errorf("cloth: unsupported pointer type. %v", f.Kind())
	}
	return
}

func setValue(f *structs.Field, val []byte) (err error) {

	switch f.Kind() {

	case reflect.Ptr:
		err = setPointerValue(f, val)

	case reflect.Slice:
		if reflect.ValueOf(f.Value()).Type().Elem().Kind() == reflect.Uint8 {
			// []byte
			f.Set(val)
		}

	case reflect.String:
		f.Set(string(val))

	case reflect.Bool:
		f.Set(boolconv.BtoB(val).Tob())

	case reflect.Int:
		var n int64
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)
		f.Set(int(n))

	case reflect.Uint:
		var n uint64
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)
		f.Set(uint(n))

	case reflect.Int8:
		var n int8
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)
		f.Set(n)

	case reflect.Uint8:
		var n uint8
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)
		f.Set(n)

	case reflect.Int16:
		var n int16
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)
		f.Set(n)

	case reflect.Uint16:
		var n uint16
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)
		f.Set(n)

	case reflect.Int32:
		var n int32
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)
		f.Set(n)

	case reflect.Uint32:
		var n uint32
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)
		f.Set(n)

	case reflect.Int64:
		var n int64
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)
		f.Set(n)

	case reflect.Uint64:
		var n uint64
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)
		f.Set(n)

	case reflect.Float32:
		var n float32
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)
		f.Set(n)

	case reflect.Float64:
		var n float64
		err = binary.Read(bytes.NewReader(val), binary.BigEndian, &n)
		f.Set(n)

	default:
		err = fmt.Errorf("cloth: unsupported type. %v", f.Kind())

	}

	return
}
