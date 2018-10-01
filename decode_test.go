package btawel_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/osamingo/boolconv"

	"github.com/tvlk-data/btawel"
	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/require"
)

func TestReadRowErrorCase(t *testing.T) {

	ris := []bigtable.ReadItem{
		bigtable.ReadItem{
			Row:    "rowkey",
			Column: "fc:test",
			Value:  []byte("test"),
		},
	}

	row := bigtable.Row{
		"fc": ris,
	}

	t.Run("No error", func(t *testing.T) {

		t.Run("ReadRow", func(t *testing.T) {
			err := btawel.ReadRow(row, struct{}{})
			require.NoError(t, err)
		})

		t.Run("ReadItem", func(t *testing.T) {
			err := btawel.ReadItems(ris, struct{}{})
			require.NoError(t, err)
		})
	})

	t.Run("Error if type is different", func(t *testing.T) {
		s := struct {
			T int `bigtable:"test"`
		}{}

		t.Run("ReadItems", func(t *testing.T) {
			err := btawel.ReadItems(ris, &s)
			require.Error(t, err)
		})

		t.Run("ReadRow", func(t *testing.T) {
			err := btawel.ReadRow(row, &s)
			require.Error(t, err)
		})

	})

	t.Run("Error if it uses unsupported type", func(t *testing.T) {
		r := struct {
			R bigtable.ReadItem `bigtable:",rowkey"`
		}{}

		t.Run("ReadRow", func(t *testing.T) {
			err := btawel.ReadRow(row, &r)
			require.Error(t, err)
		})

		t.Run("ReadItems", func(t *testing.T) {
			err := btawel.ReadItems(ris, &r)
			require.Error(t, err)
		})

	})

}

func TestRead(t *testing.T) {

	s := struct {
		TNonTag  string
		TRowKey  string  `bigtable:",rowkey"`
		TBytes   []byte  `bigtable:"fc:tbytes"`
		TString  string  `bigtable:"fc:tstr"`
		TBool    bool    `bigtable:"fc:tbool"`
		TInt     int     `bigtable:"fc:tint"`
		TInt8    int8    `bigtable:"fc:tint8"`
		TInt16   int16   `bigtable:"fc:tint16"`
		TInt32   int32   `bigtable:"fc:tint32"`
		TInt64   int64   `bigtable:"fc:tint64"`
		TUint    uint    `bigtable:"fc:tuint"`
		TUint8   uint8   `bigtable:"fc:tuint8"`
		TUint16  uint16  `bigtable:"fc:tuint16"`
		TUint32  uint32  `bigtable:"fc:tuint32"`
		TUint64  uint64  `bigtable:"fc:tuint64"`
		TFloat32 float32 `bigtable:"fc:tfloat32"`
		TFloat64 float64 `bigtable:"fc:tfloat64"`
	}{}

	key := "thisisrowkey"
	bstr := "bytebyte"
	str := "hoge"
	bl := true
	num := 123
	buf := &bytes.Buffer{}

	ris := []bigtable.ReadItem{
		bigtable.ReadItem{
			Row:    key,
			Column: "fc:tbytes",
			Value:  []byte(bstr),
		},
		bigtable.ReadItem{
			Row:    key,
			Column: "fc:tstr",
			Value:  []byte(str),
		},
		bigtable.ReadItem{
			Row:    key,
			Column: "fc:tbool",
			Value:  boolconv.NewBool(bl).Bytes(),
		},
	}

	binary.Write(buf, binary.BigEndian, int64(num))
	ris = append(ris, bigtable.ReadItem{
		Row:    key,
		Column: "fc:tint",
		Value:  buf.Bytes(),
	})

	buf = &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, int8(num))
	ris = append(ris, bigtable.ReadItem{
		Row:    key,
		Column: "fc:tint8",
		Value:  buf.Bytes(),
	})

	buf = &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, int16(num))
	ris = append(ris, bigtable.ReadItem{
		Row:    key,
		Column: "fc:tint16",
		Value:  buf.Bytes(),
	})

	buf = &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, int32(num))
	ris = append(ris, bigtable.ReadItem{
		Row:    key,
		Column: "fc:tint32",
		Value:  buf.Bytes(),
	})

	buf = &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, int64(num))
	ris = append(ris, bigtable.ReadItem{
		Row:    key,
		Column: "fc:tint64",
		Value:  buf.Bytes(),
	})

	buf = &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, uint64(num))
	ris = append(ris, bigtable.ReadItem{
		Row:    key,
		Column: "fc:tuint",
		Value:  buf.Bytes(),
	})

	buf = &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, uint8(num))
	ris = append(ris, bigtable.ReadItem{
		Row:    key,
		Column: "fc:tuint8",
		Value:  buf.Bytes(),
	})

	buf = &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, uint16(num))
	ris = append(ris, bigtable.ReadItem{
		Row:    key,
		Column: "fc:tuint16",
		Value:  buf.Bytes(),
	})

	buf = &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, uint32(num))
	ris = append(ris, bigtable.ReadItem{
		Row:    key,
		Column: "fc:tuint32",
		Value:  buf.Bytes(),
	})

	buf = &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, uint64(num))
	ris = append(ris, bigtable.ReadItem{
		Row:    key,
		Column: "fc:tuint64",
		Value:  buf.Bytes(),
	})

	buf = &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, float32(num))
	ris = append(ris, bigtable.ReadItem{
		Row:    key,
		Column: "fc:tfloat32",
		Value:  buf.Bytes(),
	})

	buf = &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, float64(num))
	ris = append(ris, bigtable.ReadItem{
		Row:    key,
		Column: "fc:tfloat64",
		Value:  buf.Bytes(),
	})

	t.Run("ReadRow should parse data correctly", func(t *testing.T) {
		row := bigtable.Row{"fc": ris}
		err := btawel.ReadRow(row, &s)

		require.NoError(t, err)
		require.Equal(t, key, s.TRowKey)
		require.Equal(t, bstr, string(s.TBytes))
		require.Equal(t, str, s.TString)
		require.True(t, s.TBool)
		require.Equal(t, int(num), s.TInt)
		require.Equal(t, int8(num), s.TInt8)
		require.Equal(t, int16(num), s.TInt16)
		require.Equal(t, int32(num), s.TInt32)
		require.Equal(t, int64(num), s.TInt64)
		require.Equal(t, uint(num), s.TUint)
		require.Equal(t, uint8(num), s.TUint8)
		require.Equal(t, uint16(num), s.TUint16)
		require.Equal(t, uint32(num), s.TUint32)
		require.Equal(t, uint64(num), s.TUint64)
		require.Equal(t, float32(num), s.TFloat32)
		require.Equal(t, float64(num), s.TFloat64)
	})

	t.Run("ReadItems Should Parse Data correctly", func(t *testing.T) {

		err := btawel.ReadItems(ris, &s)

		require.NoError(t, err)
		require.Equal(t, key, s.TRowKey)
		require.Equal(t, bstr, string(s.TBytes))
		require.Equal(t, str, s.TString)
		require.True(t, s.TBool)
		require.Equal(t, int(num), s.TInt)
		require.Equal(t, int8(num), s.TInt8)
		require.Equal(t, int16(num), s.TInt16)
		require.Equal(t, int32(num), s.TInt32)
		require.Equal(t, int64(num), s.TInt64)
		require.Equal(t, uint(num), s.TUint)
		require.Equal(t, uint8(num), s.TUint8)
		require.Equal(t, uint16(num), s.TUint16)
		require.Equal(t, uint32(num), s.TUint32)
		require.Equal(t, uint64(num), s.TUint64)
		require.Equal(t, float32(num), s.TFloat32)
		require.Equal(t, float64(num), s.TFloat64)

	})
}

type Address struct {
	Address string `bigtable:"address:address"`
}

type Person struct {
	Name    string `bigtable:"info:name"`
	Age     int32    `bigtable:"info:age"`
	Address Address
}

func TestReadRowNestedStruct(t *testing.T) {

	buf := make([]byte, binary.MaxVarintLen32)
	binary.BigEndian.PutUint32(buf, uint32(16))

	row := map[string][]bigtable.ReadItem{
		"info": []bigtable.ReadItem{
			bigtable.ReadItem{
				Row:    "john",
				Column: "info:name",
				Value:  []byte("John"),
			},
			bigtable.ReadItem{
				Row:    "john",
				Column: "info:age",
				Value:  buf,
			},
		},
		"address": []bigtable.ReadItem{
			bigtable.ReadItem{
				Row:    "john",
				Column: "address:address",
				Value:  []byte("Rafless st."),
			},
		},
	}

	var person Person
	err := btawel.ReadRow(row, &person)

	require.NoError(t, err)
	require.Equal(t, "John", person.Name)
	require.Equal(t, "Rafless st.", person.Address.Address)
	require.Equal(t, int32(16), person.Age)
}


func TestReadColumnQualifiers(t *testing.T) {

	ris := []bigtable.ReadItem{
		bigtable.ReadItem{
			Row:    "rowkey",
			Column: "fc:test",
			Value:  []byte("test"),
		},
		bigtable.ReadItem{
			Row:    "rowkey",
			Column: "fc:test2",
			Value:  []byte("test"),
		},
	}

	cqs := btawel.ReadColumnQualifier(ris)

	require.Len(t, cqs, 2)
	require.Equal(t, "test", cqs[0])
	require.Equal(t, "test2", cqs[1])
}
