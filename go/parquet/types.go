// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parquet

import (
	"encoding/binary"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/arrow"
	format "github.com/apache/arrow/go/parquet/internal/gen-go/parquet"
)

const (
	julianUnixEpoch int64 = 2440588
	nanosPerDay     int64 = 3600 * 24 * 1000 * 1000 * 1000
	// Int96SizeBytes is the number of bytes that make up an Int96
	Int96SizeBytes int = 12
)

var (
	// Int96Traits provides information about the Int96 type
	Int96Traits int96Traits
	// ByteArrayTraits provides information about the ByteArray type, which is just an []byte
	ByteArrayTraits byteArrayTraits
	// FixedLenByteArrayTraits provides information about the FixedLenByteArray type which is just an []byte
	FixedLenByteArrayTraits fixedLenByteArrayTraits
	// ByteArraySizeBytes is the number of bytes returned by reflect.TypeOf(ByteArray{}).Size()
	ByteArraySizeBytes int = int(reflect.TypeOf(ByteArray{}).Size())
	// FixedLenByteArraySizeBytes is the number of bytes returned by reflect.TypeOf(FixedLenByteArray{}).Size()
	FixedLenByteArraySizeBytes int = int(reflect.TypeOf(FixedLenByteArray{}).Size())
)

// NewInt96 creates a new Int96 from the given 3 uint32 values.
func NewInt96(v [3]uint32) (out Int96) {
	binary.LittleEndian.PutUint32(out[0:], v[0])
	binary.LittleEndian.PutUint32(out[4:], v[1])
	binary.LittleEndian.PutUint32(out[8:], v[2])
	return
}

// Int96 is a 12 byte integer value utilized for representing timestamps as a 64 bit integer and a 32 bit
// integer.
type Int96 [12]byte

// SetNanoSeconds sets the Nanosecond field of the Int96 timestamp to the provided value
func (i96 *Int96) SetNanoSeconds(nanos int64) {
	binary.LittleEndian.PutUint64(i96[:8], uint64(nanos))
}

// String provides the string representation as a timestamp via converting to a time.Time
// and then calling String
func (i96 Int96) String() string {
	return i96.ToTime().String()
}

// ToTime returns a go time.Time object that represents the same time instant as the given Int96 value
func (i96 Int96) ToTime() time.Time {
	nanos := binary.LittleEndian.Uint64(i96[:8])
	jdays := binary.LittleEndian.Uint32(i96[8:])

	nanos = (uint64(jdays)-uint64(julianUnixEpoch))*uint64(nanosPerDay) + nanos
	t := time.Unix(0, int64(nanos))
	return t.UTC()
}

type int96Traits struct{}

func (int96Traits) BytesRequired(n int) int { return Int96SizeBytes * n }

func (int96Traits) CastFromBytes(b []byte) []Int96 {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []Int96
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / Int96SizeBytes
	s.Cap = h.Cap / Int96SizeBytes

	return res
}

func (int96Traits) CastToBytes(b []Int96) []byte {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []byte
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len * Int96SizeBytes
	s.Cap = h.Cap * Int96SizeBytes

	return res
}

// ByteArray is a type to be utilized for representing the Parquet ByteArray physical type, represented as a byte slice
type ByteArray []byte

// Len returns the current length of the ByteArray, equivalent to len(bytearray)
func (b ByteArray) Len() int {
	return len(b)
}

// String returns a string representation of the ByteArray
func (b ByteArray) String() string {
	return *(*string)(unsafe.Pointer(&b))
}

type byteArrayTraits struct{}

func (byteArrayTraits) BytesRequired(n int) int {
	return ByteArraySizeBytes * n
}

func (byteArrayTraits) CastFromBytes(b []byte) []ByteArray {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []ByteArray
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / ByteArraySizeBytes
	s.Cap = h.Cap / ByteArraySizeBytes

	return res
}

// FixedLenByteArray is a go type to represent a FixedLengthByteArray as a byte slice
type FixedLenByteArray []byte

// Len returns the current length of this FixedLengthByteArray, equivalent to len(fixedlenbytearray)
func (b FixedLenByteArray) Len() int {
	return len(b)
}

// String returns a string representation of the FixedLenByteArray
func (b FixedLenByteArray) String() string {
	return *(*string)(unsafe.Pointer(&b))
}

type fixedLenByteArrayTraits struct{}

func (fixedLenByteArrayTraits) BytesRequired(n int) int {
	return FixedLenByteArraySizeBytes * n
}

func (fixedLenByteArrayTraits) CastFromBytes(b []byte) []FixedLenByteArray {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))

	var res []FixedLenByteArray
	s := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	s.Data = h.Data
	s.Len = h.Len / FixedLenByteArraySizeBytes
	s.Cap = h.Cap / FixedLenByteArraySizeBytes

	return res
}

// Creating our own enums allows avoiding the transitive dependency on the
// compiled thrift definitions in the public API, allowing us to not export
// the entire Thrift definitions, while making everything a simple cast between.
//
// It also let's us add special values like NONE to distinguish between values
// that are set or not set
type (
	// Type is the physical type as in parquet.thrift
	Type format.Type
	// Cipher is the parquet Cipher Algorithms
	Cipher int
	// ColumnOrder is the Column Order from the parquet.thrift
	ColumnOrder *format.ColumnOrder
	// Version is the parquet version type
	Version int8
	// DataPageVersion is the version of the Parquet Data Pages
	DataPageVersion int8
	// Encoding is the parquet Encoding type
	Encoding format.Encoding
	// Repetition is the underlying parquet field repetition type as in parquet.thrift
	Repetition format.FieldRepetitionType
	// ColumnPath is the path from the root of the schema to a given column
	ColumnPath []string
)

func (c ColumnPath) String() string {
	if c == nil {
		return ""
	}
	return strings.Join(c, ".")
}

// Extend creates a new ColumnPath from an existing one, with the new ColumnPath having s appended to the end.
func (c ColumnPath) Extend(s string) ColumnPath {
	p := make([]string, len(c), len(c)+1)
	copy(p, c)
	return append(p, s)
}

// ColumnPathFromString constructs a ColumnPath from a dot separated string
func ColumnPathFromString(s string) ColumnPath {
	return strings.Split(s, ".")
}

// constants for choosing the Aes Algorithm to use for encryption/decryption
const (
	AesGcm Cipher = iota
	AesCtr
)

// Constants for the parquet Version
const (
	V1 Version = 1
	V2 Version = 2
)

// constants for the parquet DataPage Version to use
const (
	DataPageV1 DataPageVersion = iota
	DataPageV2
)

func (e Encoding) String() string {
	return format.Encoding(e).String()
}

var (
	// Types contains constants for the Physical Types that are used in the Parquet Spec
	//
	// They can be specified when needed as such: `parquet.Types.Int32` etc. The values
	// all correspond to the values in parquet.thrift
	Types = struct {
		Boolean           Type
		Int32             Type
		Int64             Type
		Int96             Type
		Float             Type
		Double            Type
		ByteArray         Type
		FixedLenByteArray Type
		// this only exists as a convienence so we can denote it when necessary
		// nearly all functions that take a parquet.Type will error/panic if given
		// Undefined
		Undefined Type
	}{
		Boolean:           Type(format.Type_BOOLEAN),
		Int32:             Type(format.Type_INT32),
		Int64:             Type(format.Type_INT64),
		Int96:             Type(format.Type_INT96),
		Float:             Type(format.Type_FLOAT),
		Double:            Type(format.Type_DOUBLE),
		ByteArray:         Type(format.Type_BYTE_ARRAY),
		FixedLenByteArray: Type(format.Type_FIXED_LEN_BYTE_ARRAY),
		Undefined:         Type(format.Type_FIXED_LEN_BYTE_ARRAY + 1),
	}

	// Encodings contains constants for the encoding types of the column data
	//
	// The values used all correspond to the values in parquet.thrift for the
	// corresponding encoding type.
	Encodings = struct {
		Plain                Encoding
		PlainDict            Encoding
		RLE                  Encoding
		RLEDict              Encoding
		BitPacked            Encoding // deprecated, not implemented
		DeltaByteArray       Encoding
		DeltaBinaryPacked    Encoding
		DeltaLengthByteArray Encoding
	}{
		Plain:                Encoding(format.Encoding_PLAIN),
		PlainDict:            Encoding(format.Encoding_PLAIN_DICTIONARY),
		RLE:                  Encoding(format.Encoding_RLE),
		RLEDict:              Encoding(format.Encoding_RLE_DICTIONARY),
		BitPacked:            Encoding(format.Encoding_BIT_PACKED),
		DeltaByteArray:       Encoding(format.Encoding_DELTA_BYTE_ARRAY),
		DeltaBinaryPacked:    Encoding(format.Encoding_DELTA_BINARY_PACKED),
		DeltaLengthByteArray: Encoding(format.Encoding_DELTA_LENGTH_BYTE_ARRAY),
	}

	// ColumnOrders contains constants for the Column Ordering fields
	ColumnOrders = struct {
		Undefined        ColumnOrder
		TypeDefinedOrder ColumnOrder
	}{
		Undefined:        format.NewColumnOrder(),
		TypeDefinedOrder: &format.ColumnOrder{TYPE_ORDER: format.NewTypeDefinedOrder()},
	}

	// DefaultColumnOrder is to use TypeDefinedOrder
	DefaultColumnOrder = ColumnOrders.TypeDefinedOrder

	// Repetitions contains the constants for Field Repetition Types
	Repetitions = struct {
		Required  Repetition
		Optional  Repetition
		Repeated  Repetition
		Undefined Repetition // convenience value
	}{
		Required:  Repetition(format.FieldRepetitionType_REQUIRED),
		Optional:  Repetition(format.FieldRepetitionType_OPTIONAL),
		Repeated:  Repetition(format.FieldRepetitionType_REPEATED),
		Undefined: Repetition(format.FieldRepetitionType_REPEATED + 1),
	}
)

func (t Type) String() string {
	switch t {
	case Types.Undefined:
		return "UNDEFINED"
	default:
		return format.Type(t).String()
	}
}

func (r Repetition) String() string {
	return strings.ToLower(format.FieldRepetitionType(r).String())
}

// ByteSize returns the number of bytes required to store a single value of
// the given parquet.Type in memory.
func (t Type) ByteSize() int {
	switch t {
	case Types.Boolean:
		return 1
	case Types.Int32:
		return arrow.Int32SizeBytes
	case Types.Int64:
		return arrow.Int64SizeBytes
	case Types.Int96:
		return Int96SizeBytes
	case Types.Float:
		return arrow.Float32SizeBytes
	case Types.Double:
		return arrow.Float64SizeBytes
	case Types.ByteArray:
		return ByteArraySizeBytes
	case Types.FixedLenByteArray:
		return FixedLenByteArraySizeBytes
	}
	panic("no bytesize info for type")
}
