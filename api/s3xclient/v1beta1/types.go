package v1beta1

import (
	"encoding/xml"
)

type ObjectType string
type ContentType string

type ObjectCreationOptions struct {
	ObjectType  ObjectType
	ContentType ContentType
	ChunkSize   int
	BTreeOrder  int
}

const (
	OBJECT_TYPE_OBJECT    ObjectType  = "object"
	OBJECT_TYPE_KEY_VALUE ObjectType  = "keyValue"
	ContentTypeJSON       ContentType = "application/json"

	DEFAULT_CHUNKSIZE   int = 4096
	DEFAULT_BTREE_ORDER int = 4

	SS_CONT            int = 0x00
	SS_FIN             int = 0x01
	SS_APPEND          int = 0x02
	SS_RANDWR          int = 0x04
	SS_KV              int = 0x08
	SS_STAT            int = 0x10
	CCOW_O_REPLACE     int = 0x01
	CCOW_O_CREATE      int = 0x02
	BYTE_BUFFER        int = 16 * 1024
	DEFAULT_EDGEX_PORT int = 3000
)

var DefaultObjectCreationOption = ObjectCreationOptions{
	ObjectType:  OBJECT_TYPE_KEY_VALUE,
	ContentType: ContentTypeJSON,
	ChunkSize:   DEFAULT_CHUNKSIZE,
	BTreeOrder:  DEFAULT_BTREE_ORDER,
}

// ListAllMyBucketsResult - bucket list structure
type ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Buckets Buckets  `xml:"Buckets"`
}

// Buckets - array of Buckets
type Buckets struct {
	XMLName xml.Name `xml:"Buckets"`
	Buckets []Bucket `xml:"Bucket"`
}

// Bucket structure
type Bucket struct {
	XMLName      xml.Name `xml:"Bucket"`
	CreationDate string   `xml:"CreationDate"`
	Name         string   `xml:"Name"`
}

// ListBucketResult - bucket list structure
type ListBucketResult struct {
	XMLName xml.Name `xml:"ListBucketResult"`
	Objects []Object `xml:"Contents"`
}

// Object - object structure
type Object struct {
	XMLName      xml.Name `xml:"Contents"`
	Key          string   `xml:"Key"`
	LastModified string   `xml:"LastModified"`
	Size         int      `xml:"Size"`
}
