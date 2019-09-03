package edgex

import (
	"bytes"
	"encoding/xml"
)

const OBJECT_TYPE_OBJECT = "object"
const OBJECT_TYPE_KEY_VALUE = "keyValue"

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

// S3xClient - s3x client interface
type S3xClient interface {
	BucketCreate(bucket string) error
	BucketHead(bucket string) error
	BucketDelete(bucket string) error

	ObjectCreate(bucket string, object string, objectType string,
		contentType string, chunkSize int, btreeOrder int) error
	KeyValuePost(bucket string, object string, contentType string,
		key string, value *bytes.Buffer, more bool) error
	KeyValuePostJSON(bucket string, object string,
		keyValueJSON string, more bool) error
	KeyValuePostCSV(bucket string, object string,
		keyValueCSV string, more bool) error
	KeyValueDelete(bucket string, object string,
		key string, more bool) error
	KeyValueDeleteJSON(bucket string, object string,
		keyValueJSON string, more bool) error
	KeyValueCommit(bucket string, object string) error
	KeyValueRollback(bucket string, object string) error
	KeyValueGet(bucket string, object string, key string) error
	KeyValueList(bucket string, object string,
		from string, pattern string, contentType string, maxcount int, values bool) error
	ObjectHead(bucket string, object string) error
	ObjectDelete(bucket string, object string) error
	BucketList() ([]Bucket, error)
	ObjectList(bucket string, from string, pattern string, maxcount int) ([]Object, error)

	GetLastValue() string
}
