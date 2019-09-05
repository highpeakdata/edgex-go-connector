package v1beta1

import "bytes"

type S3xKVMap map[string]interface{}
type S3xClient interface {

	// Lists all buckets in system
	BucketList() ([]Bucket, error)

	// Bucket related operations
	BucketHead(bucket string) error
	BucketCreate(bucket string) error
	BucketDelete(bucket string) error

	// Lists all objects for specifuc bucket
	ObjectList(bucket, from, pattern string, maxcount int) ([]Object, error)

	// KeyValue Object related operations
	ObjectHead(bucket, object string) error
	ObjectCreate(bucket, object string, objectType ObjectType, contentType string, chunkSize int, btreeOrder int) error
	ObjectDelete(bucket, object string) error

	// Single key operations
	KeyValueGet(bucket, object, key string) (string, error)
	KeyValuePost(bucket, object, key string, value *bytes.Buffer, contentType string, more bool) error
	KeyValueDelete(bucket, object, key string, more bool) error

	// Massive key/value operations
	KeyValueMapPost(bucket, object string, values S3xKVMap, more bool) error
	KeyValueMapDelete(bucket, object string, values S3xKVMap, more bool) error
	KeyValuePostJSON(bucket, object, values string, more bool) error
	KeyValuePostCSV(bucket, object, values string, more bool) error
	KeyValueDeleteJSON(bucket, object, keyValueJSON string, more bool) error

	// Object's key/value list
	KeyValueList(bucket, object, from, pattern, contentType string, maxcount int, values bool) (string, error)

	// Transactional methods
	KeyValueCommit(bucket string, object string) error
	KeyValueRollback(bucket string, object string) error
	//Close(bucket, object string) error
}
