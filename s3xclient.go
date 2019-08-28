package edgex

type S3xClient interface {
	BucketCreate(bucket string) error
	BucketHead(bucket string) error
	BucketDelete(bucket string) error

	KeyValueCreate(bucket string, object string,
		contentType string, chunkSize int, btreeOrder int) error
	KeyValuePost(bucket string, object string, contentType string,
		key string, value string, more bool) error
	KeyValuePostJSON(bucket string, object string,
		keyValueJSON string, more bool) error
	KeyValuePostCSV(bucket string, object string,
		keyValueCSV string, more bool) error
	KeyValueDelete(bucket string, object string,
		key string, more bool) error
	KeyValueDeleteJSON(bucket string, object string,
		keyValueJSON string, more bool) error
	KeyValueGet(bucket string, object string, key string) error
	KeyValueList(bucket string, object string,
		from string, pattern string, contentType string, maxcount int, values bool) error
	ObjectHead(bucket string, object string) error
	ObjectDelete(bucket string, object string) error

	GetLastValue() string
}
