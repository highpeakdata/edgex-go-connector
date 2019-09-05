package s3xMockClient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	s3xApi "github.com/Nexenta/edgex-go-connector/api/s3xclient/v1beta1"
)

type kvobj struct {
	keyValue  map[string]string
	recent    map[string]string
	recentDel []string
}

// Mockup - mockup client mockup structure
type Mockup struct {
	objects map[string]kvobj
	buckets map[string]s3xApi.Bucket

	// Current session
	Bucket string
	Object string
	Sid    string
	Value  string
	Debug  int
}

// CreateMockup - client structure constructor
func CreateMockup(debug int) *Mockup {
	mockup := new(Mockup)
	mockup.buckets = make(map[string]s3xApi.Bucket)
	mockup.objects = make(map[string]kvobj)
	mockup.Debug = debug
	mockup.Sid = ""
	mockup.Bucket = ""
	mockup.Object = ""
	mockup.Value = ""
	return mockup
}

// GetLastValue - get last result value
func (mockup *Mockup) GetLastValue() string {
	return mockup.Value
}

// BucketCreate - create a new bucket
func (mockup *Mockup) BucketCreate(bucket string) error {
	_, exists := mockup.buckets[bucket]
	if exists {
		return fmt.Errorf("%s bucket already exists", bucket)
	}

	t := time.Now()
	mockup.buckets[bucket] = s3xApi.Bucket{Name: bucket, CreationDate: t.Format(time.RFC3339)}
	return nil
}

// ObjectCreate - create object
func (mockup *Mockup) ObjectCreate(bucket string, object string, objectType s3xApi.ObjectType,
	contentType string, chunkSize int, btreeOrder int) error {

	_, exists := mockup.buckets[bucket]
	if !exists {
		return fmt.Errorf("%s bucket not found", bucket)
	}

	var uri = bucket + "/" + object
	_, e := mockup.objects[uri]
	if e {
		return fmt.Errorf("%s/%s already exists", bucket, object)
	}

	var kv kvobj
	kv.keyValue = make(map[string]string)
	kv.recent = make(map[string]string)
	mockup.objects[uri] = kv
	return nil
}

// keyValueCommitNow - commit key/value insert/update/delete
func keyValueCommitNow(mockup *Mockup, bucket string, object string) error {
	var uri = bucket + "/" + object
	o, exists := mockup.objects[uri]
	if !exists {
		return fmt.Errorf("Object %s/%s not found", bucket, object)
	}
	for key, value := range o.recent {
		o.keyValue[key] = value
	}
	for _, key := range o.recentDel {
		delete(o.keyValue, key)
	}
	if len(o.recentDel) > 0 {
		o.recentDel = nil
	}
	if len(o.recent) > 0 {
		o.recent = make(map[string]string)
	}
	return nil
}

// KeyValueCommit - commit key/value insert/update/delete
func (mockup *Mockup) KeyValueCommit(bucket string, object string) error {
	return keyValueCommitNow(mockup, bucket, object)
}

// KeyValueRollback - rollback key/value insert/update/delete session
func (mockup *Mockup) KeyValueRollback(bucket string, object string) error {
	var uri = bucket + "/" + object
	o, exists := mockup.objects[uri]
	if !exists {
		return fmt.Errorf("Object %s/%s not found", bucket, object)
	}
	if len(o.recentDel) > 0 {
		o.recentDel = nil
	}
	if len(o.recent) > 0 {
		o.recent = make(map[string]string)
	}
	return nil
}

// ObjectDelete - delete object
func (mockup *Mockup) ObjectDelete(bucket string, object string) error {
	var uri = bucket + "/" + object
	delete(mockup.objects, uri)
	return nil
}

// BucketDelete - delete bucket
func (mockup *Mockup) BucketDelete(bucket string) error {
	delete(mockup.buckets, bucket)
	return nil
}

// ObjectHead - read object header fields
func (mockup *Mockup) ObjectHead(bucket string, object string) error {
	var uri = bucket + "/" + object

	_, exists := mockup.objects[uri]
	if exists {
		return nil
	}
	return fmt.Errorf("Object %s/%s not found", bucket, object)
}

// BucketHead - read bucket header fields
func (mockup *Mockup) BucketHead(bucket string) error {
	_, exists := mockup.buckets[bucket]
	if exists {
		return nil
	}
	return fmt.Errorf("Bucket %s not found", bucket)
}

func (mockup *Mockup) KeyValueMapPost(bucket, object string, valuesMap s3xApi.S3xKVMap, more bool) error {
	var uri = bucket + "/" + object

	o, exists := mockup.objects[uri]
	if !exists {
		return fmt.Errorf("Object %s/%s not found", bucket, object)
	}

	if !more {
		keyValueCommitNow(mockup, bucket, object)
	}

	for key, value := range valuesMap {

		valueMapByte, err := json.Marshal(value)
		if err != nil {
			return err
		}

		if more {
			o.recent[key] = string(valueMapByte)
		} else {
			o.keyValue[key] = string(valueMapByte)
		}
	}
	return nil
}

// KeyValuePost - post key/value pairs
func (mockup *Mockup) KeyValuePost(bucket string, object string, key string, value *bytes.Buffer, contentType string, more bool) error {
	var uri = bucket + "/" + object

	o, exists := mockup.objects[uri]
	if !exists {
		return fmt.Errorf("Object %s/%s not found", bucket, object)
	}
	if more {
		o.recent[key] = value.String()
	} else {
		keyValueCommitNow(mockup, bucket, object)
		o.keyValue[key] = value.String()
	}
	return nil
}

// KeyValuePostJSON - post key/value pairs
func (mockup *Mockup) KeyValuePostJSON(bucket string, object string,
	keyValueJSON string, more bool) error {
	var uri = bucket + "/" + object

	o, exists := mockup.objects[uri]
	if !exists {
		return fmt.Errorf("Object %s/%s not found", bucket, object)
	}

	if !more {
		keyValueCommitNow(mockup, bucket, object)
	}

	var result map[string]interface{}
	json.Unmarshal([]byte(keyValueJSON), &result)

	for key, value := range result {
		if more {
			o.recent[key] = value.(string)
		} else {
			o.keyValue[key] = value.(string)
		}
	}
	return nil
}

// KeyValuePostCSV - post key/value pairs presented like csv
func (mockup *Mockup) KeyValuePostCSV(bucket string, object string,
	keyValueCSV string, more bool) error {

	var uri = bucket + "/" + object

	o, exists := mockup.objects[uri]
	if !exists {
		return fmt.Errorf("Object %s/%s not found", bucket, object)
	}

	if !more {
		keyValueCommitNow(mockup, bucket, object)
	}

	result := strings.Split(keyValueCSV, "\n")

	for _, s := range result {
		kv := strings.Split(s, ";")
		if len(kv) < 2 {
			continue
		}
		if more {
			o.recent[kv[0]] = kv[1]
		} else {
			o.keyValue[kv[0]] = kv[1]
		}
	}
	return nil
}

// KeyValueDelete - delete key/value pair
func (mockup *Mockup) KeyValueDelete(bucket string, object string,
	key string, more bool) error {

	var uri = bucket + "/" + object

	o, exists := mockup.objects[uri]
	if !exists {
		return fmt.Errorf("Object %s/%s not found", bucket, object)
	}

	if !more {
		keyValueCommitNow(mockup, bucket, object)
	}

	if more {
		delete(o.recent, key)
		o.recentDel = append(o.recentDel, key)
	} else {
		delete(o.keyValue, key)
	}
	return nil
}

// KeyValueDeleteJSON - delete key/value pairs defined by json
func (mockup *Mockup) KeyValueMapDelete(bucket string, object string,
	valuesMap s3xApi.S3xKVMap, more bool) error {
	var uri = bucket + "/" + object

	o, exists := mockup.objects[uri]
	if !exists {
		return fmt.Errorf("Object %s/%s not found", bucket, object)
	}

	if !more {
		keyValueCommitNow(mockup, bucket, object)
	}

	for key := range valuesMap {
		if more {
			delete(o.recent, key)
			o.recentDel = append(o.recentDel, key)
		} else {
			delete(o.keyValue, key)
		}
	}
	return nil
}

// KeyValueDeleteJSON - delete key/value pairs defined by json
func (mockup *Mockup) KeyValueDeleteJSON(bucket string, object string,
	keyValueJSON string, more bool) error {
	var uri = bucket + "/" + object

	o, exists := mockup.objects[uri]
	if !exists {
		return fmt.Errorf("Object %s/%s not found", bucket, object)
	}

	if !more {
		keyValueCommitNow(mockup, bucket, object)
	}

	var result map[string]interface{}
	json.Unmarshal([]byte(keyValueJSON), &result)

	for key := range result {
		if more {
			delete(o.recent, key)
			o.recentDel = append(o.recentDel, key)
		} else {
			delete(o.keyValue, key)
		}
	}
	return nil
}

// KeyValueGet - read object value field
func (mockup *Mockup) KeyValueGet(bucket string, object string, key string) (string, error) {
	var uri = bucket + "/" + object

	o, exists := mockup.objects[uri]
	if !exists {
		return "", fmt.Errorf("Object %s/%s not found", bucket, object)
	}

	_, e := o.keyValue[key]
	if !e {
		return "", fmt.Errorf("Object %s/%s key %s found", bucket, object, key)
	}

	return mockup.Value, nil
}

// KeyValueList - read key/value pairs, contentType: application/json or text/csv
func (mockup *Mockup) KeyValueList(bucket string, object string,
	from string, pattern string, contentType string, maxcount int, values bool) (string, error) {

	var uri = bucket + "/" + object

	o, exists := mockup.objects[uri]
	if !exists {
		return "", fmt.Errorf("Object %s/%s not found", bucket, object)
	}

	keys := make([]string, 0, len(o.keyValue))

	for k := range o.keyValue {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b bytes.Buffer

	json := strings.Contains(contentType, "json")

	if json {
		b.WriteString("{")
	}

	n := 0
	for i := range keys {
		key := keys[i]
		if key < from {
			continue
		}

		if pattern != "" && !strings.HasPrefix(key, pattern) {
			continue
		}

		value, e := o.keyValue[key]
		if !e {
			continue
		}

		if json {
			if n > 0 {
				b.WriteString(", ")
			}
			b.WriteString(" \"")
			b.WriteString(key)
			b.WriteString("\": \"")
			b.WriteString(value)
			b.WriteString("\"")
		} else {
			if n > 0 {
				b.WriteString("\n")
			}
			b.WriteString(key)
			b.WriteString(";")
			b.WriteString(value)
		}

		n++
		if n == maxcount {
			break
		}
	}

	if json {
		b.WriteString("}")
	}

	return b.String(), nil
}

// BucketList - read bucket list
func (mockup *Mockup) BucketList() ([]s3xApi.Bucket, error) {
	keys := make([]string, 0, len(mockup.buckets))

	for k := range mockup.buckets {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buckets []s3xApi.Bucket
	for i := range keys {
		key := keys[i]
		buckets = append(buckets, mockup.buckets[key])
	}
	return buckets, nil
}

// ObjectList - read object list from bucket
func (mockup *Mockup) ObjectList(bucket string,
	from string, pattern string, maxcount int) ([]s3xApi.Object, error) {

	var objects []s3xApi.Object
	keys := make([]string, 0, len(mockup.objects))

	for k := range mockup.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	n := 0
	t := time.Now()

	for i := range keys {
		key := strings.TrimPrefix(keys[i], bucket+"/")
		if key < from {
			continue
		}

		if pattern != "" && !strings.HasPrefix(key, pattern) {
			continue
		}

		objects = append(objects, s3xApi.Object{Key: key, LastModified: t.Format(time.RFC3339), Size: 0})
		n++
		if n == maxcount {
			break
		}
	}

	return objects, nil
}
