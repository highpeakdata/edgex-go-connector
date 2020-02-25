package s3xMockClient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sort"
	"strings"
	"sync"
	"time"

	s3xApi "github.com/highpeakdata/edgex-go-connector/api/s3xclient/v1beta1"
	s3xErrors "github.com/highpeakdata/edgex-go-connector/pkg/errors"
)

var (
	mockupPath string = "/tmp/hpdcdb.json"
)

type kvobj struct {
	KeyValue  map[string]string `json:"keyValue"`
	recent    map[string]string `json:"-"`
	recentDel []string          `json:"-"`
}

// Mockup - mockup client mockup structure
type Mockup struct {
	Objects map[string]kvobj         `json:"objects"`
	Buckets map[string]s3xApi.Bucket `json:"buckets"`
	lock    sync.Mutex

	// Current session
	Bucket string `json:"-"`
	Object string `json:"-"`
	Sid    string `json:"-"`
	Debug  int    `json:"-"`
}

// CreateMockup - client structure constructor
func CreateMockup(debug int) *Mockup {
	mockup := new(Mockup)
	mockup.Buckets = make(map[string]s3xApi.Bucket)
	mockup.Objects = make(map[string]kvobj)
	mockup.Debug = debug
	mockup.Sid = ""
	mockup.Bucket = ""
	mockup.Object = ""
	f, err := ioutil.ReadFile(mockupPath)
	if err != nil {
		fmt.Printf("CreateMockup() config file not found\n")
	} else {
		err = json.Unmarshal(f, mockup)
		if err != nil {
			fmt.Printf("CreateMockup() configuration read error\n")
		}
	}
	return mockup
}

func keyValueSync(mockup *Mockup) error {
	buf, err := json.Marshal(mockup)
	if err != nil {
		return fmt.Errorf("Mockup::keyValueSync() JSON marshal error: %v", err)
	} else {
		err = ioutil.WriteFile(mockupPath, buf, 0666)
		if err != nil {
			return fmt.Errorf("Mockup::keyValueSync() JSON write error: %v", err)
		}
	}
	return nil
}

// keyValueCommitNow - commit key/value insert/update/delete
func keyValueCommitNow(mockup *Mockup, bucket string, object string) error {
	var uri = bucket + "/" + object
	o, exists := mockup.Objects[uri]
	if !exists {
		return fmt.Errorf("Object %s/%s not found", bucket, object)
	}
	for key, value := range o.recent {
		o.KeyValue[key] = value
	}
	for _, key := range o.recentDel {
		delete(o.KeyValue, key)
	}
	if len(o.recentDel) > 0 {
		o.recentDel = nil
	}
	if len(o.recent) > 0 {
		o.recent = make(map[string]string)
	}
	return keyValueSync(mockup)
}

// CloseEdgex - close client connection
func (mockup *Mockup) CloseEdgex() {
	return
}

// BucketCreate - create a new bucket
func (mockup *Mockup) BucketCreate(bucket string) error {
	mockup.lock.Lock()
	defer mockup.lock.Unlock()

	_, exists := mockup.Buckets[bucket]
	if exists {
		return fmt.Errorf("%s bucket already exists", bucket)
	}

	t := time.Now()
	mockup.Buckets[bucket] = s3xApi.Bucket{Name: bucket, CreationDate: t.Format(time.RFC3339)}
	return keyValueSync(mockup)
}

// ObjectCreate - create object
func (mockup *Mockup) ObjectCreate(bucket string, object string, objectType s3xApi.ObjectType,
	contentType string, chunkSize int, btreeOrder int) error {
	mockup.lock.Lock()
	defer mockup.lock.Unlock()

	_, exists := mockup.Buckets[bucket]
	if !exists {
		return fmt.Errorf("%s bucket not found", bucket)
	}

	var uri = bucket + "/" + object
	_, e := mockup.Objects[uri]
	if e {
		return fmt.Errorf("%s/%s already exists", bucket, object)
	}

	var kv kvobj
	kv.KeyValue = make(map[string]string)
	kv.recent = make(map[string]string)
	mockup.Objects[uri] = kv
	return keyValueSync(mockup)
}

// KeyValueCommit - commit key/value insert/update/delete
func (mockup *Mockup) KeyValueCommit(bucket string, object string) error {
	mockup.lock.Lock()
	defer mockup.lock.Unlock()
	return keyValueCommitNow(mockup, bucket, object)
}

// KeyValueRollback - rollback key/value insert/update/delete session
func (mockup *Mockup) KeyValueRollback(bucket string, object string) error {
	mockup.lock.Lock()
	defer mockup.lock.Unlock()
	var uri = bucket + "/" + object
	o, exists := mockup.Objects[uri]
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
	mockup.lock.Lock()
	defer mockup.lock.Unlock()
	var uri = bucket + "/" + object
	delete(mockup.Objects, uri)
	return keyValueSync(mockup)
}

// BucketDelete - delete bucket
func (mockup *Mockup) BucketDelete(bucket string) error {
	mockup.lock.Lock()
	defer mockup.lock.Unlock()
	delete(mockup.Buckets, bucket)
	return keyValueSync(mockup)
}

// ObjectHead - read object header fields
func (mockup *Mockup) ObjectHead(bucket string, object string) error {
	mockup.lock.Lock()
	defer mockup.lock.Unlock()

	var uri = bucket + "/" + object

	_, exists := mockup.Objects[uri]
	if exists {
		return nil
	}
	return s3xErrors.ErrObjectNotExist
}

// BucketHead - read bucket header fields
func (mockup *Mockup) BucketHead(bucket string) error {
	mockup.lock.Lock()
	defer mockup.lock.Unlock()

	_, exists := mockup.Buckets[bucket]
	if exists {
		return nil
	}
	return s3xErrors.ErrBucketNotExist
}

// KeyValuePost - post key/value pairs
func (mockup *Mockup) KeyValuePost(bucket string, object string,
	key string, value *bytes.Buffer, contentType string, more bool) error {
	var uri = bucket + "/" + object
	mockup.lock.Lock()
	defer mockup.lock.Unlock()

	o, exists := mockup.Objects[uri]
	if !exists {
		return fmt.Errorf("Object %s/%s not found", bucket, object)
	}
	if more {
		o.recent[key] = value.String()
	} else {
		keyValueCommitNow(mockup, bucket, object)
		o.KeyValue[key] = value.String()
	}
	return keyValueSync(mockup)
}

// KeyValuePostJSON - post key/value pairs
func (mockup *Mockup) KeyValuePostJSON(bucket string, object string,
	keyValueJSON string, more bool) error {
	var uri = bucket + "/" + object
	mockup.lock.Lock()
	defer mockup.lock.Unlock()

	o, exists := mockup.Objects[uri]
	if !exists {
		return fmt.Errorf("Object %s/%s not found", bucket, object)
	}

	if !more {
		keyValueCommitNow(mockup, bucket, object)
	}

	var result map[string]interface{}
	err := json.Unmarshal([]byte(keyValueJSON), &result)
	if err != nil {
		return fmt.Errorf("Unmarshal error %v", err)
	}

	for key, value := range result {
		if more {
			o.recent[key] = value.(string)
		} else {
			o.KeyValue[key] = value.(string)
		}
	}
	return keyValueSync(mockup)
}

func (mockup *Mockup) KeyValueMapPost(bucket, object string, valuesMap s3xApi.S3xKVMap, more bool) error {
	var uri = bucket + "/" + object
	mockup.lock.Lock()
	defer mockup.lock.Unlock()

	o, exists := mockup.Objects[uri]
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
			o.KeyValue[key] = string(valueMapByte)
		}
	}
	return keyValueSync(mockup)
}

// KeyValuePostCSV - post key/value pairs presented like csv
func (mockup *Mockup) KeyValuePostCSV(bucket string, object string,
	keyValueCSV string, more bool) error {
	mockup.lock.Lock()
	defer mockup.lock.Unlock()

	var uri = bucket + "/" + object

	o, exists := mockup.Objects[uri]
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
			o.KeyValue[kv[0]] = kv[1]
		}
	}
	return keyValueSync(mockup)
}

// KeyValueDelete - delete key/value pair
func (mockup *Mockup) KeyValueDelete(bucket string, object string,
	key string, more bool) error {
	mockup.lock.Lock()
	defer mockup.lock.Unlock()

	var uri = bucket + "/" + object

	o, exists := mockup.Objects[uri]
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
		delete(o.KeyValue, key)
	}
	return keyValueSync(mockup)
}

// KeyValueDeleteJSON - delete key/value pairs defined by json
func (mockup *Mockup) KeyValueMapDelete(bucket string, object string,
	valuesMap s3xApi.S3xKVMap, more bool) error {
	var uri = bucket + "/" + object
	mockup.lock.Lock()
	defer mockup.lock.Unlock()

	o, exists := mockup.Objects[uri]
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
			delete(o.KeyValue, key)
		}
	}
	return keyValueSync(mockup)
}

// KeyValueDeleteJSON - delete key/value pairs defined by json
func (mockup *Mockup) KeyValueDeleteJSON(bucket string, object string,
	keyValueJSON string, more bool) error {
	var uri = bucket + "/" + object
	mockup.lock.Lock()
	defer mockup.lock.Unlock()

	o, exists := mockup.Objects[uri]
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
			delete(o.KeyValue, key)
		}
	}
	return keyValueSync(mockup)
}

// KeyValueGet - read object value field
func (mockup *Mockup) KeyValueGet(bucket string, object string, key string) (string, error) {
	var uri = bucket + "/" + object
	var str string
	mockup.lock.Lock()
	defer mockup.lock.Unlock()

	//fmt.Printf("Objects: %#v\n", mockup.objects)
	o, exists := mockup.Objects[uri]
	if !exists {
		return str, fmt.Errorf("Object %s/%s not found", bucket, object)
	}

	v, e := o.KeyValue[key]
	if !e {
		return str, fmt.Errorf("Object %s/%s key %s not found", bucket, object, key)
	}
	return v, nil
}

// KeyValueList - read key/value pairs, contentType: application/json or text/csv
func (mockup *Mockup) KeyValueList(bucket string, object string,
	from string, pattern string, contentType string, maxcount int, values bool) (string, error) {
	mockup.lock.Lock()
	defer mockup.lock.Unlock()

	var uri = bucket + "/" + object
	var str string

	o, exists := mockup.Objects[uri]
	if !exists {
		return str, fmt.Errorf("Object %s/%s not found", bucket, object)
	}

	keys := make([]string, 0, len(o.KeyValue))

	for k := range o.KeyValue {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b bytes.Buffer

	json := strings.Contains(contentType, "json")

	if json {
		if values {
			b.WriteString("{")
		} else {
			b.WriteString("[")
		}
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

		value, e := o.KeyValue[key]
		if !e {
			continue
		}

		if json {
			if n > 0 {
				b.WriteString(", ")
			}
			b.WriteString(" \"")
			b.WriteString(key)
			b.WriteString("\"")
			if values {
				b.WriteString(": \"")
				b.WriteString(value)
				b.WriteString("\"")
			}
		} else {
			if n > 0 {
				b.WriteString("\n")
			}
			b.WriteString(key)
			if values {
				b.WriteString(";")
				b.WriteString(value)
			}
		}

		n++
		if n == maxcount {
			break
		}
	}

	if json {
		if values {
			b.WriteString("}")
		} else {
			b.WriteString("]")
		}
	}

	return b.String(), nil
}

// BucketList - read bucket list
func (mockup *Mockup) BucketList() ([]s3xApi.Bucket, error) {
	mockup.lock.Lock()
	defer mockup.lock.Unlock()

	keys := make([]string, 0, len(mockup.Buckets))

	for k := range mockup.Buckets {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buckets []s3xApi.Bucket
	for i := range keys {
		key := keys[i]
		buckets = append(buckets, mockup.Buckets[key])
	}
	return buckets, nil
}

// ObjectList - read object list from bucket
func (mockup *Mockup) ObjectList(bucket string,
	from string, pattern string, maxcount int) ([]s3xApi.Object, error) {
	mockup.lock.Lock()
	defer mockup.lock.Unlock()

	var objects []s3xApi.Object
	keys := make([]string, 0, len(mockup.Objects))

	for k := range mockup.Objects {
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
