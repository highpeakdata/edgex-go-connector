package edgex

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type kvobj struct {
	keyValue map[string]string
}

// Mockup - mockup client mockup structure
type Mockup struct {
	objects map[string]kvobj
	buckets map[string]int

	// Current session
	Bucket string
	Object string
	Sid    string
	Value  string
	Debug  int
}

// Createmockup - client structure constructorcd
func CreateMockup(debug int) *Mockup {
	mockup := new(Mockup)
	mockup.buckets = make(map[string]int)
	mockup.objects = make(map[string]kvobj)
	mockup.Debug = debug
	mockup.Sid = ""
	mockup.Bucket = ""
	mockup.Object = ""
	mockup.Value = ""
	return mockup
}

// GetValue - get last result value
func (mockup *Mockup) GetLastValue() string {
	return mockup.Value
}

// BucketCreate - create a new bucket
func (mockup *Mockup) BucketCreate(bucket string) error {
	_, exists := mockup.buckets[bucket]
	if exists {
		return fmt.Errorf("%s bucket already exists", bucket)
	}

	mockup.buckets[bucket] = 1
	return nil
}

// KeyValueCreate - create key/value object
func (mockup *Mockup) KeyValueCreate(bucket string, object string,
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
	mockup.objects[uri] = kv
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

// KeyValuePost - post key/value pairs
func (mockup *Mockup) KeyValuePost(bucket string, object string, contentType string,
	key string, value string, more bool) error {
	var uri = bucket + "/" + object

	o, exists := mockup.objects[uri]
	if !exists {
		return fmt.Errorf("Object %s/%s not found", bucket, object)
	}
	o.keyValue[key] = value
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

	var result map[string]interface{}
	json.Unmarshal([]byte(keyValueJSON), &result)

	for key, value := range result {
		o.keyValue[key] = value.(string)
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

	result := strings.Split(keyValueCSV, "\n")

	for _, s := range result {
		kv := strings.Split(s, ";")
		if len(kv) < 2 {
			continue
		}
		o.keyValue[kv[0]] = kv[1]
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

	delete(o.keyValue, key)
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

	var result map[string]interface{}
	json.Unmarshal([]byte(keyValueJSON), &result)

	for key, _ := range result {
		delete(o.keyValue, key)
	}
	return nil
}

// KeyValueGet - read object value field
func (mockup *Mockup) KeyValueGet(bucket string, object string, key string) error {
	var uri = bucket + "/" + object

	o, exists := mockup.objects[uri]
	if !exists {
		return fmt.Errorf("Object %s/%s not found", bucket, object)
	}

	v, e := o.keyValue[key]
	if !e {
		return fmt.Errorf("Object %s/%s key %s found", bucket, object, key)
	}
	mockup.Value = v
	return nil
}

// KeyValueList - read key/value pairs, contentType: application/json or text/csv
func (mockup *Mockup) KeyValueList(bucket string, object string,
	from string, pattern string, contentType string, maxcount int, values bool) error {

	var uri = bucket + "/" + object

	o, exists := mockup.objects[uri]
	if !exists {
		return fmt.Errorf("Object %s/%s not found", bucket, object)
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

	mockup.Value = b.String()
	return nil
}