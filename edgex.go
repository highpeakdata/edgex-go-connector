package edgex

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
)

const DEFAULT_CHUNKSIZE int = 4096
const DEFAULT_BTREE_ORDER int = 4

const SS_CONT int = 0x00
const SS_FIN int = 0x01
const SS_APPEND int = 0x02
const SS_RANDWR int = 0x04
const SS_KV int = 0x08
const SS_STAT int = 0x10
const CCOW_O_REPLACE int = 0x01
const CCOW_O_CREATE int = 0x02
const BYTE_BUFFER int = 16 * 1024

// Edgex - Edgex client structure
type Edgex struct {
	Url string

	// s3 authentication keys
	Authkey string
	Secret  string

	// Current session
	Bucket string
	Object string
	Sid    string
	Value  string
	Debug  int
}

// EdgexMockup - Edgex client mockup structure
type EdgexMockup struct {

	// Current session
	Bucket string
	Object string
	Sid    string
	Value  string
	Debug  int
}

// CreateEdgex - client structure constructorcd
func CreateEdgex(url, authkey, secret string, debug int) *Edgex {
	edgex := new(Edgex)
	edgex.Url = url
	edgex.Authkey = authkey
	edgex.Secret = secret
	edgex.Debug = debug
	edgex.Sid = ""
	edgex.Bucket = ""
	edgex.Object = ""
	edgex.Value = ""
	return edgex
}

// CloseEdgex - close client connection
func CloseEdgex(edgex *Edgex) {
	if edgex.Bucket != "" && edgex.Object != "" {
		if edgex.Debug > 0 {
			fmt.Printf("Closing connection to %s/%s\n", edgex.Bucket, edgex.Object)
		}
		var url = edgex.Url + "/" + edgex.Bucket + "/" + edgex.Object
		url += "?comp=streamsession&finalize"
		http.Head(url)
	}
	return
}

// GetValue - get last result value
func (edgex *Edgex) GetLastValue() string {
	return edgex.Value
}

// BucketCreate - create a new bucket
func (edgex *Edgex) BucketCreate(bucket string) error {
	var url = edgex.Url + "/" + bucket
	edgex.Bucket = bucket

	client := &http.Client{}
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		fmt.Printf("k/v create bucket error: %v\n", err)
		return err
	}

	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("k/v bucket create error: %v\n", err)
		return err
	}
	if res.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("%s bucket create status code: %v", bucket, res.StatusCode)
}

// KeyValueCreate - create key/value object
func (edgex *Edgex) KeyValueCreate(bucket string, object string,
	contentType string, chunkSize int, btreeOrder int) error {
	var url = edgex.Url + "/" + bucket + "/" + object + "?comp=kv&finalize"
	edgex.Bucket = bucket
	edgex.Object = object

	client := &http.Client{}
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		fmt.Printf("k/v create post error: %v\n", err)
		return err
	}

	req.Header.Add("Content-Type", contentType)
	req.Header.Add("Content-Length", "0")
	req.Header.Add("x-ccow-object-oflags", strconv.Itoa(CCOW_O_CREATE|CCOW_O_REPLACE))
	req.Header.Add("x-ccow-chunkmap-btree-order", strconv.Itoa(btreeOrder))
	req.Header.Add("x-ccow-chunkmap-chunk-size", strconv.Itoa(chunkSize))

	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("k/v create error: %v\n", err)
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("%s/%s create status code: %v", bucket, object, res.StatusCode)
}

// ObjectDelete - delete object
func (edgex *Edgex) ObjectDelete(bucket string, object string) error {
	var url = edgex.Url + "/" + bucket + "/" + object + "?comp=del"
	edgex.Bucket = bucket
	edgex.Object = object

	client := &http.Client{}
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		fmt.Printf("k/v create object delete error: %v\n", err)
		return err
	}

	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("k/v object delete error: %v\n", err)
		return err
	}
	if res.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("%s/%s object delete status code: %v", bucket, object, res.StatusCode)
}

// BucketDelete - delete bucket
func (edgex *Edgex) BucketDelete(bucket string) error {
	var url = edgex.Url + "/" + bucket
	edgex.Bucket = bucket

	client := &http.Client{}
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		fmt.Printf("k/v create bucket delete error: %v\n", err)
		return err
	}

	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("k/v bucket delete error: %v\n", err)
		return err
	}
	if res.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("%s bucket delete status code: %v", bucket, res.StatusCode)
}

// ObjectHead - read object header fields
func (edgex *Edgex) ObjectHead(bucket string, object string) error {
	var url = edgex.Url + "/" + bucket + "/" + object
	url += "?comp=streamsession&finalize"
	edgex.Bucket = bucket
	edgex.Object = object

	res, err := http.Head(url)
	if err != nil {
		fmt.Printf("Object Head error: %v\n", err)
		return err
	}

	if edgex.Debug > 0 {
		fmt.Printf("Object Head %v\n", res)
	}

	if res.StatusCode < 300 {
		return nil
	}
	if res.StatusCode == 404 {
		return fmt.Errorf("Object %s/%s not found", bucket, object)
	}
	return fmt.Errorf("Object %s/%s head error: %v", bucket, object, res)
}

// BucketHead - read bucket header fields
func (edgex *Edgex) BucketHead(bucket string) error {
	var url = edgex.Url + "/" + bucket
	url += "?comp=streamsession&finalize"
	edgex.Bucket = bucket

	res, err := http.Head(url)
	if err != nil {
		fmt.Printf("Bucket Head error: %v\n", err)
		return err
	}

	if edgex.Debug > 0 {
		fmt.Printf("Bucket Head %v\n", res)
	}

	if res.StatusCode < 300 {
		return nil
	}
	if res.StatusCode == 404 {
		return fmt.Errorf("Bucket %s not found", bucket)
	}
	return fmt.Errorf("Bucket %s head error: %v", bucket, res)
}

// KeyValuePost - post key/value pairs
func (edgex *Edgex) KeyValuePost(bucket string, object string, contentType string,
	key string, value *bytes.Buffer, more bool) error {
	var url = edgex.Url + "/" + bucket + "/" + object + "?comp=kv&key=" + key
	edgex.Bucket = bucket
	edgex.Object = object

	if !more {
		url += "&x-ccow-autocommit=1&finalize"
	} else {
		url += "&x-ccow-autocommit=0"
	}

	client := &http.Client{}
	req, err := http.NewRequest("POST", url, value)
	if err != nil {
		fmt.Printf("k/v create key/value post error: %v\n", err)
		return err
	}

	if contentType != "" {
		req.Header.Add("Content-Type", contentType)
	} else {
		req.Header.Add("Content-Type", "application/octet-stream")
	}
	req.Header.Add("Content-Length", strconv.Itoa(value.Len()))
	if edgex.Sid != "" {
		req.Header.Add("x-session-id", edgex.Sid)
	}
	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("k/v post error: %v\n", err)
		return err
	}
	defer res.Body.Close()
	if edgex.Debug > 0 {
		fmt.Printf("k/v post result %v\n", res)
	}
	if res.StatusCode < 300 {
		sid := res.Header.Get("X-Session-Id")
		edgex.Sid = sid
		return nil
	}
	return fmt.Errorf("%s/%s post status code: %v", bucket, object, res.StatusCode)
}

// KeyValuePostJSON - post key/value pairs
func (edgex *Edgex) KeyValuePostJSON(bucket string, object string,
	keyValueJSON string, more bool) error {
	var url = edgex.Url + "/" + bucket + "/" + object + "?comp=kv"
	edgex.Bucket = bucket
	edgex.Object = object

	if !more {
		url += "&x-ccow-autocommit=1&finalize"
	} else {
		url += "&x-ccow-autocommit=0"
	}

	client := &http.Client{}
	req, err := http.NewRequest("POST", url, bytes.NewBufferString(keyValueJSON))
	if err != nil {
		fmt.Printf("k/v create key/value json post error: %v\n", err)
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Length", strconv.Itoa(len(keyValueJSON)))
	if edgex.Sid != "" {
		req.Header.Add("x-session-id", edgex.Sid)
	}
	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("k/v json post error: %v\n", err)
		return err
	}
	defer res.Body.Close()
	if edgex.Debug > 0 {
		fmt.Printf("k/v json post result %v\n", res)
	}
	if res.StatusCode < 300 {
		sid := res.Header.Get("X-Session-Id")
		edgex.Sid = sid
		return nil
	}
	return fmt.Errorf("%s/%s json post status code: %v", bucket, object, res.StatusCode)
}

// KeyValuePostCSV - post key/value pairs presented like csv
func (edgex *Edgex) KeyValuePostCSV(bucket string, object string,
	keyValueCSV string, more bool) error {
	var url = edgex.Url + "/" + bucket + "/" + object + "?comp=kv"
	edgex.Bucket = bucket
	edgex.Object = object

	if !more {
		url += "&x-ccow-autocommit=1&finalize"
	} else {
		url += "&x-ccow-autocommit=0"
	}

	client := &http.Client{}
	req, err := http.NewRequest("POST", url, bytes.NewBufferString(keyValueCSV))
	if err != nil {
		fmt.Printf("k/v create key/value csv post error: %v\n", err)
		return err
	}

	req.Header.Add("Content-Type", "text/csv")
	req.Header.Add("Content-Length", strconv.Itoa(len(keyValueCSV)))
	if edgex.Sid != "" {
		req.Header.Add("x-session-id", edgex.Sid)
	}
	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("k/v csv post error: %v\n", err)
		return err
	}
	defer res.Body.Close()
	if edgex.Debug > 0 {
		fmt.Printf("k/v csv post result %v\n", res)
	}
	if res.StatusCode < 300 {
		sid := res.Header.Get("X-Session-Id")
		edgex.Sid = sid
		return nil
	}
	return fmt.Errorf("%s/%s csv post status code: %v", bucket, object, res.StatusCode)
}

// KeyValueDelete - delete key/value pair
func (edgex *Edgex) KeyValueDelete(bucket string, object string,
	key string, more bool) error {
	var url = edgex.Url + "/" + bucket + "/" + object + "?comp=kv&key=" + key
	edgex.Bucket = bucket
	edgex.Object = object

	if !more {
		url += "&x-ccow-autocommit=1&finalize"
	} else {
		url += "&x-ccow-autocommit=0"
	}

	client := &http.Client{}
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		fmt.Printf("k/v delete key/value error: %v\n", err)
		return err
	}

	req.Header.Add("Content-Type", "application/octet-stream")
	req.Header.Add("Content-Length", "0")
	if edgex.Sid != "" {
		req.Header.Add("x-session-id", edgex.Sid)
	}
	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("k/v delete error: %v\n", err)
		return err
	}
	defer res.Body.Close()
	if edgex.Debug > 0 {
		fmt.Printf("k/v delete result %v\n", res)
	}
	if res.StatusCode < 300 {
		sid := res.Header.Get("X-Session-Id")
		edgex.Sid = sid
		return nil
	}
	return fmt.Errorf("%s/%s delete status code: %v", bucket, object, res.StatusCode)
}

// KeyValueDeleteJSON - delete key/value pairs defined by json
func (edgex *Edgex) KeyValueDeleteJSON(bucket string, object string,
	keyValueJSON string, more bool) error {
	var url = edgex.Url + "/" + bucket + "/" + object + "?comp=kv"
	edgex.Bucket = bucket
	edgex.Object = object

	if !more {
		url += "&x-ccow-autocommit=1&finalize"
	} else {
		url += "&x-ccow-autocommit=0"
	}

	client := &http.Client{}
	req, err := http.NewRequest("DELETE", url, bytes.NewBufferString(keyValueJSON))
	if err != nil {
		fmt.Printf("k/v create key/value json post error: %v\n", err)
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Length", strconv.Itoa(len(keyValueJSON)))
	if edgex.Sid != "" {
		req.Header.Add("x-session-id", edgex.Sid)
	}
	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("k/v json delete error: %v\n", err)
		return err
	}
	defer res.Body.Close()
	if edgex.Debug > 0 {
		fmt.Printf("k/v json delete result %v\n", res)
	}
	if res.StatusCode < 300 {
		sid := res.Header.Get("X-Session-Id")
		edgex.Sid = sid
		return nil
	}
	return fmt.Errorf("%s/%s json delete status code: %v", bucket, object, res.StatusCode)
}

// KeyValueCommit - commit key/value insert/update/delete
func (edgex *Edgex) KeyValueCommit(bucket string, object string) error {
	var url = edgex.Url + "/" + bucket + "/" + object + "?comp=kv"
	edgex.Bucket = bucket
	edgex.Object = object

	url += "&x-ccow-autocommit=1&finalize"

	kvjson := "{}"

	client := &http.Client{}
	req, err := http.NewRequest("POST", url, bytes.NewBufferString(kvjson))
	if err != nil {
		fmt.Printf("k/v create key/value commit post error: %v\n", err)
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Length", strconv.Itoa(len(kvjson)))
	if edgex.Sid != "" {
		req.Header.Add("x-session-id", edgex.Sid)
	}
	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("k/v commit post error: %v\n", err)
		return err
	}
	defer res.Body.Close()
	if edgex.Debug > 0 {
		fmt.Printf("k/v commit post result %v\n", res)
	}
	edgex.Sid = ""
	if res.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("%s/%s commit post status code: %v", bucket, object, res.StatusCode)
}

// KeyValueRollback - rollback key/value insert/update/delete session
func (edgex *Edgex) KeyValueRollback(bucket string, object string) error {
	var url = edgex.Url + "/" + bucket + "/" + object + "?comp=kv"
	edgex.Bucket = bucket
	edgex.Object = object

	url += "&x-ccow-autocommit=0&cancel=1"

	kvjson := "{}"

	client := &http.Client{}
	req, err := http.NewRequest("POST", url, bytes.NewBufferString(kvjson))
	if err != nil {
		fmt.Printf("k/v create key/value rollback post error: %v\n", err)
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Length", strconv.Itoa(len(kvjson)))
	if edgex.Sid != "" {
		req.Header.Add("x-session-id", edgex.Sid)
	}
	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("k/v rollback post error: %v\n", err)
		return err
	}
	defer res.Body.Close()
	if edgex.Debug > 0 {
		fmt.Printf("k/v rollback post result %v\n", res)
	}
	edgex.Sid = ""
	if res.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("%s/%s rollback post status code: %v", bucket, object, res.StatusCode)
}

// KeyValueGet - read object value field
func (edgex *Edgex) KeyValueGet(bucket string, object string, key string) error {
	var url = edgex.Url + "/" + bucket + "/" + object + "?comp=kvget&key=" + key
	edgex.Bucket = bucket
	edgex.Object = object

	res, err := http.Get(url)
	if err != nil {
		fmt.Printf("Object Get error: %v\n", err)
		return err
	}

	defer res.Body.Close()

	if res.StatusCode < 300 {
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			fmt.Printf("Object Get read error: %v\n", err)
			return err
		}
		edgex.Value = string(body)
		return nil
	}
	if res.StatusCode == 404 {
		return fmt.Errorf("Object %s/%s not found", bucket, object)
	}
	return fmt.Errorf("Object %s/%s get error: %v", bucket, object, res)
}

// KeyValueList - read key/value pairs, contentType: application/json or text/csv
func (edgex *Edgex) KeyValueList(bucket string, object string,
	from string, pattern string, contentType string, maxcount int, values bool) error {
	var url = edgex.Url + "/" + bucket + "/" + object + "?comp=kv"
	edgex.Bucket = bucket
	edgex.Object = object

	if from != "" {
		url += "&key=" + from
	}

	if pattern != "" {
		url += "&pattern=" + pattern
	}

	if maxcount > 0 {
		url += "&maxresults=" + strconv.Itoa(maxcount)
	}

	if values {
		url += "&values=1"
	}

	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Printf("k/v create key/value list error: %v\n", err)
		return err
	}

	req.Header.Add("Content-Type", contentType)
	req.Header.Add("Content-Length", "0")
	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("k/v list error: %v\n", err)
		return err
	}
	defer res.Body.Close()

	if res.StatusCode < 300 {
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			fmt.Printf("Object key/value list read error: %v\n", err)
			return err
		}
		edgex.Value = string(body)
		return nil
	}
	if res.StatusCode == 404 {
		return fmt.Errorf("Object %s/%s not found", bucket, object)
	}
	return fmt.Errorf("Object %s/%s list error: %v", bucket, object, res)
}
