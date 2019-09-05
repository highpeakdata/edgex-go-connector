package v1beta1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	s3xApi "github.com/Nexenta/edgex-go-connector/api/s3xclient/v1beta1"
	"github.com/Nexenta/edgex-go-connector/pkg/utils"
)

// KeyValueGet - read object value field
func (edgex *Edgex) KeyValueGet(bucket, object, key string) (string, error) {
	objectPath, err := utils.GetObjectPath(bucket, object)
	if err != nil {
		return "", err
	}

	s3xurl := edgex.newS3xURL(objectPath)
	s3xurl.AddOptions(S3XURLOptions{
		"comp": "kvget",
		"key":  key,
	})

	fmt.Printf("KeyValueGet request: %s\n", s3xurl.String())
	res, err := http.Get(s3xurl.String())
	if err != nil {
		fmt.Printf("Object Get error: %v\n", err)
		return "", err
	}

	defer res.Body.Close()
	fmt.Printf("KeyValueGet response: %+v\n", res)

	if res.StatusCode < 300 {
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			fmt.Printf("Object Get read error: %v\n", err)
			return "", err
		}
		return string(body), nil
	}
	if res.StatusCode == 404 {
		return "", fmt.Errorf("Object %s not found", objectPath)
	}
	return "", fmt.Errorf("Object %s get error: %v", objectPath, res)
}

// KeyValuePost - post key/value pairs
func (edgex *Edgex) KeyValuePost(bucket, object, key string, value *bytes.Buffer, contentType string, more bool) error {

	objectPath, err := utils.GetObjectPath(bucket, object)
	if err != nil {
		return err
	}

	s3xurl := edgex.newS3xURL(objectPath)
	s3xurl.AddOptions(S3XURLOptions{
		"comp": "kv",
		"key":  key,
	})

	if !more {
		s3xurl.AddOptions(S3XURLOptions{
			"x-ccow-autocommit": "1",
			"finalize":          "",
		})
	} else {
		s3xurl.AddOptions(S3XURLOptions{
			"x-ccow-autocommit": "0",
		})
	}

	req, err := http.NewRequest("POST", s3xurl.String(), value)
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
	fmt.Printf("KeyValuePost request: %+v\n", req)
	res, err := edgex.httpClient.Do(req)
	if err != nil {
		fmt.Printf("k/v post error: %v\n", err)
		return err
	}
	defer res.Body.Close()
	fmt.Printf("KeyValuePost response: %+v\n", res)
	if res.StatusCode < 300 {
		sid := res.Header.Get("X-Session-Id")
		edgex.Sid = sid
		return nil
	}
	return fmt.Errorf("%s post status code: %v", objectPath, res.StatusCode)
}

// KeyValuePostMap - post key/value map in JSON format
func (edgex *Edgex) KeyValueMapPost(bucket, object string, values s3xApi.S3xKVMap, more bool) error {
	objectPath, err := utils.GetObjectPath(bucket, object)
	if err != nil {
		return err
	}

	s3xurl := edgex.newS3xURL(objectPath)
	s3xurl.AddOptions(S3XURLOptions{
		"comp": "kv",
	})

	if !more {
		s3xurl.AddOptions(S3XURLOptions{
			"x-ccow-autocommit": "1",
			"finalize":          "",
		})
	} else {
		s3xurl.AddOptions(S3XURLOptions{
			"x-ccow-autocommit": "0",
		})
	}
	jsonBytes, err := json.Marshal(values)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", s3xurl.String(), bytes.NewBuffer(jsonBytes))
	if err != nil {
		fmt.Printf("k/v create key/value json post error: %v\n", err)
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Length", strconv.Itoa(len(jsonBytes)))
	if edgex.Sid != "" {
		req.Header.Add("x-session-id", edgex.Sid)
	}

	fmt.Printf("KeyValuePostMap request: %+v\n", req)
	res, err := edgex.httpClient.Do(req)
	if err != nil {
		fmt.Printf("k/v json post error: %v\n", err)
		return err
	}
	defer res.Body.Close()
	fmt.Printf("KeyValuePostMap response: %+v\n", res)

	if res.StatusCode < 300 {
		sid := res.Header.Get("X-Session-Id")
		edgex.Sid = sid
		return nil
	}
	return fmt.Errorf("%s json post status code: %v", objectPath, res.StatusCode)
}

// KeyValueMapDelete - delete key/value map in JSON format
func (edgex *Edgex) KeyValueMapDelete(bucket, object string, values s3xApi.S3xKVMap, more bool) error {
	objectPath, err := utils.GetObjectPath(bucket, object)
	if err != nil {
		return err
	}
	s3xurl := edgex.newS3xURL(objectPath)
	s3xurl.AddOptions(S3XURLOptions{
		"comp": "kv",
	})

	if !more {
		s3xurl.AddOptions(S3XURLOptions{
			"x-ccow-autocommit": "1",
			"finalize":          "",
		})
	} else {
		s3xurl.AddOptions(S3XURLOptions{
			"x-ccow-autocommit": "0",
		})
	}

	jsonBytes, err := json.Marshal(values)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("DELETE", s3xurl.String(), bytes.NewBuffer(jsonBytes))
	if err != nil {
		fmt.Printf("k/v delete key/value json post error: %v\n", err)
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Length", strconv.Itoa(len(jsonBytes)))
	if edgex.Sid != "" {
		req.Header.Add("x-session-id", edgex.Sid)
	}
	res, err := edgex.httpClient.Do(req)
	if err != nil {
		fmt.Printf("k/v json delete error: %v\n", err)
		return err
	}
	defer res.Body.Close()

	if res.StatusCode < 300 {
		sid := res.Header.Get("X-Session-Id")
		edgex.Sid = sid
		return nil
	}
	return fmt.Errorf("%s json delete status code: %v", objectPath, res.StatusCode)
}

// KeyValuePostJSON - post key/value pairs
func (edgex *Edgex) KeyValuePostJSON(bucket, object, keyValueJSON string, more bool) error {
	objectPath, err := utils.GetObjectPath(bucket, object)
	if err != nil {
		return err
	}

	s3xurl := edgex.newS3xURL(objectPath)
	s3xurl.AddOptions(S3XURLOptions{
		"comp": "kv",
	})

	if !more {
		s3xurl.AddOptions(S3XURLOptions{
			"x-ccow-autocommit": "1",
			"finalize":          "",
		})
	} else {
		s3xurl.AddOptions(S3XURLOptions{
			"x-ccow-autocommit": "0",
		})
	}

	req, err := http.NewRequest("POST", s3xurl.String(), bytes.NewBufferString(keyValueJSON))
	if err != nil {
		fmt.Printf("k/v create key/value json post error: %v\n", err)
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Length", strconv.Itoa(len(keyValueJSON)))
	if edgex.Sid != "" {
		req.Header.Add("x-session-id", edgex.Sid)
	}

	fmt.Printf("KeyValuePostJSON request: %+v\n", req)
	res, err := edgex.httpClient.Do(req)
	if err != nil {
		fmt.Printf("k/v json post error: %v\n", err)
		return err
	}
	defer res.Body.Close()
	fmt.Printf("KeyValuePostJSON response: %+v\n", res)

	if res.StatusCode < 300 {
		sid := res.Header.Get("X-Session-Id")
		edgex.Sid = sid
		return nil
	}
	return fmt.Errorf("%s json post status code: %v", objectPath, res.StatusCode)
}

// KeyValuePostCSV - post key/value pairs presented like csv
func (edgex *Edgex) KeyValuePostCSV(bucket, object, keyValueCSV string, more bool) error {

	objectPath, err := utils.GetObjectPath(bucket, object)
	if err != nil {
		return err
	}

	s3xurl := edgex.newS3xURL(objectPath)
	s3xurl.AddOptions(S3XURLOptions{
		"comp": "kv",
	})

	if !more {
		s3xurl.AddOptions(S3XURLOptions{
			"x-ccow-autocommit": "1",
			"finalize":          "",
		})
	} else {
		s3xurl.AddOptions(S3XURLOptions{
			"x-ccow-autocommit": "0",
		})
	}

	req, err := http.NewRequest("POST", s3xurl.String(), bytes.NewBufferString(keyValueCSV))
	if err != nil {
		fmt.Printf("k/v create key/value csv post error: %v\n", err)
		return err
	}

	req.Header.Add("Content-Type", "text/csv")
	req.Header.Add("Content-Length", strconv.Itoa(len(keyValueCSV)))
	if edgex.Sid != "" {
		req.Header.Add("x-session-id", edgex.Sid)
	}
	res, err := edgex.httpClient.Do(req)
	if err != nil {
		fmt.Printf("k/v csv post error: %v\n", err)
		return err
	}
	defer res.Body.Close()

	if res.StatusCode < 300 {
		sid := res.Header.Get("X-Session-Id")
		edgex.Sid = sid
		return nil
	}
	return fmt.Errorf("%s csv post status code: %v", objectPath, res.StatusCode)
}

// KeyValueDelete - delete key/value pair
func (edgex *Edgex) KeyValueDelete(bucket, object, key string, more bool) error {
	objectPath, err := utils.GetObjectPath(bucket, object)
	if err != nil {
		return err
	}
	s3xurl := edgex.newS3xURL(objectPath)
	s3xurl.AddOptions(S3XURLOptions{
		"comp": "kv",
		"key":  key,
	})

	if !more {
		s3xurl.AddOptions(S3XURLOptions{
			"x-ccow-autocommit": "1",
			"finalize":          "",
		})
	} else {
		s3xurl.AddOptions(S3XURLOptions{
			"x-ccow-autocommit": "0",
		})
	}

	req, err := http.NewRequest("DELETE", s3xurl.String(), nil)
	if err != nil {
		fmt.Printf("k/v delete key/value error: %v\n", err)
		return err
	}

	req.Header.Add("Content-Type", "application/octet-stream")
	req.Header.Add("Content-Length", "0")
	if edgex.Sid != "" {
		req.Header.Add("x-session-id", edgex.Sid)
	}
	res, err := edgex.httpClient.Do(req)
	if err != nil {
		fmt.Printf("k/v delete error: %v\n", err)
		return err
	}
	defer res.Body.Close()

	if res.StatusCode < 300 {
		sid := res.Header.Get("X-Session-Id")
		edgex.Sid = sid
		return nil
	}
	return fmt.Errorf("%s delete status code: %v", objectPath, res.StatusCode)
}

// KeyValueDeleteJSON - delete key/value pairs defined by json
func (edgex *Edgex) KeyValueDeleteJSON(bucket, object, keyValueJSON string, more bool) error {

	objectPath, err := utils.GetObjectPath(bucket, object)
	if err != nil {
		return err
	}
	s3xurl := edgex.newS3xURL(objectPath)
	s3xurl.AddOptions(S3XURLOptions{
		"comp": "kv",
	})

	if !more {
		s3xurl.AddOptions(S3XURLOptions{
			"x-ccow-autocommit": "1",
			"finalize":          "",
		})
	} else {
		s3xurl.AddOptions(S3XURLOptions{
			"x-ccow-autocommit": "0",
		})
	}

	req, err := http.NewRequest("DELETE", s3xurl.String(), bytes.NewBufferString(keyValueJSON))
	if err != nil {
		fmt.Printf("k/v create key/value json post error: %v\n", err)
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Length", strconv.Itoa(len(keyValueJSON)))
	if edgex.Sid != "" {
		req.Header.Add("x-session-id", edgex.Sid)
	}
	res, err := edgex.httpClient.Do(req)
	if err != nil {
		fmt.Printf("k/v json delete error: %v\n", err)
		return err
	}
	defer res.Body.Close()

	if res.StatusCode < 300 {
		sid := res.Header.Get("X-Session-Id")
		edgex.Sid = sid
		return nil
	}
	return fmt.Errorf("%s json delete status code: %v", objectPath, res.StatusCode)
}

// KeyValueList - read key/value pairs, contentType: application/json or text/csv
func (edgex *Edgex) KeyValueList(bucket, object, from, pattern, contentType string, maxcount int, values bool) (string, error) {

	objectPath, err := utils.GetObjectPath(bucket, object)
	if err != nil {
		return "", err
	}
	s3xurl := edgex.newS3xURL(objectPath)
	s3xurl.AddOptions(S3XURLOptions{
		"comp": "kv",
	})

	if from != "" {
		s3xurl.AddOptions(S3XURLOptions{
			"key": from,
		})
	}

	if pattern != "" {
		s3xurl.AddOptions(S3XURLOptions{
			"pattern": pattern,
		})
	}

	if maxcount > 0 {
		s3xurl.AddOptions(S3XURLOptions{
			"maxresults": strconv.Itoa(maxcount),
		})

	}

	if values {
		s3xurl.AddOptions(S3XURLOptions{
			"values": "1",
		})
	}

	req, err := http.NewRequest("GET", s3xurl.String(), nil)
	if err != nil {
		fmt.Printf("k/v create key/value list error: %v\n", err)
		return "", err
	}

	req.Header.Add("Content-Type", contentType)
	req.Header.Add("Content-Length", "0")
	res, err := edgex.httpClient.Do(req)
	if err != nil {
		fmt.Printf("k/v list error: %v\n", err)
		return "", err
	}
	defer res.Body.Close()

	if res.StatusCode < 300 {
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			fmt.Printf("Object key/value list read error: %v\n", err)
			return "", err
		}
		return string(body), nil
	}
	if res.StatusCode == 404 {
		return "", fmt.Errorf("Object %s not found", objectPath)
	}
	return "", fmt.Errorf("Object %s list error: %v", objectPath, res)
}
