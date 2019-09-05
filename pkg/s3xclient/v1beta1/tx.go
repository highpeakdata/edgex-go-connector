package v1beta1

import (
	"bytes"
	"fmt"
	"net/http"
	"strconv"

	"github.com/Nexenta/edgex-go-connector/pkg/utils"
)

// KeyValueCommit - commit key/value insert/update/delete
func (edgex *Edgex) KeyValueCommit(bucket string, object string) error {

	objectPath, err := utils.GetObjectPath(bucket, object)
	if err != nil {
		return err
	}

	s3xurl := edgex.newS3xURL(objectPath)
	s3xurl.AddOptions(S3XURLOptions{
		"comp":              "kv",
		"x-ccow-autocommit": "0",
		"finalize":          "",
	})

	kvjson := "{}"

	client := &http.Client{}
	req, err := http.NewRequest("POST", s3xurl.String(), bytes.NewBufferString(kvjson))
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

	fmt.Printf("k/v commit post result %v\n", res)

	edgex.Sid = ""
	if res.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("%s/%s commit post status code: %v", bucket, object, res.StatusCode)
}

// KeyValueRollback - rollback key/value insert/update/delete session
func (edgex *Edgex) KeyValueRollback(bucket string, object string) error {
	objectPath, err := utils.GetObjectPath(bucket, object)
	if err != nil {
		return err
	}

	s3xurl := edgex.newS3xURL(objectPath)
	s3xurl.AddOptions(S3XURLOptions{
		"comp":              "kv",
		"x-ccow-autocommit": "0",
		"cancel":            "1",
	})

	kvjson := "{}"

	client := &http.Client{}
	req, err := http.NewRequest("POST", s3xurl.String(), bytes.NewBufferString(kvjson))
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

	fmt.Printf("k/v rollback post result %v\n", res)

	edgex.Sid = ""
	if res.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("%s rollback post status code: %v", objectPath, res.StatusCode)
}

// Finalize - close client connection
func (edgex *Edgex) Close(bucket, object string) error {
	objectPath, err := utils.GetObjectPath(bucket, object)
	if err != nil {
		return err
	}

	fmt.Printf("Closing connection to %s\n", objectPath)

	s3xurl := edgex.newS3xURL(objectPath)
	s3xurl.AddOptions(S3XURLOptions{
		"comp":     "streamsession",
		"finalize": "",
	})

	http.Head(s3xurl.String())

	return nil
}
