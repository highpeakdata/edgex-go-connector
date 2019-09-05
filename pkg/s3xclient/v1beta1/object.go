package v1beta1

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	s3xApi "github.com/Nexenta/edgex-go-connector/api/s3xclient/v1beta1"
	s3xErrors "github.com/Nexenta/edgex-go-connector/pkg/errors"

	"github.com/Nexenta/edgex-go-connector/pkg/utils"
)

// ObjectCreate - create key/value object
func (edgex *Edgex) ObjectCreate(bucket, object string, objectType s3xApi.ObjectType, contentType string, chunkSize int, btreeOrder int) error {

	objectPath, err := utils.GetObjectPath(bucket, object)
	if err != nil {
		return err
	}

	s3xurl := edgex.newS3xURL(objectPath)
	if objectType == s3xApi.OBJECT_TYPE_KEY_VALUE {
		s3xurl.AddOptions(S3XURLOptions{
			"comp":     "kv",
			"finalize": "",
		})
	} else {
		s3xurl.AddOptions(S3XURLOptions{
			"comp":     "streamsession",
			"finalize": "",
		})

	}

	req, err := http.NewRequest("POST", s3xurl.String(), nil)
	if err != nil {
		fmt.Printf("k/v create post error: %v\n", err)
		return err
	}

	req.Header.Add("Content-Type", contentType)
	req.Header.Add("Content-Length", "0")
	req.Header.Add("x-ccow-object-oflags", strconv.Itoa(s3xApi.CCOW_O_CREATE|s3xApi.CCOW_O_REPLACE))
	req.Header.Add("x-ccow-chunkmap-btree-order", strconv.Itoa(btreeOrder))
	req.Header.Add("x-ccow-chunkmap-chunk-size", strconv.Itoa(chunkSize))

	res, err := edgex.httpClient.Do(req)
	if err != nil {
		fmt.Printf("k/v create error: %v\n", err)
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("%s create status code: %v", objectPath, res.StatusCode)
}

// ObjectDelete - delete object
func (edgex *Edgex) ObjectDelete(bucket, object string) error {

	objectPath, err := utils.GetObjectPath(bucket, object)
	if err != nil {
		return err
	}

	s3xurl := edgex.newS3xURL(objectPath)
	s3xurl.AddOptions(S3XURLOptions{
		"comp": "del",
	})

	req, err := http.NewRequest("DELETE", s3xurl.String(), nil)
	if err != nil {
		fmt.Printf("k/v create object delete error: %v\n", err)
		return err
	}

	res, err := edgex.httpClient.Do(req)
	if err != nil {
		fmt.Printf("k/v object delete error: %v\n", err)
		return err
	}
	if res.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("%s object delete status code: %v", objectPath, res.StatusCode)
}

// ObjectHead - read object header fields
func (edgex *Edgex) ObjectHead(bucket, object string) error {
	objectPath, err := utils.GetObjectPath(bucket, object)
	if err != nil {
		return err
	}

	s3xurl := edgex.newS3xURL(objectPath)
	s3xurl.AddOptions(S3XURLOptions{
		"comp":     "streamsession",
		"finalize": "",
	})

	res, err := http.Head(s3xurl.String())
	if err != nil {
		fmt.Printf("Object Head error: %v\n", err)
		return err
	}

	if res.StatusCode < 300 {
		return nil
	}
	if res.StatusCode == 404 {
		return s3xErrors.ErrObjectNotExist
	}
	return fmt.Errorf("Object %s/ head error: %v", objectPath, err)
}

func (edgex *Edgex) ObjectList(bucket, from, pattern string, maxcount int) ([]s3xApi.Object, error) {
	bucketPath, err := utils.GetBucketPath(bucket)
	if err != nil {
		return nil, err
	}
	s3xurl := edgex.newS3xURL(bucketPath)

	var list s3xApi.ListBucketResult

	if from != "" {
		s3xurl.AddOptions(S3XURLOptions{
			"marker": from,
		})
	}

	if pattern != "" {
		s3xurl.AddOptions(S3XURLOptions{
			"prefix": pattern,
		})
	}

	if maxcount > 0 {
		s3xurl.AddOptions(S3XURLOptions{
			"max-keys": strconv.Itoa(maxcount),
		})

	}

	client := &http.Client{}
	req, err := http.NewRequest("GET", s3xurl.String(), nil)
	if err != nil {
		fmt.Printf("Object list error: %v\n", err)
		return list.Objects, err
	}

	fmt.Printf("ObjectList request: %+v", req)
	req.Header.Add("Content-Length", "0")
	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("Object list error: %v\n", err)
		return list.Objects, err
	}
	defer res.Body.Close()

	fmt.Printf("ObjectList response: %+v", res)
	if res.StatusCode < 300 {
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			fmt.Printf("Object list read error: %v\n", err)
			return list.Objects, err
		}
		err = xml.Unmarshal(body, &list)
		return list.Objects, err
	}
	return list.Objects, fmt.Errorf("Bucket %s list error: %v", bucket, res)
}
