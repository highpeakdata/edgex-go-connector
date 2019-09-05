package v1beta1

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"

	s3xApi "github.com/Nexenta/edgex-go-connector/api/s3xclient/v1beta1"
	s3xErrors "github.com/Nexenta/edgex-go-connector/pkg/errors"

	"github.com/Nexenta/edgex-go-connector/pkg/utils"
)

// BucketCreate - create a new bucket
func (edgex *Edgex) BucketCreate(bucket string) error {

	bucketPath, err := utils.GetBucketPath(bucket)
	if err != nil {
		return err
	}

	s3xurl := edgex.newS3xURL(bucketPath)

	req, err := http.NewRequest("PUT", s3xurl.String(), nil)
	if err != nil {
		fmt.Printf("k/v create bucket error: %v\n", err)
		return err
	}

	res, err := edgex.httpClient.Do(req)
	if err != nil {
		fmt.Printf("k/v bucket create error: %v\n", err)
		return err
	}
	if res.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("%s bucket create status code: %v", bucketPath, res.StatusCode)
}

// BucketHead - read bucket header fields
func (edgex *Edgex) BucketHead(bucket string) error {

	bucketPath, err := utils.GetBucketPath(bucket)
	if err != nil {
		return err
	}
	s3xurl := edgex.newS3xURL(bucketPath)
	s3xurl.AddOptions(S3XURLOptions{
		"comp":     "streamsession",
		"finalize": "",
	})

	res, err := http.Head(s3xurl.String())
	if err != nil {
		fmt.Printf("Bucket Head error: %v\n", err)
		return err
	}

	if res.StatusCode < 300 {
		return nil
	}
	if res.StatusCode == 404 {
		return s3xErrors.ErrBucketNotExist
	}
	return fmt.Errorf("Bucket %s head error: %v", bucketPath, res)
}

// BucketDelete - delete bucket
func (edgex *Edgex) BucketDelete(bucket string) error {

	bucketPath, err := utils.GetBucketPath(bucket)
	if err != nil {
		return err
	}
	s3xurl := edgex.newS3xURL(bucketPath)

	req, err := http.NewRequest("DELETE", s3xurl.String(), nil)
	if err != nil {
		fmt.Printf("k/v create bucket delete error: %v\n", err)
		return err
	}

	res, err := edgex.httpClient.Do(req)
	if err != nil {
		fmt.Printf("k/v bucket delete error: %v\n", err)
		return err
	}
	if res.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("%s bucket delete status code: %v", bucketPath, res.StatusCode)
}

func (edgex *Edgex) BucketList() ([]s3xApi.Bucket, error) {

	s3xurl := edgex.newS3xURL("")

	req, err := http.NewRequest("GET", s3xurl.String(), nil)
	if err != nil {
		fmt.Printf("k/v create key/value list error: %v\n", err)
		return nil, err
	}

	req.Header.Add("Content-Length", "0")
	res, err := edgex.httpClient.Do(req)
	if err != nil {
		fmt.Printf("Bucket list error: %v\n", err)
		return nil, err
	}
	defer res.Body.Close()

	var list s3xApi.ListAllMyBucketsResult
	if res.StatusCode < 300 {
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			fmt.Printf("Object list read error: %v\n", err)
			return list.Buckets.Buckets, err
		}
		err = xml.Unmarshal(body, &list)
		return list.Buckets.Buckets, err
	}
	return list.Buckets.Buckets, fmt.Errorf("Bucket list error: %v", res)
}
