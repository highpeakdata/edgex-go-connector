package v1beta1

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"

	s3xApi "github.com/highpeakdata/edgex-go-connector/api/s3xclient/v1beta1"
	s3xErrors "github.com/highpeakdata/edgex-go-connector/pkg/errors"

	"github.com/highpeakdata/edgex-go-connector/pkg/utils"
)

type s3xObjectStream struct {
	edgex     *Edgex
	path      string
	sessionID string
	offset    int
	size      int
	dirty     bool
}

func (s *s3xObjectStream) Read(p []byte) (n int, err error) {
	contentLen := len(p)
	if s.offset+contentLen > s.size {
		contentLen = s.size - s.offset
	}
	if contentLen == 0 {
		return 0, nil
	}
	s3xurl := s.edgex.newS3xURL(s.path)
	s3xurl.AddOptions(S3XURLOptions{
		"comp": "streamsession",
	})
	req, err := http.NewRequest("GET", s3xurl.String(), nil)
	if err != nil {
		return 0, fmt.Errorf("StreamRead create GET error: %v", err)
	}

	req.Header.Add("x-session-id", s.sessionID)
	req.Header.Add("x-ccow-offset", strconv.Itoa(s.offset))
	req.Header.Add("x-ccow-length", strconv.Itoa(contentLen))

	res, err := s.edgex.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("StreamRead GET error: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return 0, fmt.Errorf("StreamRead GET Status code %v", res.StatusCode)
	}
	n, err = res.Body.Read(p)
	if err == nil {
		s.offset += n
		newID := res.Header.Get("x-session-id")
		if newID != s.sessionID {
			s.sessionID = newID
		}
	}
	return n, err
}

func (s *s3xObjectStream) Write(p []byte) (n int, err error) {
	size := len(p)
	if size == 0 {
		return 0, nil
	}
	s3xurl := s.edgex.newS3xURL(s.path)
	s3xurl.AddOptions(S3XURLOptions{
		"comp":     "streamsession",
		"finalize": "",
	})
	req, err := http.NewRequest("POST", s3xurl.String(), bytes.NewBuffer(p))
	if err != nil {
		return 0, fmt.Errorf("StreamWrite create POST error: %v", err)
	}

	req.Header.Add("x-session-id", s.sessionID)
	req.Header.Add("x-ccow-offset", strconv.Itoa(int(s.offset)))
	req.Header.Add("x-ccow-length", strconv.Itoa(int(size)))

	res, err := s.edgex.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("StreamWrite POST error: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return 0, fmt.Errorf("StreamWrite POST Status code %v", res.StatusCode)
	}

	if s.offset+size > s.size {
		s.size = s.offset + size
	}
	s.offset += size
	s.dirty = true
	newID := res.Header.Get("x-session-id")
	if newID != s.sessionID {
		s.sessionID = newID
	}
	return size, nil
}

func (s *s3xObjectStream) Seek(offset int64, whence int) (int64, error) {
	newPos := 0
	if whence == io.SeekCurrent {
		newPos = s.offset + int(offset)
	} else if whence == io.SeekEnd {
		newPos = s.size + int(offset)
	} else if whence == io.SeekStart {
		newPos = int(offset)
	}
	if newPos > s.size || newPos < 0 {
		return 0, fmt.Errorf("Invalid offset %v", offset)
	}
	s.offset = newPos
	return int64(newPos), nil
}

func (s *s3xObjectStream) Close() error {
	s3xurl := s.edgex.newS3xURL(s.path)
	s3xurl.AddOptions(S3XURLOptions{
		"comp": "streamsession",
	})
	if s.dirty {
		s3xurl.AddOptions(S3XURLOptions{
			"finalize": "",
		})
	} else {
		s3xurl.AddOptions(S3XURLOptions{
			"cancel": "",
		})
	}
	_, err := http.NewRequest("HEAD", s3xurl.String(), nil)
	if err != nil {
		return fmt.Errorf("StreamClose create HEAD error: %v", err)
	}
	return nil
}

func (edgex *Edgex) ObjectGetStream(bucket, object string) (s3xApi.ObjectStream, error) {
	objectPath, err := utils.GetObjectPath(bucket, object)
	if err != nil {
		return nil, err
	}

	s3xurl := edgex.newS3xURL(objectPath)
	s3xurl.AddOptions(S3XURLOptions{
		"comp": "streamsession",
	})

	res, err := http.Head(s3xurl.String())
	if err != nil {
		fmt.Printf("Object Head error: %v\n", err)
		return nil, err
	}

	if res.StatusCode == 404 {
		return nil, s3xErrors.ErrObjectNotExist
	}
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("Object %s/ head error: %v", objectPath, err)
	}
	sizeStr := res.Header.Get("x-ccow-logical-size")
	size := 0
	if len(sizeStr) > 0 {
		size, _ = strconv.Atoi(sizeStr)
	}
	return &s3xObjectStream{
		edgex:     edgex,
		sessionID: res.Header.Get("x-session-id"),
		path:      objectPath,
		offset:    size,
	}, nil
}

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

	if edgex.Debug > 0 {
		fmt.Printf("ObjectList request: %+v", req)
	}
	req.Header.Add("Content-Length", "0")
	res, err := client.Do(req)
	if err != nil {
		fmt.Printf("Object list error: %v\n", err)
		return list.Objects, err
	}
	defer res.Body.Close()

	if edgex.Debug > 0 {
		fmt.Printf("ObjectList response: %+v", res)
	}
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
