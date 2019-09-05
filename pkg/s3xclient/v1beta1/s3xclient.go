package v1beta1

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	s3xApi "github.com/Nexenta/edgex-go-connector/api/s3xclient/v1beta1"
)

// Edgex - Edgex is S3xClient implementation
type Edgex struct {
	//Parsed url contains scheme, host(:port) only i.e scheme://host:[port]
	baseUrl    *url.URL
	httpClient *http.Client

	// s3 authentication keys
	Authkey string
	Secret  string

	// Should move to Tx struct
	Sid string
}

//getValidUrl: returns S3X endpoint w/o path and parameters
func getValidUrl(s3xurl string) (*url.URL, error) {
	u, err := url.Parse(s3xurl)
	if err != nil {
		fmt.Printf("Error: %+v", err)
		return nil, err
	}

	host, port, _ := net.SplitHostPort(u.Host)
	if len(host) == 0 {
		host = "localhost"
	}

	if len(port) == 0 {
		port = fmt.Sprintf("%d", s3xApi.DEFAULT_EDGEX_PORT)
	}

	// clean up path e.t.c values
	u.Path = ""
	u.RawQuery = ""

	u.Host = fmt.Sprintf("%s:%s", host, port)
	return u, nil
}

type EdgexOption func(*Edgex)

func SetHTTPClient(httpClient *http.Client) EdgexOption {
	return func(edgex *Edgex) {
		edgex.httpClient = httpClient
	}
}

// CreateEdgex - S3X client factory
func CreateEdgex(s3xurl, authkey, secret string, options ...EdgexOption) (s3xApi.S3xClient, error) {
	url, err := getValidUrl(s3xurl)
	if err != nil {
		return nil, err
	}

	edgex := Edgex{
		baseUrl:    url,
		Authkey:    authkey,
		Secret:     secret,
		httpClient: &http.Client{Timeout: 45 * time.Second},
	}

	// apply all options handlers to edgex instance
	for i := range options {
		options[i](&edgex)
	}

	return &edgex, nil
}

type S3XURLOptions map[string]string
type S3XURL struct {
	url.URL
}

//NewS3XURL Copies endpoint values to internal url for further use
func NewS3XURL(baseUrl *url.URL, path string) S3XURL {
	rel := &url.URL{Path: path}
	return S3XURL{URL: *baseUrl.ResolveReference(rel)}
}

func (s3xurl *S3XURL) AddOptions(values S3XURLOptions) {
	q := s3xurl.Query()
	for k, v := range values {
		q.Add(k, v)
	}
	s3xurl.RawQuery = q.Encode()
}

func (s3xurl *S3XURL) String() string {
	return s3xurl.URL.String()
}

// Returns base url for additional operations
func (edgex *Edgex) newS3xURL(path string) S3XURL {
	return NewS3XURL(edgex.baseUrl, path)
}
