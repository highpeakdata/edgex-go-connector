package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const (
	testConfigurationFileName = "test_setup.json"
)

// EdgexTest - general Edgex client test structure
type S3xClientTestConfig struct {
	Mockup  int    `json:"mockup"`
	Url     string `json:"url"`
	Authkey string `json:"authkey"`
	Secret  string `json:"secret"`
	Bucket  string `json:"bucket"`
	Object  string `json:"object"`
	//Debug   int    `json:"debug"`
}

func GetTestConfig() (*S3xClientTestConfig, error) {

	currentFolder, err := os.Getwd()
	if err != nil {
		log.Printf("Failed to get `pwd`: %v", err)
		return nil, err
	}

	rootProjectPath, err := GetAbsRootProjectPath(currentFolder)
	if err != nil {
		log.Printf("Failed to find root project path: %v", err)
		return nil, err
	}

	buf, err := ioutil.ReadFile(filepath.Join(rootProjectPath, testConfigurationFileName))
	if err != nil {
		return nil, err
	}

	config := &S3xClientTestConfig{}
	err = json.Unmarshal(buf, config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

// GetAbsRootProjectPath takes deeper project folder path, and retrive root's project path
// I.e test path ~/go/src/github.com/Nexenta/edgex-go-connector/tests/e2e/bucket
// Method return ~/go/src/github.com/Nexenta/edgex-go-connector
// Must be used in testing suites ONLY!
func GetAbsRootProjectPath(deeperAbsPath string) (string, error) {

	parts := strings.Split(deeperAbsPath, string(os.PathSeparator))
	for i := len(parts); i > 0; i-- {
		pathSegment := parts[i-1]
		if pathSegment == "edgex-go-connector" {
			return strings.Join(parts[:i], string(os.PathSeparator)), nil
		}
	}
	return "", fmt.Errorf("No root project found")
}

func GetBucketPath(bucket string) (string, error) {
	bucket = strings.TrimSpace(bucket)
	if len(bucket) == 0 {
		return "", fmt.Errorf("Invalid bucket name `%s`", bucket)
	}
	return bucket, nil
}

func GetObjectPath(bucket, object string) (string, error) {

	bucket = strings.TrimSpace(bucket)
	if len(bucket) == 0 {
		return "", fmt.Errorf("Invalid bucket name `%s`", bucket)
	}

	object = strings.TrimSpace(object)
	if len(object) == 0 {
		return "", fmt.Errorf("Invalid object name: `%s`", object)
	}
	return fmt.Sprintf("%s/%s", bucket, object), nil
}

// ArrToJSON - convert k/v pairs to json
func ArrToJSON(arr ...string) string {
	var b bytes.Buffer

	b.WriteString("{")
	n := 0
	for i := 0; i < len(arr); i += 2 {
		if n > 0 {
			b.WriteString(", ")
		}
		b.WriteString(" \"")
		b.WriteString(arr[i])
		b.WriteString("\": \"")
		b.WriteString(arr[i+1])
		b.WriteString("\"")
		n++
	}
	b.WriteString("}")

	return b.String()
}

// ArrToCVS - convert k/v pairs to cvs
func ArrToCVS(arr ...string) string {
	var b bytes.Buffer

	n := 0
	for i := 0; i < len(arr); i += 2 {
		if n > 0 {
			b.WriteString("\n")
		}
		b.WriteString(arr[i])
		b.WriteString(";")
		b.WriteString(arr[i+1])
		n++
	}

	return b.String()
}
