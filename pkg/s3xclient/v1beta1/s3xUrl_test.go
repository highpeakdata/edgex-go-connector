package v1beta1

import (
	"fmt"
	"log"
	"os"
	"testing"
)

func Test_GetValidURl(t *testing.T) {
	s3xurlValid := "http://:5000"
	s3xurlValid2 := "http://localhost"
	s3xurlValid3 := "http://192.168.0.1:5000/test?x=y&m=n"
	sub1Annotation := fmt.Sprintf("Validate S3X url: %s ", s3xurlValid)
	sub2Annotation := fmt.Sprintf("Validate S3X url: %s ", s3xurlValid2)
	sub3Annotation := fmt.Sprintf("Validate S3X url: %s ", s3xurlValid3)
	t.Run(sub1Annotation, func(t *testing.T) {
		url, err := getValidUrl(s3xurlValid)
		if err != nil {
			log.Printf("Error parsing s3x url: %s, error: %v", s3xurlValid, err)
			os.Exit(1)
		}
		if url.String() != "http://localhost:5000" {
			log.Printf("Error parsing s3x url: %s, error: %v", s3xurlValid, err)
			os.Exit(1)
		}
		log.Printf(url.String())
	})

	t.Run(sub2Annotation, func(t *testing.T) {
		url, err := getValidUrl(s3xurlValid2)
		if err != nil {
			log.Printf("Error parsing s3x url: %s, error: %v", s3xurlValid2, err)
			os.Exit(1)
		}
		if url.String() != "http://localhost:3000" {
			log.Printf("Error parsing s3x url: %s, error: %v", s3xurlValid2, err)
			os.Exit(1)
		}
		log.Printf(url.String())
	})

	t.Run(sub3Annotation, func(t *testing.T) {
		url, err := getValidUrl(s3xurlValid3)
		if err != nil {
			log.Printf("Error parsing s3x url: %s, error: %v", s3xurlValid3, err)
			os.Exit(1)
		}
		if url.String() != "http://192.168.0.1:5000" {
			log.Printf("Error parsing s3x url: %s, error: %v", s3xurlValid3, err)
			os.Exit(1)
		}
		log.Printf(url.String())
	})

}

func Test_CheckS3XURL(t *testing.T) {
	rawurl := "http://192.168.0.1:5000/test?x=y&m=n"
	baseurl, err := getValidUrl(rawurl)
	if err != nil {
		log.Printf("Failed to get valid url from %s, error: %v", rawurl, err)
		os.Exit(1)
	}
	log.Printf("base URL %s", baseurl.String())
	t.Run("Checking S3XURL", func(t *testing.T) {
		s3xurl := NewS3XURL(baseurl, "edgex.Bucket/edgex.Object")
		s3xurl.AddOptions(S3XURLOptions{
			"comp":     "streamSession",
			"finalize": "",
		})

		s3xurl.AddOptions(S3XURLOptions{
			"newOption": "newValue",
		})

		log.Printf("S3XURL: %s", s3xurl.String())
		log.Printf("Base URL: %s", baseurl.String())
		expectedValue := "http://192.168.0.1:5000/edgex.Bucket/edgex.Object?comp=streamSession&finalize=&newOption=newValue"
		if s3xurl.String() != expectedValue {
			log.Printf("Failed to get S3XURL expected value: %s", expectedValue)
			os.Exit(1)
		}
	})
}
