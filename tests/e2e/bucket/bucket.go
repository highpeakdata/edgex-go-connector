package bucket

import (
	"fmt"

	s3xApi "github.com/Nexenta/edgex-go-connector/api/s3xclient/v1beta1"
	s3xErrors "github.com/Nexenta/edgex-go-connector/pkg/errors"
	"github.com/stretchr/testify/suite"
)

func BucketCreationFlow(suite suite.Suite, client s3xApi.S3xClient, bucket string) {
	err := client.BucketHead(bucket)
	// bucket not exist
	if suite.Error(err) {
		// should be ErrBucketNotExist error
		suite.Equal(s3xErrors.ErrBucketNotExist, err)

	} else {
		// deleting existing bucket
		err = client.BucketDelete(bucket)
		suite.Nil(err)
	}

	fmt.Printf("Creating bucket: %s\n", bucket)
	// create new bucket
	err = client.BucketCreate(bucket)
	suite.Nil(err)

	fmt.Printf("Checking bucket: %s\n", bucket)
	// check bucket existance
	err = client.BucketHead(bucket)
	suite.Nil(err)

	buckets, err := client.BucketList()
	suite.Nil(err)
	// Need to discover how to compare struct by specific field
	//suite.Contains(buckets, s3xApi.Bucket{Name: bucket})
	bucketExistInList := false
	fmt.Printf("Buckets:\n")
	for _, b := range buckets {
		fmt.Printf("\t%s\n", b.Name)
		if b.Name == bucket {
			bucketExistInList = true
			break
		}
	}

	suite.Equal(true, bucketExistInList, "Bucket should be presented on list")

	//fmt.Printf("Buckets: %+v\n", buckets)
}

func BucketDeletionFlow(suite suite.Suite, client s3xApi.S3xClient, bucket string) {
	// ccheck bucket existance
	err := client.BucketHead(bucket)
	suite.Nil(err)

	fmt.Printf("Deleting bucket: %s\n", bucket)
	// deleting existing bucket
	err = client.BucketDelete(bucket)
	suite.Nil(err)

	// check bucket existance
	err = client.BucketHead(bucket)
	// bucket not exist
	if suite.Error(err) {
		// should be ErrBucketNotExist error
		suite.Equal(s3xErrors.ErrBucketNotExist, err)

	}
}
