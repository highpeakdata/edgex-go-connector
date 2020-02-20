package object

import (
	"fmt"

	s3xApi "github.com/highpeakdata/edgex-go-connector/api/s3xclient/v1beta1"
	s3xErrors "github.com/highpeakdata/edgex-go-connector/pkg/errors"
	"github.com/stretchr/testify/suite"
)

func ObjectCreationFlow(suite suite.Suite, client s3xApi.S3xClient, bucket, object string) {
	err := client.ObjectHead(bucket, object)
	// object not exist
	if suite.Error(err) {
		// should be ErrObjectNotExist error
		suite.Equal(s3xErrors.ErrObjectNotExist, err)

	} else {
		// deleting existing object
		err = client.ObjectDelete(bucket, object)
		suite.Nil(err)
	}

	fmt.Printf("Creating object %s/%s\n", bucket, object)
	// create new object
	err = client.ObjectCreate(bucket, object, s3xApi.OBJECT_TYPE_KEY_VALUE, "application/json", s3xApi.DEFAULT_CHUNKSIZE, s3xApi.DEFAULT_BTREE_ORDER)
	suite.Nil(err)

	// check object existance
	err = client.ObjectHead(bucket, object)
	suite.Nil(err)

	// check object existance
	err = client.ObjectHead(bucket, object)
	// object not exist
	objectExists := true
	if err != nil {
		objectExists = false
	}

	suite.Equal(true, objectExists, "Object should be presented")
}

func ObjectDeletionFlow(suite suite.Suite, client s3xApi.S3xClient, bucket, object string) {
	// check object existance
	err := client.ObjectHead(bucket, object)
	suite.Nil(err)

	fmt.Printf("Deleting object %s/%s\n", bucket, object)
	// deleting existing object
	err = client.ObjectDelete(bucket, object)
	suite.Nil(err)

	// check object existance
	err = client.ObjectHead(bucket, object)
	// object not exist
	if suite.Error(err) {
		// should be ErrObjectNotExist error
		suite.Equal(s3xErrors.ErrObjectNotExist, err)
	}
}
