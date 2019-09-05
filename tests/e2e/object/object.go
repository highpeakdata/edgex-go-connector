package object

import (
	"fmt"
	"time"

	s3xApi "github.com/Nexenta/edgex-go-connector/api/s3xclient/v1beta1"
	s3xErrors "github.com/Nexenta/edgex-go-connector/pkg/errors"
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

	objects, err := client.ObjectList(bucket, "", "", 100)
	suite.Nil(err)
	// Need to discover how to compare struct by specific field
	//suite.Contains(objects, s3xApi.Object{Name: object})
	time.Sleep(30 * time.Second)
	objectExistInList := false
	fmt.Printf("%s Objects:\n", bucket)
	for _, obj := range objects {
		fmt.Printf("\t%s\n", obj.Key)
		if obj.Key == object {
			objectExistInList = true
			break
		}
	}

	suite.Equal(true, objectExistInList, "Object should be presented on list")

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
