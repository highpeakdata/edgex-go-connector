package object

import (
	"bufio"
	"fmt"
	"io"

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

func ObjectStreamFlow(suite suite.Suite, client s3xApi.S3xClient, bucket, object string) {

	err := client.ObjectCreate(bucket, object, s3xApi.OBJECT_TYPE_OBJECT, "application/json", s3xApi.DEFAULT_CHUNKSIZE, s3xApi.DEFAULT_BTREE_ORDER)
	suite.Nil(err)
	stream, err := client.ObjectGetStream(bucket, object)
	suite.Nil(err)
	suite.NotNil(stream)

	bufferString :=
		`Text is not just meant to write content, text itself can be a creative element if we use wisely. 
	Typography is not just limited to color, contrast, and size. 
	There are a few more aspects to achieve effective Typography. 
	Text effects are often used to create super cool typography. 
	Text effects are extremely popular in print designing like posters, flyer, ad boards etc. 
	When it comes to web designing use of text effects in typography is negligible even though they are useful in many situations. 
	A good designer must aware of beautiful typography and how to deploy content beautifully. 
	In order to learn typographic effects, you need to observe and inspect great works done by great designers. 
	Today we have collected a great collection of Photoshop Text Styles & Effects from great designers. 
	These free Photoshop text effects help you understand the technique of effective typography.`
	appendString := "_My_new_string_\n"
	fmt.Printf("Write test\n")
	n, err := stream.Write([]byte(bufferString))
	suite.Nil(err)
	suite.Equal(n, len(bufferString))
	fmt.Printf("Append test\n")
	n, err = stream.Write([]byte(appendString))
	suite.Nil(err)
	suite.Equal(n, len(appendString))
	fmt.Printf("Seek and read test\n")
	n1, err := stream.Seek(0, io.SeekStart)
	suite.Equal(n1, int64(0))
	suite.Nil(err)
	bio := bufio.NewReader(stream)
	suite.NotNil(bio)

	line, err := bio.ReadString('\n')
	suite.Nil(err)
	offset := len(line)
	line, err = bio.ReadString('\n')

	n1, err = stream.Seek(int64(offset), io.SeekStart)
	suite.Equal(n1, int64(offset))
	suite.Nil(err)
	bio.Reset(stream)
	line1, err := bio.ReadString('\n')
	suite.Equal(line, line1)
	err = stream.Close()
	suite.Nil(err)
	fmt.Printf("Deleting object %s/%s\n", bucket, object)
	//deleting existing object
	err = client.ObjectDelete(bucket, object)
	suite.Nil(err)
}
