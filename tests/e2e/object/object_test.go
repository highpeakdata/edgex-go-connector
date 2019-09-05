package object

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	s3xApi "github.com/Nexenta/edgex-go-connector/api/s3xclient/v1beta1"
	v1beta1 "github.com/Nexenta/edgex-go-connector/pkg/s3xclient/v1beta1"
	"github.com/Nexenta/edgex-go-connector/pkg/utils"
	"github.com/Nexenta/edgex-go-connector/tests/e2e/bucket"
	"github.com/stretchr/testify/suite"
)

// Edgex end to end client test suite
type e2eObjectTestSuite struct {
	suite.Suite
	s3x s3xApi.S3xClient

	// global bucket/object names for this test
	Bucket string
	Object string
}

func (suite *e2eObjectTestSuite) SetupSuite() {
	// Initialize things or do any setup stuff inside here

	rand.Seed(time.Now().UnixNano())
	testConfig, err := utils.GetTestConfig()
	if err != nil {
		log.Printf("Failed to find test configuration file path: %v", err)
		os.Exit(1)
	}

	suite.Bucket = testConfig.Bucket
	if suite.Bucket == "" {
		suite.Bucket = fmt.Sprintf("bktest-%d", rand.Intn(1000))
	}

	suite.Object = testConfig.Object
	if suite.Object == "" {
		suite.Object = fmt.Sprintf("obj-%d", rand.Intn(10000))
	}

	client, err := v1beta1.CreateEdgex(testConfig.Url, testConfig.Authkey, testConfig.Secret)
	if err != nil {
		log.Printf("Failed to create Edgex client: %v", err)
		os.Exit(1)
	}

	suite.s3x = client

}

func TestEnd2EndObjectTestSuite(t *testing.T) {
	// This is what actually runs our suite
	suite.Run(t, new(e2eObjectTestSuite))
}

//SetupTest Prepare env for Object testing i.e. creates bucket
func (suite *e2eObjectTestSuite) SetupTest() {
	bucket.BucketCreationFlow(suite.Suite, suite.s3x, suite.Bucket)
}

//SetupTest Prepare env for Object testing i.e. deletes bucket
func (suite *e2eObjectTestSuite) TearDownTest() {
	bucket.BucketDeletionFlow(suite.Suite, suite.s3x, suite.Bucket)
}

//TestBucketFlow - object create/head/delete test
func (suite *e2eObjectTestSuite) TestObjectFlow() {
	ObjectCreationFlow(suite.Suite, suite.s3x, suite.Bucket, suite.Object)
	ObjectDeletionFlow(suite.Suite, suite.s3x, suite.Bucket, suite.Object)
}
