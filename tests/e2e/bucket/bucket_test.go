package bucket

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/Nexenta/edgex-go-connector/pkg/utils"

	s3xApi "github.com/Nexenta/edgex-go-connector/api/s3xclient/v1beta1"
	v1beta1 "github.com/Nexenta/edgex-go-connector/pkg/s3xclient/v1beta1"
	"github.com/stretchr/testify/suite"
)

// Edgex end to end client bucket's tests suite
type e2eBucketTestSuite struct {
	suite.Suite
	s3x s3xApi.S3xClient

	// global bucket name for this test
	Bucket string
}

func (suite *e2eBucketTestSuite) SetupSuite() {
	// Initialize things or do any setup stuff inside here

	rand.Seed(time.Now().UnixNano())

	testConfig, err := utils.GetTestConfig()
	if err != nil {
		log.Printf("Failed to find test configuration file path: %v", err)
		os.Exit(1)
	}

	fmt.Printf("TestConfig is %+v\n", testConfig)
	suite.Bucket = testConfig.Bucket
	if suite.Bucket == "" {
		suite.Bucket = fmt.Sprintf("bktest-%d", rand.Intn(1000))
	}

	client, err := v1beta1.CreateEdgex(testConfig.Url, testConfig.Authkey, testConfig.Secret)
	if err != nil {
		log.Printf("Failed to create Edgex client: %v", err)
		os.Exit(1)
	}

	suite.s3x = client
}

func TestEnd2EndBucketTestSuite(t *testing.T) {
	// This is what actually runs our suite
	suite.Run(t, new(e2eBucketTestSuite))
}

//TestBucketFlow - bucket create/head/delete test
func (suite *e2eBucketTestSuite) TestBucketFlow() {
	BucketCreationFlow(suite.Suite, suite.s3x, suite.Bucket)
	BucketDeletionFlow(suite.Suite, suite.s3x, suite.Bucket)
}
