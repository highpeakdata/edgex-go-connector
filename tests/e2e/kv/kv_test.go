package kv

import (
	"bytes"
	"encoding/json"
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
	"github.com/Nexenta/edgex-go-connector/tests/e2e/object"
	"github.com/stretchr/testify/suite"
)

var (
	flagEndpoint string
	flagBucket   string
	flagObject   string
)

type TestMapStruct struct {
	Name       string   `json:"name"`
	IntValue   int      `json:"value"`
	ArrayValue []string `json:"arr"`
}

// Edgex end to end key/value test suite
type e2eKVTestSuite struct {
	suite.Suite
	s3x s3xApi.S3xClient

	// global bucket/object names for this test
	Bucket string
	Object string
}

func (suite *e2eKVTestSuite) SetupSuite() {
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

func TestEnd2EndKVTestSuite(t *testing.T) {
	// This is what actually runs our suite
	suite.Run(t, new(e2eKVTestSuite))
}

//SetupTest Prepare env for key/value testing i.e. creates bucket/object
func (suite *e2eKVTestSuite) SetupTest() {
	bucket.BucketCreationFlow(suite.Suite, suite.s3x, suite.Bucket)
	object.ObjectCreationFlow(suite.Suite, suite.s3x, suite.Bucket, suite.Object)
}

//SetupTest Remove all created object/buckets
func (suite *e2eKVTestSuite) TearDownTest() {
	object.ObjectDeletionFlow(suite.Suite, suite.s3x, suite.Bucket, suite.Object)
	bucket.BucketDeletionFlow(suite.Suite, suite.s3x, suite.Bucket)
}

//TestBucketFlow - object create/head/delete test
func (suite *e2eKVTestSuite) TestObjectFlow() {
	//suite.PostSingleKVTest()
	//suite.PostJSONTest()
	suite.PostMapTest()
}

func (suite *e2eKVTestSuite) PostSingleKVTest() {

	singleKeyName := "singleKey"
	singleKeyValue := "singleKeyValue"

	// Check key NOT exists
	result, err := suite.s3x.KeyValueGet(suite.Bucket, suite.Object, singleKeyName)
	suite.NotNil(err)

	// Create new key with value, `more` set to false to apply changes immideatly
	fmt.Printf("Creating %s/%s[%s]=`%s`\n", suite.Bucket, suite.Object, singleKeyName, singleKeyValue)
	err = suite.s3x.KeyValuePost(suite.Bucket, suite.Object, singleKeyName, bytes.NewBufferString(singleKeyValue), "application/json", false)
	suite.Nil(err)

	// gets key value
	result, err = suite.s3x.KeyValueGet(suite.Bucket, suite.Object, singleKeyName)
	suite.Nil(err)

	// and check key value
	fmt.Printf("Value %s/%s[%s]=`%s`\n", suite.Bucket, suite.Object, singleKeyName, result)
	suite.Equal(singleKeyValue, result)

	// deletes key
	err = suite.s3x.KeyValueDelete(suite.Bucket, suite.Object, singleKeyName, false)
	suite.Nil(err)

	// key should NOT exists
	result, err = suite.s3x.KeyValueGet(suite.Bucket, suite.Object, singleKeyName)
	suite.NotNil(err)

}

/*
func (suite *e2eKVTestSuite) PostJSONTest() {
	json := utils.ArrToJSON("key3", "value3", "key4", "value4", "key5", "value5")

	err := suite.s3x.KeyValuePostJSON(suite.Bucket, suite.Object, json, false)
	suite.Nil(err)

	result, err := suite.s3x.KeyValueGet(suite.Bucket, suite.Object, "key3")
	suite.Nil(err)

	fmt.Printf("Value %s/%s[%s]=`%s`\n", suite.Bucket, suite.Object, "key3", result)
	suite.Equal("value3", result)
}
*/

func (suite *e2eKVTestSuite) PostMapTest() {
	mapKey1Name := "mapKey1"
	mapKey1NameValue := "mapKey1Value"
	mapKey2Name := "mapKey2"

	structValue := TestMapStruct{
		Name:       "namedValue",
		IntValue:   123,
		ArrayValue: []string{"arrValue1", "arrValue2"},
	}
	testMap := map[string]interface{}{
		mapKey1Name: mapKey1NameValue,
		mapKey2Name: structValue,
	}

	// Check key1 NOT exists
	result, err := suite.s3x.KeyValueGet(suite.Bucket, suite.Object, mapKey1Name)
	suite.NotNil(err)

	// Check key2 NOT exists
	result, err = suite.s3x.KeyValueGet(suite.Bucket, suite.Object, mapKey2Name)
	suite.NotNil(err)

	// post map
	err = suite.s3x.KeyValueMapPost(suite.Bucket, suite.Object, testMap, false)
	suite.Nil(err)

	// Check values
	result, err = suite.s3x.KeyValueGet(suite.Bucket, suite.Object, mapKey1Name)
	suite.Nil(err)

	key2ValueResult, err := suite.s3x.KeyValueGet(suite.Bucket, suite.Object, mapKey2Name)
	suite.Nil(err)

	fmt.Printf("Value %s/%s[%s]=`%s`\n", suite.Bucket, suite.Object, "key3", result)
	jsonBytes, err := json.Marshal(structValue)
	suite.Nil(err)

	suite.JSONEq(key2ValueResult, string(jsonBytes))

	// delete post map
	err = suite.s3x.KeyValueMapDelete(suite.Bucket, suite.Object, testMap, false)
	suite.Nil(err)
}
