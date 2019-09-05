package edgex

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	s3xApi "github.com/Nexenta/edgex-go-connector/api/s3xclient/v1beta1"
	v1beta1 "github.com/Nexenta/edgex-go-connector/pkg/s3xclient/v1beta1"
	"github.com/Nexenta/edgex-go-connector/pkg/utils"
	mock "github.com/Nexenta/edgex-go-connector/tests/s3xMockClient"
)

const ()

var (
	s3x    s3xApi.S3xClient
	config *utils.S3xClientTestConfig
)

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano())

	// Assign to global config
	cfg, err := utils.GetTestConfig()
	if err != nil {
		log.Printf("Error reading configuration file. %v", err)
		os.Exit(1)
	}
	config = cfg

	if config.Bucket == "" {
		config.Bucket = fmt.Sprintf("bktest-%d", rand.Intn(1000))
	}
	if config.Object == "" {
		config.Object = fmt.Sprintf("obj-%d", rand.Intn(10000))
	}

	log.Printf("Test configuration: %+v\n", config)

	if config.Mockup == 1 {
		log.Println("Create Edgex mockup")
		s3x = mock.CreateMockup(1)

	} else {
		log.Printf("Create Edgex: %+v\n", config)
		s3x, err = v1beta1.CreateEdgex(config.Url, config.Authkey, config.Secret)
		log.Printf("s3x : %+v\n", s3x)
		if err != nil {
			log.Println("Error creating v1beta1.Edgex", err)
			os.Exit(1)
		}
	}

	// Run tests
	exitVal := m.Run()

	if config.Mockup == 0 {
		log.Println("Finalize Edgex connection:")
		clnt := s3x.(*v1beta1.Edgex)
		if clnt != nil {
			clnt.Close(config.Bucket, config.Object)
		}
	} else {
		log.Println("Close Edgex mockup:")
	}

	os.Exit(exitVal)

}

func TestBucketCreate(t *testing.T) {

	log.Printf("TestBucketCreate config %+v\n", config)
	err := s3x.BucketCreate(config.Bucket)

	if err != nil {
		t.Fatal("Bucket create error", err)
	}
}

func TestBucketHead(t *testing.T) {

	err := s3x.BucketHead(config.Bucket)

	if err != nil {
		t.Fatal("Head error", err)
	}
}

func TestKeyValueCreate(t *testing.T) {

	err := s3x.ObjectCreate(config.Bucket, config.Object, s3xApi.OBJECT_TYPE_KEY_VALUE, "application/json", s3xApi.DEFAULT_CHUNKSIZE, s3xApi.DEFAULT_BTREE_ORDER)

	if err != nil {
		t.Fatal("K/V object create error", err)
	}
}

func TestObjectHead(t *testing.T) {

	err := s3x.ObjectHead(config.Bucket, config.Object)

	if err != nil {
		t.Fatalf("Object Head error: %v", err)
	}
}

func TestNotObjectHead(t *testing.T) {

	err := s3x.ObjectHead(config.Bucket, "notobject")

	if err == nil {
		t.Fatalf("Not Object Head error: %v", err)
	}
}

func TestKeyValuePost(t *testing.T) {

	err := s3x.KeyValuePost(config.Bucket, config.Object, "key1", bytes.NewBufferString("value1"), "", false)
	if err != nil {
		t.Fatal("K/V object post1 error", err)
	}
	err = s3x.KeyValuePost(config.Bucket, config.Object, "key2", bytes.NewBufferString("value2"), "", false)
	if err != nil {
		t.Fatal("K/V object post1 error", err)
	}
}

func TestKeyValueCommit(t *testing.T) {

	key := "aaa1"
	err := s3x.KeyValuePost(config.Bucket, config.Object, key,
		bytes.NewBufferString("value1"), "", true)
	if err != nil {
		t.Fatal("K/V object post1 error", err)
	}
	err = s3x.KeyValueCommit(config.Bucket, config.Object)
	if err != nil {
		t.Fatal("K/V object commit error", err)
	}
	value, err := s3x.KeyValueGet(config.Bucket, config.Object, key)
	if err != nil {
		t.Fatal("K/V object get after commit error", err)
	}
	fmt.Printf("K/V get key: %s, value : %s\n", key, value)
}

func TestKeyValuePostJSON(t *testing.T) {

	json := utils.ArrToJSON("key3", "value3", "key4", "value4", "key5", "value5")

	err := s3x.KeyValuePostJSON(config.Bucket, config.Object, json, false)
	if err != nil {
		t.Fatal("K/V object json post error", err)
	}
}

func TestKeyValuePostCVS(t *testing.T) {

	cvs := utils.ArrToCVS("xx/kk9", "vv9", "xx/kk1", "vv1", "xz", "vv2", "xx/k3", "vv3")

	err := s3x.KeyValuePostCSV(config.Bucket, config.Object, cvs, false)
	if err != nil {
		t.Fatal("K/V object cvs post error", err)
	}

	key := "xx"
	prefix := "xx"
	values, err := s3x.KeyValueList(config.Bucket, config.Object, key, prefix, "text/csv", 100, true)
	if err != nil {
		t.Fatal("K/V object json list error", err)
	}
	fmt.Printf("K/V list from key: %s:\n%s\n", key, values)

}

func TestKeyValueGet(t *testing.T) {
	key := "key1"
	value, err := s3x.KeyValueGet(config.Bucket, config.Object, key)
	if err != nil {
		t.Fatal("K/V object get error", err)
	}
	fmt.Printf("K/V get key: %s, value : %s\n", key, value)
	key = "ke"
	value, err = s3x.KeyValueGet(config.Bucket, config.Object, key)
	if err == nil {
		t.Fatal("K/V object get should fail for", key)
	}
	key = "key1x"
	value, err = s3x.KeyValueGet(config.Bucket, config.Object, key)
	if err == nil {
		t.Fatal("K/V object get should fail for", key)
	}
}

func TestKeyValueDeleteJSON(t *testing.T) {

	json := utils.ArrToJSON("key1", "", "key2", "")

	err := s3x.KeyValueDeleteJSON(config.Bucket, config.Object, json, false)
	if err != nil {
		t.Fatal("K/V object json delete error", err)
	}

	_, err = s3x.KeyValueGet(config.Bucket, config.Object, "key1")
	if err == nil {
		t.Fatal("K/V object key1 not deleted")
	}

	_, err = s3x.KeyValueGet(config.Bucket, config.Object, "key2")
	if err == nil {
		t.Fatal("K/V object key2 not deleted")
	}
}

func TestKeyValueListCSV(t *testing.T) {
	key := ""
	value, err := s3x.KeyValueList(config.Bucket, config.Object, key, "", "text/csv", 100, true)
	if err != nil {
		t.Fatal("K/V object json list error", err)
	}
	fmt.Printf("K/V list from key: %s:\n%s\n", key, value)
}

func TestKeyValueListJSON(t *testing.T) {
	key := "key4"
	value, err := s3x.KeyValueList(config.Bucket, config.Object, key, "", "application/json", 100, true)
	if err != nil {
		t.Fatal("K/V object json list error", err)
	}
	fmt.Printf("K/V list from key: %s:\n %s\n", key, value)
}

func TestBucketList(t *testing.T) {
	res, err := s3x.BucketList()
	if err != nil {
		t.Fatal("Bucket list error", err)
	}
	fmt.Printf("Bucket list %v\n", res)
	found := false
	for i := range res {
		if res[i].Name == config.Bucket {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Bucket not found: ", config.Bucket)
	}
}

func TestObjectList(t *testing.T) {

	// Wait for trlog
	time.Sleep(45 * time.Second)
	res, err := s3x.ObjectList(config.Bucket, "", "", 100)
	if err != nil {
		t.Fatal("Object list error", err)
	}
	fmt.Printf("Object list %v\n", res)
	found := false
	for i := range res {
		if res[i].Key == config.Object {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Object not found: ", config.Object)
	}
}

func TestKeyValueDelete(t *testing.T) {
	key := "key5"
	err := s3x.KeyValueDelete(config.Bucket, config.Object, key, false)
	if err != nil {
		t.Fatal("K/V object delete error", err)
	}
	_, err = s3x.KeyValueGet(config.Bucket, config.Object, key)
	if err == nil {
		t.Fatal("K/V object not deleted")
	}
}

func TestKeyValueObjectDelete(t *testing.T) {

	err := s3x.ObjectDelete(config.Bucket, config.Object)

	if err != nil {
		t.Fatal("K/V object delete error", err)
	}
}

func TestBucketDelete(t *testing.T) {

	err := s3x.BucketDelete(config.Bucket)

	if err != nil {
		t.Fatal("Bucket delete error", err)
	}
}
