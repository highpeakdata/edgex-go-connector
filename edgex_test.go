package edgex

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

// EdgexTest - general Edgex client test structure
type EdgexTest struct {
	Mockup  int    `json:"mockup"`
	Url     string `json:"url"`
	Authkey string `json:"authkey"`
	Secret  string `json:"secret"`
	Bucket  string `json:"bucket"`
	Object  string `json:"object"`
	Debug   int    `json:"debug"`
}

var ed EdgexTest
var s3x S3xClient
var ex *Edgex
var em *Mockup

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano())

	buf, err := ioutil.ReadFile("test_setup.json")
	if err != nil {
		log.Println("Error reading test_setup.json file", err)
		os.Exit(1)
	}

	log.Println("Read setup", string(buf))

	err = json.Unmarshal(buf, &ed)
	if err != nil {
		log.Println("Error reading setup file", err)
		os.Exit(1)
	}

	if ed.Bucket == "" {
		ed.Bucket = fmt.Sprintf("bktest%d", rand.Intn(1000))
	}
	if ed.Object == "" {
		ed.Object = fmt.Sprintf("obj%d", rand.Intn(10000))
	}

	if ed.Mockup == 0 {
		log.Println("Create Edgex:", ed)
		ex = CreateEdgex(ed.Url, ed.Authkey, ed.Secret, ed.Debug)
		s3x = ex
	} else {
		log.Println("Create Edgex mockup")
		em = CreateMockup(ed.Debug)
		s3x = em
	}

	// Run tests
	exitVal := m.Run()

	if ed.Mockup == 0 {
		log.Println("Close Edgex connection:")
		CloseEdgex(ex)
	} else {
		log.Println("Close Edgex mockup:")
	}
	os.Exit(exitVal)
}

func TestBucketCreate(t *testing.T) {

	res := s3x.BucketCreate(ed.Bucket)

	if res != nil {
		t.Fatal("Bucket create error", res)
	}
}

func TestBucketHead(t *testing.T) {

	res := s3x.BucketHead(ed.Bucket)

	if res != nil {
		t.Fatal("Head error", res)
	}
}

func TestKeyValueCreate(t *testing.T) {

	res := s3x.ObjectCreate(ed.Bucket, ed.Object, OBJECT_TYPE_KEY_VALUE,
		"application/json", DEFAULT_CHUNKSIZE, DEFAULT_BTREE_ORDER)

	if res != nil {
		t.Fatal("K/V object create error", res)
	}
}

func TestObjectHead(t *testing.T) {

	res := s3x.ObjectHead(ed.Bucket, ed.Object)

	if res != nil {
		t.Fatal("Object Head error", res)
	}
}

func TestNotObjectHead(t *testing.T) {

	res := s3x.ObjectHead(ed.Bucket, "notobject")

	if res == nil {
		t.Fatal("Not Object Head error")
	}
}

func TestKeyValuePost(t *testing.T) {

	res := s3x.KeyValuePost(ed.Bucket, ed.Object, "", "key1",
		bytes.NewBufferString("value1"), true)
	if res != nil {
		t.Fatal("K/V object post1 error", res)
	}
	res = s3x.KeyValuePost(ed.Bucket, ed.Object, "", "key2",
		bytes.NewBufferString("value2"), true)
	if res != nil {
		t.Fatal("K/V object post1 error", res)
	}
}

func TestKeyValueCommit(t *testing.T) {
	key := "aaa1"
	res := s3x.KeyValuePost(ed.Bucket, ed.Object, "", key,
		bytes.NewBufferString("value1"), true)
	if res != nil {
		t.Fatal("K/V object post1 error", res)
	}
	res = s3x.KeyValueCommit(ed.Bucket, ed.Object)
	if res != nil {
		t.Fatal("K/V object commit error", res)
	}
	res = s3x.KeyValueGet(ed.Bucket, ed.Object, key)
	if res != nil {
		t.Fatal("K/V object get after commit error", res)
	}
	fmt.Printf("K/V get key: %s, value : %s\n", key, s3x.GetLastValue())
}

func TestKeyValueRollback(t *testing.T) {
	key := "aaa2"
	res := s3x.KeyValuePost(ed.Bucket, ed.Object, "", key,
		bytes.NewBufferString("value2"), true)
	if res != nil {
		t.Fatal("K/V object post1 error", res)
	}
	res = s3x.KeyValueRollback(ed.Bucket, ed.Object)
	if res != nil {
		t.Fatal("K/V object rollback error", res)
	}
	res = s3x.KeyValueGet(ed.Bucket, ed.Object, key)
	if res == nil {
		t.Fatal("K/V object still exists after rollback")
	}
}

func TestKeyValuePostJSON(t *testing.T) {

	json := ArrToJSON("key3", "value3", "key4", "value4", "key5", "value5")

	res := s3x.KeyValuePostJSON(ed.Bucket, ed.Object, json, false)
	if res != nil {
		t.Fatal("K/V object json post error", res)
	}
}

func TestKeyValuePostCVS(t *testing.T) {

	cvs := ArrToCVS("xx/kk9", "vv9", "xx/kk1", "vv1", "xz", "vv2", "xx/k3", "vv3")

	res := s3x.KeyValuePostCSV(ed.Bucket, ed.Object, cvs, false)
	if res != nil {
		t.Fatal("K/V object cvs post error", res)
	}

	key := "xx"
	prefix := "xx"
	res = s3x.KeyValueList(ed.Bucket, ed.Object, key, prefix, "text/csv", 100, true)
	if res != nil {
		t.Fatal("K/V object json list error", res)
	}
	fmt.Printf("K/V list from key: %s:\n%s\n", key, s3x.GetLastValue())

}

func TestKeyValueGet(t *testing.T) {
	key := "key1"
	res := s3x.KeyValueGet(ed.Bucket, ed.Object, key)
	if res != nil {
		t.Fatal("K/V object get error", res)
	}
	fmt.Printf("K/V get key: %s, value : %s\n", key, s3x.GetLastValue())
	key = "ke"
	res = s3x.KeyValueGet(ed.Bucket, ed.Object, key)
	if res == nil {
		t.Fatal("K/V object get should fail for", key)
	}
	key = "key1x"
	res = s3x.KeyValueGet(ed.Bucket, ed.Object, key)
	if res == nil {
		t.Fatal("K/V object get should fail for", key)
	}
}

func TestKeyValueDeleteJSON(t *testing.T) {

	json := ArrToJSON("key1", "", "key2", "")

	res := s3x.KeyValueDeleteJSON(ed.Bucket, ed.Object, json, false)
	if res != nil {
		t.Fatal("K/V object json delete error", res)
	}

	res = s3x.KeyValueGet(ed.Bucket, ed.Object, "key1")
	if res == nil {
		t.Fatal("K/V object key1 not deleted")
	}

	res = s3x.KeyValueGet(ed.Bucket, ed.Object, "key2")
	if res == nil {
		t.Fatal("K/V object key2 not deleted")
	}
}

func TestKeyValueListCSV(t *testing.T) {
	key := ""
	res := s3x.KeyValueList(ed.Bucket, ed.Object, key, "", "text/csv", 100, true)
	if res != nil {
		t.Fatal("K/V object json list error", res)
	}
	fmt.Printf("K/V list from key: %s:\n%s\n", key, s3x.GetLastValue())
}

func TestKeyValueListJSON(t *testing.T) {
	key := "key4"
	res := s3x.KeyValueList(ed.Bucket, ed.Object, key, "", "application/json", 100, true)
	if res != nil {
		t.Fatal("K/V object json list error", res)
	}
	fmt.Printf("K/V list from key: %s:\n %s\n", key, s3x.GetLastValue())
}

func TestBucketList(t *testing.T) {
	res, err := s3x.BucketList()
	if err != nil {
		t.Fatal("Bucket list error", err)
	}
	fmt.Printf("Bucket list %v\n", res)
	found := false
	for i := range res {
		if res[i].Name == ed.Bucket {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Bucket not found: ", ed.Bucket)
	}
}

func TestObjectList(t *testing.T) {

	// Wait for trlog
	time.Sleep(45 * time.Second)
	res, err := s3x.ObjectList(ed.Bucket, "", "", 100)
	if err != nil {
		t.Fatal("Object list error", err)
	}
	fmt.Printf("Object list %v\n", res)
	found := false
	for i := range res {
		if res[i].Key == ed.Object {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Object not found: ", ed.Object)
	}
}

func TestKeyValueDelete(t *testing.T) {
	key := "key5"
	res := s3x.KeyValueDelete(ed.Bucket, ed.Object, key, false)
	if res != nil {
		t.Fatal("K/V object delete error", res)
	}
	res = s3x.KeyValueGet(ed.Bucket, ed.Object, key)
	if res == nil {
		t.Fatal("K/V object not deleted")
	}
}

func TestKeyValueObjectDelete(t *testing.T) {

	res := s3x.ObjectDelete(ed.Bucket, ed.Object)

	if res != nil {
		t.Fatal("K/V object delete error", res)
	}
}

func TestBucketDelete(t *testing.T) {

	res := s3x.BucketDelete(ed.Bucket)

	if res != nil {
		t.Fatal("Bucket delete error", res)
	}
}
