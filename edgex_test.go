package edgex

import (
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
	Url     string `json:"url"`
	Authkey string `json:"authkey"`
	Secret  string `json:"secret"`
	Bucket  string `json:"bucket"`
	Object  string `json:"object"`
	Debug   int    `json:"debug"`
}

var ed EdgexTest
var ex *Edgex

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

	log.Println("Create Edgex connection url:", ed.Url)

	ex = CreateEdgex(ed.Url, ed.Authkey, ed.Secret, ed.Debug)

	// Run tests
	exitVal := m.Run()

	log.Println("Close Edgex connection")
	CloseEdgex(ex)
	os.Exit(exitVal)
}

func TestBucketCreate(t *testing.T) {

	res := BucketCreate(ex, ed.Bucket)

	if res != nil {
		t.Fatal("Bucket create error", res)
	}
}

func TestBucketHead(t *testing.T) {

	res := BucketHead(ex, ed.Bucket)

	if res != nil {
		t.Fatal("Head error", res)
	}
}

func TestKeyValueCreate(t *testing.T) {

	res := KeyValueCreate(ex, ed.Bucket, ed.Object,
		"application/json", DEFAULT_CHUNKSIZE, DEFAULT_BTREE_ORDER)

	if res != nil {
		t.Fatal("K/V object create error", res)
	}
}

func TestObjectHead(t *testing.T) {

	res := ObjectHead(ex, ed.Bucket, ed.Object)

	if res != nil {
		t.Fatal("Object Head error", res)
	}
}

func TestNotObjectHead(t *testing.T) {

	res := ObjectHead(ex, ed.Bucket, "notobject")

	if res == nil {
		t.Fatal("Not Object Head error")
	}
}

func TestKeyValuePost(t *testing.T) {

	res := KeyValuePost(ex, ed.Bucket, ed.Object, "", "key1", "value1", true)
	if res != nil {
		t.Fatal("K/V object post1 error", res)
	}
	res = KeyValuePost(ex, ed.Bucket, ed.Object, "", "key2", "value2", true)
	if res != nil {
		t.Fatal("K/V object post1 error", res)
	}
}

func TestKeyValuePostJSON(t *testing.T) {

	json := ArrToJSON("key3", "value3", "key4", "value4", "key5", "value5")

	res := KeyValuePostJSON(ex, ed.Bucket, ed.Object, json, false)
	if res != nil {
		t.Fatal("K/V object json post error", res)
	}
}

func TestKeyValueGet(t *testing.T) {
	key := "key2"
	res := KeyValueGet(ex, ed.Bucket, ed.Object, key)
	if res != nil {
		t.Fatal("K/V object get error", res)
	}
	fmt.Printf("K/V get key: %s, value : %s\n", key, ex.Value)
}

func TestKeyValueDeleteJSON(t *testing.T) {

	json := ArrToJSON("key1", "", "key2", "")

	res := KeyValueDeleteJSON(ex, ed.Bucket, ed.Object, json, false)
	if res != nil {
		t.Fatal("K/V object json delete error", res)
	}

	res = KeyValueGet(ex, ed.Bucket, ed.Object, "key1")
	if res == nil {
		t.Fatal("K/V object key1 not deleted")
	}

	res = KeyValueGet(ex, ed.Bucket, ed.Object, "key2")
	if res != nil {
		t.Fatal("K/V object key2 not available")
	}
}

func TestKeyValueListCSV(t *testing.T) {
	key := "key4"
	res := KeyValueList(ex, ed.Bucket, ed.Object, key, "text/csv", 100, true)
	if res != nil {
		t.Fatal("K/V object json list error", res)
	}
	fmt.Printf("K/V list from key: %s:\n %s\n", key, ex.Value)
}

func TestKeyValueListJSON(t *testing.T) {
	key := "key4"
	res := KeyValueList(ex, ed.Bucket, ed.Object, key, "application/json", 100, true)
	if res != nil {
		t.Fatal("K/V object json list error", res)
	}
	fmt.Printf("K/V list from key: %s:\n %s\n", key, ex.Value)
}

func TestKeyValueDelete(t *testing.T) {
	key := "key5"
	res := KeyValueDelete(ex, ed.Bucket, ed.Object, key, false)
	if res != nil {
		t.Fatal("K/V object delete error", res)
	}
	res = KeyValueGet(ex, ed.Bucket, ed.Object, key)
	if res == nil {
		t.Fatal("K/V object not deleted")
	}
}

func TestKeyValueObjectDelete(t *testing.T) {

	res := ObjectDelete(ex, ed.Bucket, ed.Object)

	if res != nil {
		t.Fatal("K/V object delete error", res)
	}
}

func TestBucketDelete(t *testing.T) {

	res := BucketDelete(ex, ed.Bucket)

	if res != nil {
		t.Fatal("Bucket delete error", res)
	}
}
