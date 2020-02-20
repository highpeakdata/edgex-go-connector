# edgex-go-connector
EdgeFS S3X Connector for Go programs

S3X interface for High-Performance load/unload/edit of billions of images
via object HTTP/S interface.

<p align="center">
  <img src="https://github.com/Nexenta/edgex-perl-connector/raw/master/edgefs-s3x-kv-benefits.png?raw=true" alt="edgefs-s3x-kv-benefits.png"/>
</p>

It is S3 compatible protocol, with extensions that allows batch operations
so that load of hundreds objects (like pictures, logs, packets, etc) can be
combined as one S3 emulated object.

## Folder's structure
```bash
.
+api -- S3xClient versioned interfaces folder
+pkg --  Versioned S3xClient implementations
|        +s3xclient --  Specific S3xClient interface implementations folder
         | -- {version} Specific version implementation
|        +errors      Error definitions related for S3xClient project
|        +utils       Global utils folder
+tests   Testify's test suits
```

## S3xClient Initialization

```go
	s3xApi "github.com/highpeakdata/edgex-go-connector/api/s3xclient/v1beta1"
	s3xErrors "github.com/highpeakdata/edgex-go-connector/pkg/errors"
	v1beta1 "github.com/highpeakdata/edgex-go-connector/pkg/s3xclient/v1beta1/"
	...

	client, err := v1beta1.CreateEdgex("http://{s3x-service-ip:port}", {s3x-service-auth}, {s3x-service-secretKey})
	if err != nil {
		log.Printf("Failed to create Edgex client: %v", err)
		os.Exit(1)
	}
```

## S3xClient method invocation

```go
	...
	bucketName := "{new bucket name}"
	err = client.BucketCreate(bucketName)
	if err != nil {
		log.Printf("Failed to create %s bucket: %v", bucketName, err)
		os.Exit(1)
	}
	...

	os.Exit(0)
```

## S3xClient run specific test suite

```bash
	#Before running tests edit test_setup.json
	#For example we would start bucket creation/validation/deletion test
	go test -count=1 -timeout 60s github.com/highpeakdata/edgex-go-connector/tests/e2e/bucket -run TestEnd2EndBucketTestSuite -v
	#Run object tests
	go test -count=1 -timeout 60s github.com/highpeakdata/edgex-go-connector/tests/e2e/object -run TestEnd2EndObjectTestSuite -v
	#Run key/value tests
	go test -count=1 -timeout 60s github.com/highpeakdata/edgex-go-connector/tests/e2e/kv -run TestEnd2EndKVTestSuite -v
```
