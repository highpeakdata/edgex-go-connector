# edgex-go-connector
EdgeFS S3X Connector for Go programs

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
	s3xApi "github.com/Nexenta/edgex-go-connector/api/s3xclient/v1beta1"
	s3xErrors "github.com/Nexenta/edgex-go-connector/pkg/errors"
	v1beta1 "github.com/Nexenta/edgex-go-connector/pkg/s3xclient/v1beta1/"
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
	#For example we would start bucket creation/validation/deletion test
	go test -timeout 30s github.com\Nexenta\edgex-go-connector\tests\e2e\bucket -run ^(TestEnd2EndBucketTestSuite)$ -v
```
