package errors

import "errors"

var (
	ErrBucketExist    = errors.New("bucket already exists")
	ErrBucketNotExist = errors.New("bucket does not exist")
	ErrObjectExist    = errors.New("object already exists")
	ErrObjectNotExist = errors.New("object does not exist")
	ErrKeyNotExist    = errors.New("key does not exist")
)
