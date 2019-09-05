package utils

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_GetAbsRootProjectPath(t *testing.T) {
	// OS independent path representation
	rootPath := filepath.FromSlash("/root/go/src/github.com/Nexenta/edgex-go-connector")
	validTestPath := filepath.FromSlash("/root/go/src/github.com/Nexenta/edgex-go-connector/tests/e2e/bucket")
	validTestPath2 := filepath.FromSlash("/root/go/src/github.com/Nexenta/edgex-go-connector/")
	invalidTestPath := filepath.FromSlash("/root/go/src/github.com/Nexenta/another-project/tests/e2e/bucket")

	validRootPath, err := GetAbsRootProjectPath(validTestPath)
	assert.Nil(t, err)
	assert.Equal(t, rootPath, validRootPath)

	validRootPath2, err := GetAbsRootProjectPath(validTestPath2)
	assert.Nil(t, err)
	assert.Equal(t, rootPath, validRootPath2)

	_, err = GetAbsRootProjectPath(invalidTestPath)
	assert.NotNil(t, err)
}
