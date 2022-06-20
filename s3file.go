package s3fs

import (
	"io"
	"io/fs"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

type s3File struct {
	io.ReadSeeker
	key string
	obj *s3.GetObjectOutput
}

func (f *s3File) Close() error {
	if f.obj.Body != nil {
		return f.obj.Body.Close()
	}

	return nil
}
func (f *s3File) Readdir(count int) ([]fs.FileInfo, error) { return []fs.FileInfo{}, nil }
func (f *s3File) Stat() (fs.FileInfo, error)               { return &s3FileInfo{obj: f.obj, key: f.key}, nil }

type s3FileInfo struct {
	key string
	obj *s3.GetObjectOutput
}

func (f *s3FileInfo) Name() string { return f.key }
func (f *s3FileInfo) Size() int64 {
	if f.obj.ContentLength == nil {
		return 0
	}

	return *f.obj.ContentLength
}
func (f *s3FileInfo) Mode() fs.FileMode  { return fs.ModePerm }
func (f *s3FileInfo) ModTime() time.Time { return *f.obj.LastModified }
func (f *s3FileInfo) IsDir() bool        { return false }
func (f *s3FileInfo) Sys() interface{}   { return f.obj }
