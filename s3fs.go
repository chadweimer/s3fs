package s3fs

import (
	"io/fs"
	"log"
	"net/http"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type s3FS struct {
	bucket string
}

func New(bucket string) fs.FS {
	return s3FS{bucket}
}

func (f s3FS) Open(name string) (fs.File, error) {
	// if the path is '/', move along because we'll just get bucket information
	if name == "/" {
		return nil, fs.ErrPermission
	}

	svc := s3.New(session.Must(session.NewSession()))

	key := filepath.ToSlash(name)

	// Make the request and check the error
	getResp, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: &f.bucket,
		Key:    &key,
	})
	if err != nil {
		log.Print(err.Error())
		if reqerr, ok := err.(awserr.RequestFailure); ok {
			if reqerr.StatusCode() == http.StatusNotFound {
				return nil, fs.ErrNotExist
			}
		}
	}

	// If we got content, send it back to the caller as a http.File
	var contentLength int64
	if getResp.ContentLength != nil {
		contentLength = *getResp.ContentLength
	}
	return &s3File{key: key, obj: getResp, ReadSeeker: newLazyReadSeeker(getResp.Body, contentLength)}, nil
}
