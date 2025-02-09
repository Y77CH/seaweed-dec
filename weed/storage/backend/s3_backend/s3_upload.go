package s3_backend

import (
	"fmt"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
)

// s3UploadProgressedDiskFileReader implements io.Reader by wrapping a DiskFile.
type s3UploadProgressedDiskFileReader struct {
	df      *backend.DiskFile
	size    int64
	offset  int64
	signMap map[int64]struct{}
	fn      func(progressed int64, percentage float32) error
}

func (r *s3UploadProgressedDiskFileReader) Read(p []byte) (int, error) {
	n, err := r.df.ReadAt(p, r.offset)
	if n > 0 {
		r.offset += int64(n)
		percentage := float32(r.offset*100) / float32(r.size)
		if r.fn != nil {
			if errCb := r.fn(r.offset, percentage); errCb != nil {
				return n, errCb
			}
		}
	}
	return n, err
}

func uploadToS3(sess s3iface.S3API, df *backend.DiskFile, destBucket string, destKey string, storageClass string, fn func(progressed int64, percentage float32) error) (fileSize int64, err error) {

	// Get the file size from DiskFile.
	fileSize, _, err = df.GetStat()
	if err != nil {
		return 0, fmt.Errorf("failed to get stat of file %q, %v", df.Name(), err)
	}

	partSize := int64(64 * 1024 * 1024) // The minimum/default allowed part size is 64MB.
	for partSize*1000 < fileSize {
		partSize *= 4
	}

	// Create an uploader with the session and custom options.
	uploader := s3manager.NewUploaderWithClient(sess, func(u *s3manager.Uploader) {
		u.PartSize = partSize
		u.Concurrency = 5
	})

	// Create a reader that reads from the DiskFile.
	fileReader := &s3UploadProgressedDiskFileReader{
		df:      df,
		size:    fileSize,
		offset:  0,
		signMap: map[int64]struct{}{},
		fn:      fn,
	}

	// Upload the file to S3.
	var result *s3manager.UploadOutput
	result, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:       aws.String(destBucket),
		Key:          aws.String(destKey),
		Body:         fileReader,
		StorageClass: aws.String(storageClass),
	})

	// In case it fails to upload.
	if err != nil {
		return 0, fmt.Errorf("failed to upload file %s: %v", df.Name(), err)
	}
	glog.V(1).Infof("file %s uploaded to %s\n", df.Name(), result.Location)

	return fileSize, nil
}

// adapted from https://github.com/aws/aws-sdk-go/pull/1868
// https://github.com/aws/aws-sdk-go/blob/main/example/service/s3/putObjectWithProcess/putObjWithProcess.go
type s3UploadProgressedReader struct {
	fp      *os.File
	size    int64
	read    int64
	signMap map[int64]struct{}
	mux     sync.Mutex
	fn      func(progressed int64, percentage float32) error
}

func (r *s3UploadProgressedReader) Read(p []byte) (int, error) {
	return r.fp.Read(p)
}

func (r *s3UploadProgressedReader) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.fp.ReadAt(p, off)
	if err != nil {
		return n, err
	}

	r.mux.Lock()
	// Ignore the first signature call
	if _, ok := r.signMap[off]; ok {
		r.read += int64(n)
	} else {
		r.signMap[off] = struct{}{}
	}
	r.mux.Unlock()

	if r.fn != nil {
		read := r.read
		if err := r.fn(read, float32(read*100)/float32(r.size)); err != nil {
			return n, err
		}
	}

	return n, err
}

func (r *s3UploadProgressedReader) Seek(offset int64, whence int) (int64, error) {
	return r.fp.Seek(offset, whence)
}
