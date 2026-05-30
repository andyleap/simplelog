package store

import (
	"bytes"
	"io"
	"strings"

	s3 "github.com/jhunt/go-s3"
)

// s3UploadBlock is the multipart part size. go-s3 requires >= 5MiB; a smaller
// final/only part is allowed by S3, so this works for tiny objects too.
const s3UploadBlock = 5 * 1024 * 1024

// S3Config configures an S3-backed Blobs store.
type S3Config struct {
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	Bucket          string
	Domain          string // e.g. "s3.amazonaws.com" or "minio.local:9000"
	Protocol        string // "http" or "https"; default https
	UsePathBuckets  bool   // true for MinIO/path-style endpoints
}

// S3Blobs implements Blobs against an S3-compatible endpoint via go-s3, which
// has no ranged-GET support — Get always downloads the whole object.
type S3Blobs struct {
	c *s3.Client
}

// NewS3Blobs constructs an S3-backed blob store.
func NewS3Blobs(cfg S3Config) (*S3Blobs, error) {
	c, err := s3.NewClient(&s3.Client{
		AccessKeyID:     cfg.AccessKeyID,
		SecretAccessKey: cfg.SecretAccessKey,
		Region:          cfg.Region,
		Bucket:          cfg.Bucket,
		Domain:          cfg.Domain,
		Protocol:        cfg.Protocol,
		UsePathBuckets:  cfg.UsePathBuckets,
	})
	if err != nil {
		return nil, err
	}
	return &S3Blobs{c: c}, nil
}

func (s *S3Blobs) Get(key string) ([]byte, error) {
	r, err := s.c.Get(key)
	if err != nil {
		if isS3NotFound(err) {
			return nil, &NotFoundError{Key: key}
		}
		return nil, err
	}
	return io.ReadAll(r)
}

func (s *S3Blobs) Put(key string, data []byte) error {
	return s.PutReader(key, bytes.NewReader(data))
}

func (s *S3Blobs) PutReader(key string, r io.Reader) error {
	up, err := s.c.NewUpload(key, nil)
	if err != nil {
		return err
	}
	if _, err := up.Stream(r, s3UploadBlock); err != nil {
		return err
	}
	return up.Done()
}

func (s *S3Blobs) List(prefix string) ([]string, error) {
	objs, err := s.c.List()
	if err != nil {
		return nil, err
	}
	var out []string
	for _, o := range objs {
		if strings.HasPrefix(o.Key, prefix) {
			out = append(out, o.Key)
		}
	}
	return out, nil
}

func (s *S3Blobs) Delete(key string) error {
	if err := s.c.Delete(key); err != nil && !isS3NotFound(err) {
		return err
	}
	return nil
}

func isS3NotFound(err error) bool {
	if err == nil {
		return false
	}
	// go-s3 surfaces non-200s as a generic error embedding the S3 error code.
	msg := err.Error()
	return strings.Contains(msg, "NoSuchKey") || strings.Contains(msg, "404")
}

var _ Blobs = (*S3Blobs)(nil)
