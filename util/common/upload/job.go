package upload

import (
	"context"
	"fmt"
	"io"
)

// FileUploadJob represents a single file upload operation
type FileUploadJob interface {
	// GetID returns a unique identifier for this upload job
	GetID() string

	// GetFilePath returns the file path (for display purposes)
	GetFilePath() string

	// GetFileSize returns the file size in bytes
	GetFileSize() int64

	// Upload performs the actual upload operation
	Upload(ctx context.Context) error
}

// BaseFileUploadJob provides common functionality for file upload jobs
type BaseFileUploadJob struct {
	ID       string
	FilePath string
	FileSize int64
}

func (b *BaseFileUploadJob) GetID() string {
	return b.ID
}

func (b *BaseFileUploadJob) GetFilePath() string {
	return b.FilePath
}

func (b *BaseFileUploadJob) GetFileSize() int64 {
	return b.FileSize
}

// FileUploadResult represents the result of a file upload
type FileUploadResult struct {
	JobID    string
	FilePath string
	FileSize int64
	Error    error
	Success  bool
}

// FileContent represents file content that can be either from disk or in-memory
type FileContent struct {
	Reader     io.Reader
	Size       int64
	IsInMemory bool
	Data       []byte
}

// NewFileContentFromDisk creates FileContent from a file path
func NewFileContentFromDisk(reader io.Reader, size int64) *FileContent {
	return &FileContent{
		Reader:     reader,
		Size:       size,
		IsInMemory: false,
	}
}

// NewFileContentFromMemory creates FileContent from in-memory data
func NewFileContentFromMemory(data []byte) *FileContent {
	return &FileContent{
		Reader:     nil,
		Size:       int64(len(data)),
		IsInMemory: true,
		Data:       data,
	}
}

// GetReader returns an io.Reader for the file content
func (fc *FileContent) GetReader() (io.Reader, error) {
	if fc.IsInMemory {
		if fc.Data == nil {
			return nil, fmt.Errorf("in-memory file content has no data")
		}
		return io.NopCloser(newBytesReader(fc.Data)), nil
	}
	if fc.Reader == nil {
		return nil, fmt.Errorf("file content has no reader")
	}
	return fc.Reader, nil
}

func newBytesReader(data []byte) io.Reader {
	return &bytesReaderWrapper{data: data, pos: 0}
}

type bytesReaderWrapper struct {
	data []byte
	pos  int
}

func (b *bytesReaderWrapper) Read(p []byte) (n int, err error) {
	if b.pos >= len(b.data) {
		return 0, io.EOF
	}
	n = copy(p, b.data[b.pos:])
	b.pos += n
	return n, nil
}
