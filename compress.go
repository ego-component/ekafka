package ekafka

import (
	"bytes"
	"compress/gzip"
	"io"
	"sync"
)

var (
	compressorsMu     sync.RWMutex
	compressors       = make(map[string]Compressor)
	defaultCompressor = &gzipCompressor{}
)

const (
	compressTypeGzip = "gzip"
	// 10K
	defaultCompressLimit = 1024_0
)

type Compressor interface {
	Compress(input []byte) (output []byte, err error)
	DeCompress(input []byte) (output []byte, err error)
	ContentEncoding() string
}

func Register(comp Compressor) {
	compressorsMu.Lock()
	defer compressorsMu.Unlock()
	compressors[comp.ContentEncoding()] = comp
}

func init() {
	Register(defaultCompressor)
}

func GetCompressor(encoding string) Compressor {
	if encoding == "" {
		return defaultCompressor
	}
	compressorsMu.RLock()
	defer compressorsMu.RUnlock()
	return compressors[encoding]
}

type gzipCompressor struct {
}

func (g *gzipCompressor) Compress(input []byte) (output []byte, err error) {
	var compressedBuffer bytes.Buffer
	gw := gzip.NewWriter(&compressedBuffer)
	_, err = gw.Write(input)
	if err != nil {
		_ = gw.Close()
		return nil, err
	}
	if err = gw.Close(); err != nil {
		return nil, err
	}
	return compressedBuffer.Bytes(), nil
}

func (g *gzipCompressor) DeCompress(input []byte) (output []byte, err error) {
	reader, err := gzip.NewReader(bytes.NewReader(input))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	var deCompressedBuffer bytes.Buffer
	_, err = io.Copy(&deCompressedBuffer, reader)
	if err != nil {
		return nil, err
	}
	return deCompressedBuffer.Bytes(), nil
}

func (g *gzipCompressor) ContentEncoding() string {
	return compressTypeGzip
}
