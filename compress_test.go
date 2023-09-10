package ekafka

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	testData = `Hello 你好😂`
)

func TestGzipCompressor_Compress(t *testing.T) {
	output, err := defaultCompressor.Compress([]byte(testData))
	if err != nil {
		panic(err)
	}
	deCompress, err := defaultCompressor.DeCompress(output)
	if err != nil {
		panic(err)
	}
	assert.Equal(t, testData, string(deCompress))
}
