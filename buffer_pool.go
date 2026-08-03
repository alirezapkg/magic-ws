package magicws

import (
	"bytes"
	"sync"
)

const defaultBufferSize = 4096

var bufPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, defaultBufferSize))
	},
}

func GetBuffer() *bytes.Buffer {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	return buf
}

func PutBuffer(buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	buf.Reset()
	bufPool.Put(buf)
}
