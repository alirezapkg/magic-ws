package magicws

import "sync"

const ChunkSize = 4096

var byteSlicePool = sync.Pool{
	New: func() any {
		b := make([]byte, ChunkSize)
		return &b
	},
}

func GetByteSlice() *[]byte {
	return byteSlicePool.Get().(*[]byte)
}

func PutByteSlice(b *[]byte) {
	if b == nil || cap(*b) < ChunkSize {
		return
	}
	*b = (*b)[:0] // Reset length
	byteSlicePool.Put(b)
}
