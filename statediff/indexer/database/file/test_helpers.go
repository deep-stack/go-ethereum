package file

import "io"

type MemWriter struct {
	bytes []byte
}

func NewMemWriter() io.WriteCloser {
	return &MemWriter{}
}

// Write satisfies io.WriteCloser
func (mw *MemWriter) Write(b []byte) (int, error) {
	mw.bytes = append(mw.bytes, b...)
	return len(b), nil
}

// Close satisfies io.WriteCloser
func (mw *MemWriter) Close() error {
	mw.bytes = []byte{}
	return nil
}

// ReadAll returns all the bytes written to the memory writer
func (mw *MemWriter) ReadAll() []byte {
	return mw.bytes
}
