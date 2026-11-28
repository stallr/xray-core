//go:build !unix

package buf

import "io"

// NewPooledReader keeps the adaptive reader on platforms without Unix reads.
func NewPooledReader(reader io.Reader) Reader {
	return NewAdaptiveReader(reader)
}
