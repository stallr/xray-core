//go:build unix

package buf

import (
	"io"
	"net"
	"os"
	"syscall"
)

// NewPooledReader reads TCP responses with pooled buffers that are released
// before waiting for readability. Readers without syscall.Conn keep the
// adaptive reader's behavior. It does not take ownership of reader.
func NewPooledReader(reader io.Reader) Reader {
	if mr, ok := reader.(Reader); ok {
		return mr
	}
	if conn, ok := reader.(syscall.Conn); ok {
		if raw, err := conn.SyscallConn(); err == nil {
			return &pooledReader{reader: reader, raw: raw, size: Size}
		}
	}
	return NewAdaptiveReader(reader)
}

type pooledReader struct {
	reader io.Reader
	raw    syscall.RawConn
	size   int32
}

func (r *pooledReader) ReadMultiBuffer() (MultiBuffer, error) {
	var b *Buffer
	var readErr error
	err := r.raw.Read(func(fd uintptr) bool {
		b = NewWithSize(r.size)
		var n int
		for {
			n, readErr = syscall.Read(int(fd), b.v)
			if readErr != syscall.EINTR {
				break
			}
		}
		if readErr == syscall.EAGAIN || readErr == syscall.EWOULDBLOCK {
			// RawConn waits only after false; it must not retain this allocation.
			b.Release()
			b = nil
			return false
		}
		if n > 0 {
			b.end = int32(n)
			if n == int(r.size) {
				r.size = 32768
			}
		} else if readErr == nil {
			readErr = io.EOF
		}
		return true
	})
	if err == nil {
		err = readErr
		if err != nil && err != io.EOF {
			if _, ok := err.(syscall.Errno); ok {
				err = os.NewSyscallError("read", err)
			}
			if conn, ok := r.reader.(net.Conn); ok {
				err = &net.OpError{Op: "read", Net: conn.LocalAddr().Network(), Source: conn.LocalAddr(), Addr: conn.RemoteAddr(), Err: err}
			}
		}
	} else if op, ok := err.(*net.OpError); ok {
		readOp := *op
		readOp.Op = "read"
		err = &readOp
	}
	if b == nil || b.IsEmpty() {
		b.Release()
		return nil, err
	}
	return MultiBuffer{b}, err
}
