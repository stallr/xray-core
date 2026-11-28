//go:build unix

package buf_test

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"runtime/debug"
	"syscall"
	"testing"
	"time"

	. "github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/bytespool"
	"github.com/xtls/xray-core/transport/internet/stat"
)

func pooledTCPPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	l, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	c, err := net.DialTCP("tcp4", nil, l.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	s, err := l.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	for _, conn := range []*net.TCPConn{c, s} {
		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	return c, s
}

// Observe the real RawConn immediately after a callback declines readiness.
// The Read override observes the buffer held by the baseline adaptive reader.
type observedTCP struct {
	*net.TCPConn
	wait func(int)
}

func (c *observedTCP) Read(p []byte) (int, error) {
	if c.wait != nil {
		c.wait(len(p))
	}
	return c.TCPConn.Read(p)
}

func (c *observedTCP) SyscallConn() (syscall.RawConn, error) {
	raw, err := c.TCPConn.SyscallConn()
	return &observedRaw{RawConn: raw, conn: c}, err
}

type observedRaw struct {
	syscall.RawConn
	conn *observedTCP
}

func (r *observedRaw) Read(f func(uintptr) bool) error {
	return r.RawConn.Read(func(fd uintptr) bool {
		ready := f(fd)
		if !ready && r.conn.wait != nil {
			r.conn.wait(0)
		}
		return ready
	})
}

// With GC disabled and one P, a seeded pool entry can be followed across Get
// and Put without sync.Pool's permitted GC eviction or cross-P migration.
func pooledMarker(t *testing.T) ([]byte, func() []byte) {
	t.Helper()
	oldProcs := runtime.GOMAXPROCS(1)
	oldGC := debug.SetGCPercent(-1)
	p := bytespool.GetPool(32768)
	oldNew := p.New
	marker := make([]byte, 32768)
	empty := false
	p.New = func() any { empty = true; return marker }
	for !empty {
		p.Get()
	}
	p.New = oldNew
	p.Put(marker)
	t.Cleanup(func() {
		debug.SetGCPercent(oldGC)
		runtime.GOMAXPROCS(oldProcs)
	})
	return marker, func() []byte {
		b := p.Get().([]byte)
		p.Put(b)
		return b
	}
}

func TestPooledIdleHold(t *testing.T) {
	marker, peek := pooledMarker(t)
	c, peer := pooledTCPPair(t)
	observed := &observedTCP{TCPConn: c}
	r := NewPooledReader(observed)
	for _, n := range []int{8192, 100} {
		if _, err := peer.Write(bytes.Repeat([]byte{7}, n)); err != nil {
			t.Fatal(err)
		}
		mb, err := r.ReadMultiBuffer()
		got := mb.Len()
		ReleaseMulti(mb)
		if err != nil || got != int32(n) {
			t.Fatalf("warm read = %d, %v; want %d", got, err, n)
		}
	}
	held := make(chan int, 1)
	resume := make(chan struct{})
	observed.wait = func(n int) {
		if b := peek(); &b[0] != &marker[0] {
			n = 32768
		}
		held <- n
		<-resume
	}
	done := make(chan error, 1)
	go func() {
		mb, err := r.ReadMultiBuffer()
		ReleaseMulti(mb)
		done <- err
	}()
	select {
	case n := <-held:
		t.Logf("held=%d bytes at idle wait", n)
		if n > Size {
			t.Errorf("idle held=%d bytes; want <= %d", n, Size)
		}
	case <-time.After(3 * time.Second):
		t.Error("reader did not enter idle wait")
	}
	// Let the real poller resume and observe EOF, then join before restoring GC.
	if err := peer.CloseWrite(); err != nil {
		t.Error(err)
	}
	close(resume)
	if err := <-done; err != io.EOF {
		t.Errorf("idle completion = %v; want EOF", err)
	}
}

func TestPooledBusyStream(t *testing.T) {
	c, peer := pooledTCPPair(t)
	r := NewPooledReader(c)
	full := 0
	for i, n := range []int{8192, 32768, 32768, 100, 32768, 32768} {
		payload := bytes.Repeat([]byte{byte(i + 1)}, n)
		if _, err := peer.Write(payload); err != nil {
			t.Fatal(err)
		}
		for offset := 0; offset < n; {
			mb, err := r.ReadMultiBuffer()
			wantCap := 32768
			if i == 0 {
				wantCap = Size
			}
			if err != nil || len(mb) != 1 || mb.Len() == 0 {
				ReleaseMulti(mb)
				t.Fatalf("read %d = %v; want one nonempty buffer", i, err)
			}
			got := int(mb.Len())
			if cap(mb[0].Bytes()) != wantCap || got > n-offset || !bytes.Equal(mb[0].Bytes(), payload[offset:offset+got]) {
				t.Errorf("read %d capacity = %d, bytes = %d; want capacity %d and ordered payload", i, cap(mb[0].Bytes()), got, wantCap)
			}
			if got == 32768 {
				full++
			}
			ReleaseMulti(mb)
			offset += got
		}
	}
	if full < 2 {
		t.Fatalf("full 32 KiB reads = %d; want at least two", full)
	}
}

func TestPooledRelease(t *testing.T) {
	marker, peek := pooledMarker(t)
	c, peer := pooledTCPPair(t)
	r := NewPooledReader(c)
	for _, n := range []int{8192, 32768} {
		if _, err := peer.Write(make([]byte, n)); err != nil {
			t.Fatal(err)
		}
		mb, err := r.ReadMultiBuffer()
		if err != nil || len(mb) != 1 || mb.Len() == 0 {
			ReleaseMulti(mb)
			t.Fatalf("read = %v; want %d bytes", err, n)
		}
		if n == 32768 && &mb[0].Bytes()[0] != &marker[0] {
			t.Error("read did not use the existing 32 KiB pool")
		}
		ReleaseMulti(mb)
	}
	if b := peek(); &b[0] != &marker[0] {
		t.Fatal("released buffer missing from pool; want seeded allocation back")
	}
}

func TestPooledFallback(t *testing.T) {
	c, peer := pooledTCPPair(t)
	r := NewPooledReader(&stat.CounterConnection{Connection: c})
	for i, n := range []int{8192, 100, 8192, 8192} {
		if _, err := peer.Write(make([]byte, n)); err != nil {
			t.Fatal(err)
		}
		mb, err := r.ReadMultiBuffer()
		wantCap := 32768
		if i == 0 || i == 2 {
			wantCap = 8192
		}
		if err != nil || len(mb) != 1 || mb.Len() != int32(n) || cap(mb[0].Bytes()) != wantCap {
			ReleaseMulti(mb)
			t.Fatalf("fallback read %d: want %d bytes, capacity %d, no error; got %v", i, n, wantCap, err)
		}
		ReleaseMulti(mb)
	}
	for _, readErr := range []error{nil, io.EOF, io.ErrUnexpectedEOF} {
		for _, n := range []int{0, 17} {
			r := NewPooledReader(adaptiveReadFunc(func(p []byte) (int, error) { return n, readErr }))
			mb, err := r.ReadMultiBuffer()
			got := mb.Len()
			ReleaseMulti(mb)
			if got != int32(n) || err != readErr {
				t.Fatalf("fallback = %d, %v; want %d, %v", got, err, n, readErr)
			}
		}
	}
}

func TestPooledEOFAndDeadline(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		c, peer := pooledTCPPair(t)
		r := NewPooledReader(c)
		if deadline {
			if err := c.SetReadDeadline(time.Now().Add(10 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
		} else if err := peer.CloseWrite(); err != nil {
			t.Fatal(err)
		}
		mb, err := r.ReadMultiBuffer()
		ReleaseMulti(mb)
		if len(mb) != 0 {
			t.Fatal("zero-byte read returned a buffer")
		}
		if deadline {
			var op *net.OpError
			if !errors.Is(err, os.ErrDeadlineExceeded) || !errors.As(err, &op) || op.Op != "read" || !op.Timeout() {
				t.Fatalf("deadline = %v; want read OpError wrapping deadline exceeded", err)
			}
			_, wantErr := c.Read(make([]byte, 1))
			if err.Error() != wantErr.Error() {
				t.Fatalf("deadline = %v; want %v", err, wantErr)
			}
		} else if err != io.EOF {
			t.Fatalf("closed peer = %v; want EOF", err)
		}
	}
}
