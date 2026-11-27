package buf_test

import (
	"bytes"
	"io"
	"testing"
	"time"

	. "github.com/xtls/xray-core/common/buf"
)

type adaptiveReadFunc func([]byte) (int, error)

func (f adaptiveReadFunc) Read(p []byte) (int, error) { return f(p) }

func TestAdaptiveNoFill(t *testing.T) {
	closed := make(chan struct{})
	defer close(closed)
	calls := 0
	r := NewAdaptiveReader(adaptiveReadFunc(func(p []byte) (int, error) {
		calls++
		if calls == 1 {
			return 100, nil
		}
		<-closed
		return 0, io.EOF
	}))
	done := make(chan bool, 1)
	go func() {
		mb, err := r.ReadMultiBuffer()
		defer ReleaseMulti(mb)
		done <- err == nil && len(mb) == 1 && mb.Len() == 100 && calls == 1
	}()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("short read did not return one 100-byte buffer in one Read")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("short read blocked waiting to fill")
	}
}

func testAdaptiveSizes(t *testing.T, sizes, lengths []int) {
	t.Helper()
	calls := 0
	r := NewAdaptiveReader(adaptiveReadFunc(func(p []byte) (int, error) {
		if calls >= len(sizes) || len(p) != sizes[calls] {
			t.Fatalf("Read %d capacity = %d; want capacities %v", calls, len(p), sizes)
		}
		n := lengths[calls]
		calls++
		return n, nil
	}))
	for i, want := range lengths {
		mb, err := r.ReadMultiBuffer()
		ok := err == nil && len(mb) == 1 && int(mb.Len()) == want && calls == i+1
		ReleaseMulti(mb)
		if !ok {
			t.Fatalf("call %d: want one buffer of %d bytes and one Read", i, want)
		}
	}
}

func TestAdaptiveGrow(t *testing.T) {
	testAdaptiveSizes(t, []int{8192, 32768}, []int{8192, 32768})
}

func TestAdaptiveSticky(t *testing.T) {
	testAdaptiveSizes(t, []int{8192, 32768, 32768, 32768}, []int{8192, 32768, 100, 32768})
}

func TestAdaptiveOrderEOF(t *testing.T) {
	want := make([]byte, 200000)
	for i := range want {
		want[i] = byte(i * 37)
	}
	offset, calls := 0, 0
	chunks := []int{8192, 32768, 100, 4096, 32768}
	r := NewAdaptiveReader(adaptiveReadFunc(func(p []byte) (int, error) {
		n := copy(p[:min(len(p), chunks[calls%len(chunks)])], want[offset:])
		calls++
		offset += n
		if offset == len(want) {
			return n, io.EOF
		}
		return n, nil
	}))
	var got []byte
	for i := 1; ; i++ {
		mb, err := r.ReadMultiBuffer()
		if len(mb) != 1 || calls != i {
			t.Fatalf("call %d: buffers = %d, Reads = %d", i, len(mb), calls)
		}
		got = append(got, mb[0].Bytes()...)
		ReleaseMulti(mb)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("sequence mismatch: got %d bytes, want %d", len(got), len(want))
	}
	mb, err := r.ReadMultiBuffer()
	defer ReleaseMulti(mb)
	if mb != nil || err != io.EOF {
		t.Fatalf("after tail: buffers = %d, error = %v", len(mb), err)
	}
}

func TestAdaptivePassThrough(t *testing.T) {
	r := &BufferedReader{Reader: NewReader(bytes.NewReader(nil))}
	if NewAdaptiveReader(r) != r {
		t.Fatal("existing Reader was not returned unchanged")
	}
}
