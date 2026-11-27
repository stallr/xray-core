package pipe_test

import (
	"io"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	. "github.com/xtls/xray-core/transport/pipe"
)

func TestPipeTimeoutP1NoTimer(t *testing.T) {
	reader, _ := New()
	defer reader.Interrupt()
	allocs := testing.AllocsPerRun(100, func() {
		mb, err := reader.ReadMultiBufferTimeout(0)
		if mb != nil || err != buf.ErrReadTimeout {
			t.Fatalf("empty read = %v, %v; want nil, ErrReadTimeout", mb, err)
		}
	})
	if allocs != 0 {
		t.Fatalf("zero-timeout allocations = %g; want 0", allocs)
	}
}

func TestPipeTimeoutP2Queued(t *testing.T) {
	reader, writer := New()
	defer reader.Interrupt()
	for _, s := range []string{"first", "second"} {
		b := buf.New()
		b.WriteString(s)
		if err := writer.WriteMultiBuffer(buf.MultiBuffer{b}); err != nil {
			t.Fatal(err)
		}
	}
	mb, err := reader.ReadMultiBufferTimeout(0)
	defer buf.ReleaseMulti(mb)
	if err != nil || mb.String() != "firstsecond" {
		t.Fatalf("queued read = %q, %v; want firstsecond, nil", mb.String(), err)
	}
}

func TestPipeTimeoutP3Done(t *testing.T) {
	reader, writer := New()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	mb, err := reader.ReadMultiBufferTimeout(0)
	defer buf.ReleaseMulti(mb)
	if mb != nil || err != io.EOF {
		t.Fatalf("closed read = %v, %v; want nil, EOF", mb, err)
	}
}

func TestPipeTimeoutP4Signal(t *testing.T) {
	reader, writer := New(WithSizeLimit(0))
	defer reader.Interrupt()
	b := buf.New()
	b.WriteString("queued")
	if err := writer.WriteMultiBuffer(buf.MultiBuffer{b}); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	written := make(chan error, 1)
	go func() {
		b := buf.New()
		b.WriteString("next")
		close(started)
		written <- writer.WriteMultiBuffer(buf.MultiBuffer{b})
	}()
	<-started
	select {
	case err := <-written:
		t.Fatalf("writer completed on full pipe: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	mb, err := reader.ReadMultiBufferTimeout(0)
	defer buf.ReleaseMulti(mb)
	if err != nil || mb.String() != "queued" {
		t.Fatalf("limited read = %q, %v; want queued, nil", mb.String(), err)
	}
	select {
	case err := <-written:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("writer was not released within 1 s")
	}
}

func TestPipeTimeoutP5Positive(t *testing.T) {
	reader, writer := New()
	defer reader.Interrupt()
	written := make(chan error, 1)
	go func() {
		time.Sleep(10 * time.Millisecond)
		b := buf.New()
		b.WriteString("delayed")
		written <- writer.WriteMultiBuffer(buf.MultiBuffer{b})
	}()
	mb, err := reader.ReadMultiBufferTimeout(50 * time.Millisecond)
	defer buf.ReleaseMulti(mb)
	if err != nil || mb.String() != "delayed" {
		t.Fatalf("positive read = %q, %v; want delayed, nil", mb.String(), err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
}
