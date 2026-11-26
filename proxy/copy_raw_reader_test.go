package proxy

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/signal"
	"github.com/xtls/xray-core/transport/internet/stat"
)

type recordingRawWriter struct{ bytes.Buffer }

func (w *recordingRawWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	defer buf.ReleaseMulti(mb)
	for _, b := range mb {
		w.Buffer.Write(b.Bytes())
	}
	return nil
}

func TestCopyRawCountOnce(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	sender, err := net.DialTimeout("tcp", listener.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	receiver, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	if err := receiver.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	want := bytes.Repeat([]byte("0123456789"), 10000)
	sent := make(chan error, 1)
	go func() { _, err := sender.Write(want); sender.Close(); sent <- err }()
	counter := new(stats.Counter)
	wrapped := &stat.CounterConnection{Connection: receiver, ReadCounter: counter}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	timer := signal.CancelAfterInactivity(ctx, cancel, time.Second)
	defer timer.SetTimeout(0)
	writer := new(recordingRawWriter)
	if err := CopyRawConnIfExist(ctx, wrapped, nil, writer, timer, nil, buf.NewAdaptiveReader); err != nil {
		t.Fatal(err)
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	if counter.Value() != int64(len(want)) || !bytes.Equal(writer.Bytes(), want) {
		t.Fatalf("counter = %d, bytes = %d; want 100000 once, in order", counter.Value(), writer.Len())
	}
}
