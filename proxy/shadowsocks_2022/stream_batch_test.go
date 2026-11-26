package shadowsocks_2022

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"slices"
	"testing"

	"github.com/xtls/xray-core/common/buf"
)

func TestCoalescedStreamAllMethods(t *testing.T) {
	for _, method := range []string{MethodAES128GCM, MethodAES256GCM, MethodChaCha20Poly1305} {
		for _, size := range []int{1, 8192, 8193, 65535, 65536, 131073} {
			t.Run(fmt.Sprintf("%s/%d", method, size), func(t *testing.T) {
				m, e := GetCipherMethod(method)
				if e != nil {
					t.Fatal(e)
				}
				key := make([]byte, m.KeySaltLength)
				if _, e = rand.Read(key); e != nil {
					t.Fatal(e)
				}
				enc, e := m.NewAEAD(key)
				if e != nil {
					t.Fatal(e)
				}
				dec, e := m.NewAEAD(key)
				if e != nil {
					t.Fatal(e)
				}
				payload := make([]byte, size)
				for i := range payload {
					payload[i] = byte(i*31 + 7)
				}
				var wire, got bytes.Buffer
				writer := NewStreamWriter(&wire, enc)
				if e := writer.WriteMultiBuffer(buf.MergeBytes(nil, payload)); e != nil {
					t.Fatal(e)
				}
				reader := NewStreamReader(&wire, dec)
				if e := buf.Copy(reader, buf.NewWriter(&got)); e != nil {
					t.Fatal(e)
				}
				if _, e := reader.ReadMultiBuffer(); e != io.EOF {
					t.Fatalf("after payload: got %v, want EOF", e)
				}
				if !bytes.Equal(got.Bytes(), payload) {
					t.Fatalf("decoded=%d want=%d", got.Len(), len(payload))
				}
			})
		}
	}
}

type streamBatchRecordingWriter struct {
	bytes.Buffer
	writeLengths []int
}

func (w *streamBatchRecordingWriter) Write(p []byte) (int, error) {
	w.writeLengths = append(w.writeLengths, len(p))
	return w.Buffer.Write(p)
}

func TestCoalescedStreamPins(t *testing.T) {
	for _, method := range []string{MethodAES128GCM, MethodAES256GCM, MethodChaCha20Poly1305} {
		for _, tc := range []struct {
			name     string
			count    int
			size     int
			payloads []int
		}{
			{"P1_FOUR_INTO_ONE", 4, 8192, []int{32768}},
			{"P2_FIVE_INTO_TWO", 5, 8192, []int{32768, 8192}},
			{"P3_SINGLE_8192", 1, 8192, []int{8192}},
			{"P3_SINGLE_32769", 1, 32769, []int{32769}},
			{"P4_READER_BOUND", 4, 8192, nil},
		} {
			t.Run(method+"/"+tc.name, func(t *testing.T) {
				m, err := GetCipherMethod(method)
				if err != nil {
					t.Fatal(err)
				}
				key := make([]byte, m.KeySaltLength)
				if _, err := rand.Read(key); err != nil {
					t.Fatal(err)
				}
				enc, err := m.NewAEAD(key)
				if err != nil {
					t.Fatal(err)
				}
				payload := make([]byte, tc.count*tc.size)
				for i := range payload {
					payload[i] = byte(i*31 + 7)
				}
				var mb buf.MultiBuffer
				for i := 0; i < tc.count; i++ {
					mb = append(mb, buf.FromBytes(payload[i*tc.size:(i+1)*tc.size]))
				}
				var wire streamBatchRecordingWriter
				if err := NewStreamWriter(&wire, enc).WriteMultiBuffer(mb); err != nil {
					t.Fatal(err)
				}
				if tc.payloads != nil {
					var payloads []int
					for _, n := range wire.writeLengths {
						payloads = append(payloads, n-2-2*AEADTagSize)
					}
					if !slices.Equal(payloads, tc.payloads) {
						t.Fatalf("record payloads=%v want=%v", payloads, tc.payloads)
					}
					return
				}
				dec, err := m.NewAEAD(key)
				if err != nil {
					t.Fatal(err)
				}
				reader := NewStreamReader(&wire, dec)
				var got []byte
				for {
					mb, err := reader.ReadMultiBuffer()
					for _, b := range mb {
						if b.Len() > buf.Size {
							t.Errorf("reader buffer length=%d exceeds buf.Size=%d", b.Len(), buf.Size)
						}
						got = append(got, b.Bytes()...)
					}
					buf.ReleaseMulti(mb)
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				if !bytes.Equal(got, payload) {
					t.Fatalf("decoded=%d want=%d (bytes differ)", len(got), len(payload))
				}
			})
		}
	}
}

type streamBatchFailingWriter struct {
	calls  int
	failAt int
}

var errStreamBatchWrite = errors.New("stream batch test write failure")

func (w *streamBatchFailingWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == w.failAt {
		return 0, errStreamBatchWrite
	}
	return len(p), nil
}

func TestCoalescedStreamWriteError(t *testing.T) {
	for _, tc := range []struct {
		name   string
		count  int
		failAt int
	}{
		{"E1_FIRST_RECORD", 5, 1},
		{"E2_SECOND_RECORD", 5, 2},
		{"E3_SINGLE_BUFFER", 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := GetCipherMethod(MethodAES128GCM)
			if err != nil {
				t.Fatal(err)
			}
			key := make([]byte, m.KeySaltLength)
			if _, err := rand.Read(key); err != nil {
				t.Fatal(err)
			}
			enc, err := m.NewAEAD(key)
			if err != nil {
				t.Fatal(err)
			}
			// Pooled buffers, so a buffer that is not released keeps its storage.
			var mb buf.MultiBuffer
			for i := 0; i < tc.count; i++ {
				b := buf.New()
				b.Extend(buf.Size)
				mb = append(mb, b)
			}
			given := slices.Clone(mb)
			wire := &streamBatchFailingWriter{failAt: tc.failAt}
			if err := NewStreamWriter(wire, enc).WriteMultiBuffer(mb); !errors.Is(err, errStreamBatchWrite) {
				t.Fatalf("WriteMultiBuffer error=%v want %v", err, errStreamBatchWrite)
			}
			if wire.calls != tc.failAt {
				t.Fatalf("writes=%d want %d: nothing is written after the failed record", wire.calls, tc.failAt)
			}
			for i, b := range given {
				if b.Cap() != 0 {
					t.Fatalf("buffer %d not released after the failed write", i)
				}
			}
		})
	}
}
