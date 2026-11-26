package freedom

import (
	"testing"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/session"
)

type fillingGateReader struct{}

func (fillingGateReader) Read(p []byte) (int, error) { return len(p), nil }

func TestTCPResponseReaderGate(t *testing.T) {
	for _, name := range []string{"mieru", "naive", "sudoku", "shadowsocks-2022-multi", "vless", "vmess", "trojan", "shadowsocks-2022", "shadowsocks-2022-relay", "", "nil"} {
		t.Run(name, func(t *testing.T) {
			inbound := &session.Inbound{Name: name}
			if name == "nil" {
				inbound = nil
			}
			r := tcpResponseReader(inbound)(fillingGateReader{})
			for i := 0; i < 3; i++ {
				mb, err := r.ReadMultiBuffer()
				want := int32(buf.Size)
				if i > 0 && (name == "mieru" || name == "naive" || name == "sudoku" || name == "shadowsocks-2022-multi") {
					want = 32768
				}
				if err != nil || len(mb) == 0 {
					t.Fatalf("call %d: buffers = %d, error = %v", i, len(mb), err)
				}
				for _, b := range mb {
					if b.Len() != want {
						t.Errorf("call %d: element = %d, want %d", i, b.Len(), want)
					}
				}
				buf.ReleaseMulti(mb)
			}
		})
	}
}
