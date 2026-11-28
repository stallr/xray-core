//go:build unix

package freedom

import (
	"fmt"
	stdnet "net"
	"testing"

	"github.com/xtls/xray-core/common/session"
)

// TestTCPResponseReaderSelection gives tcpResponseReader a real loopback TCP
// conn, which exposes syscall.Conn, so the selected reader type is
// observable: mieru and naive take the pooled reader, sudoku and
// shadowsocks-2022-multi keep the adaptive reader, and every other inbound
// gets buf.NewReader's reader.
func TestTCPResponseReaderSelection(t *testing.T) {
	l, err := stdnet.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	c, err := stdnet.Dial("tcp4", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	s, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, tc := range []struct{ name, want string }{
		{"mieru", "*buf.pooledReader"},
		{"naive", "*buf.pooledReader"},
		{"sudoku", "*buf.adaptiveReader"},
		{"shadowsocks-2022-multi", "*buf.adaptiveReader"},
	} {
		if got := fmt.Sprintf("%T", tcpResponseReader(&session.Inbound{Name: tc.name})(c)); got != tc.want {
			t.Errorf("%s: reader %s, want %s", tc.name, got, tc.want)
		}
	}
	for _, name := range []string{"vless", "vmess", "trojan", "shadowsocks-2022", ""} {
		if got := fmt.Sprintf("%T", tcpResponseReader(&session.Inbound{Name: name})(c)); got == "*buf.pooledReader" || got == "*buf.adaptiveReader" {
			t.Errorf("%q: reader %s, want buf.NewReader's reader", name, got)
		}
	}
	if got := fmt.Sprintf("%T", tcpResponseReader(nil)(c)); got == "*buf.pooledReader" || got == "*buf.adaptiveReader" {
		t.Errorf("nil inbound: reader %s, want buf.NewReader's reader", got)
	}
}
