package freedom

import (
	"testing"

	"github.com/xtls/xray-core/common/geodata"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
)

func TestNodeInboundDefaultBlocksPrivateTCPAndUDP(t *testing.T) {
	h := new(Handler)
	for _, name := range []string{"mieru", "naive", "sudoku", "tuic", "anytls"} {
		for _, network := range []net.Network{net.Network_TCP, net.Network_UDP} {
			for _, address := range []string{"169.254.169.254", "127.0.0.1", "10.0.0.1"} {
				t.Run(name+"/"+network.String()+"/"+address, func(t *testing.T) {
					rule := h.matchFinalRule(network, net.ParseAddress(address), 443, getDefaultFinalRule(&session.Inbound{Name: name}))
					if rule == nil || rule.action != RuleAction_Block {
						t.Fatal("default must block private address")
					}
				})
			}
		}
	}
}

func TestDefaultRulePreservesExistingResults(t *testing.T) {
	h := new(Handler)
	for _, name := range []string{"", "socks"} {
		if rule := getDefaultFinalRule(&session.Inbound{Name: name}); rule != nil {
			t.Errorf("%q: want nil default, got %v", name, rule)
		}
	}
	if rule := getDefaultFinalRule(&session.Inbound{Name: "vless-reverse"}); rule != defaultBlockAllRule {
		t.Fatalf("vless-reverse: want block-all, got %v", rule)
	}
	for _, network := range []net.Network{net.Network_TCP, net.Network_UDP} {
		for _, address := range []string{"169.254.169.254", "127.0.0.1", "10.0.0.1", "1.1.1.1"} {
			rule := h.matchFinalRule(network, net.ParseAddress(address), 443, getDefaultFinalRule(&session.Inbound{Name: "vless"}))
			blocked := rule != nil && rule.action == RuleAction_Block
			if blocked != (address != "1.1.1.1") {
				t.Errorf("vless/%s/%s: blocked=%v", network, address, blocked)
			}
		}
	}
}

func TestMieruExplicitAllowPrecedesPrivateDefault(t *testing.T) {
	ips, err := geodata.ParseIPRules([]string{"10.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	h := new(Handler)
	if err := h.Init(&Config{FinalRules: []*FinalRuleConfig{{Action: RuleAction_Allow, Ip: ips}}}, nil); err != nil {
		t.Fatal(err)
	}
	for _, network := range []net.Network{net.Network_TCP, net.Network_UDP} {
		for _, address := range []string{"10.0.0.1", "169.254.169.254"} {
			t.Run(network.String()+"/"+address, func(t *testing.T) {
				want := RuleAction_Block
				if address == "10.0.0.1" {
					want = RuleAction_Allow
				}
				rule := h.matchFinalRule(network, net.ParseAddress(address), 443, getDefaultFinalRule(&session.Inbound{Name: "mieru"}))
				if rule == nil || rule.action != want {
					t.Fatalf("want explicit allow or default block action %v, got %v", want, rule)
				}
			})
		}
	}
}
