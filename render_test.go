package main

import (
	"bytes"
	"net"
	"strings"
	"testing"
)

func TestRenderText(t *testing.T) {
	assets := []asset{
		{
			Hostname: "slw-nas.local",
			IPv4:     []net.IP{net.ParseIP("192.168.1.20")},
			IPv6:     []net.IP{net.ParseIP("fe80::1")},
			Services: []service{
				{
					Name:     "slw-nas",
					Hostname: "slw-nas.local",
					Port:     5000,
					TTL:      10,
					IPv4:     []net.IP{net.ParseIP("192.168.1.20")},
					IPv6:     []net.IP{net.ParseIP("fe80::1")},
					Service:  "_http._tcp",
					PTR:      "_http._tcp.local",
					Label:    "http",
					Banner:   []string{"path=/"},
				},
				{
					Name:     "slw-nas",
					Hostname: "slw-nas.local",
					Port:     5000,
					TTL:      10,
					IPv4:     []net.IP{net.ParseIP("192.168.1.20")},
					IPv6:     []net.IP{net.ParseIP("fe80::1")},
					Service:  "_qdiscover._tcp",
					PTR:      "_qdiscover._tcp.local",
					Label:    "qdiscover",
					Banner:   []string{"accessType=https,accessPort=86,model=TS-X64,displayModel=TS-464C,fwVer=5.2.9,fwBuildNum=20260214"},
				},
			},
			PTR: []string{"_http._tcp.local", "_qdiscover._tcp.local"},
		},
	}

	var buf bytes.Buffer
	if err := renderText(&buf, assets, []*net.IPNet{{IP: net.ParseIP("192.168.1.0").To4(), Mask: net.CIDRMask(24, 32)}}); err != nil {
		t.Fatalf("renderText returned error: %v", err)
	}

	out := buf.String()
	for _, want := range []string{
		"asset: 192.168.1.20",
		"services:",
		"5000/tcp http:",
		"Hostname=slw-nas.local",
		"path=/",
		"5000/tcp qdiscover:",
		"accessType=https,accessPort=86,model=TS-X64,displayModel=TS-464C,fwVer=5.2.9,fwBuildNum=20260214",
		"answers:",
		"PTR:",
		"_http._tcp.local",
		"_qdiscover._tcp.local",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}
