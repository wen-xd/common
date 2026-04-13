package main

import (
	"net"
	"slices"
	"testing"
)

func TestParsePortMatcher(t *testing.T) {
	matcher, err := parsePortMatcher("80,443,5000-5002")
	if err != nil {
		t.Fatalf("parsePortMatcher returned error: %v", err)
	}

	for _, port := range []int{80, 443, 5000, 5001, 5002} {
		if !matcher.Contains(port) {
			t.Fatalf("expected port %d to match", port)
		}
	}
	for _, port := range []int{79, 444, 5003} {
		if matcher.Contains(port) {
			t.Fatalf("expected port %d to be excluded", port)
		}
	}
}

func TestParseCIDRTargetsAcceptsIPAndCIDR(t *testing.T) {
	cidrs, err := parseCIDRTargets("192.168.1.0/24,192.168.1.10,fe80::1")
	if err != nil {
		t.Fatalf("parseCIDRTargets returned error: %v", err)
	}
	if len(cidrs) != 3 {
		t.Fatalf("expected 3 targets, got %d", len(cidrs))
	}

	if !cidrs[0].Contains(net.ParseIP("192.168.1.10")) {
		t.Fatal("expected CIDR to include 192.168.1.10")
	}
	if !cidrs[1].Contains(net.ParseIP("192.168.1.10")) {
		t.Fatal("expected host target to include 192.168.1.10")
	}
	if !cidrs[2].Contains(net.ParseIP("fe80::1")) {
		t.Fatal("expected IPv6 host target to include fe80::1")
	}
}

func TestNormalizeTXTRecords(t *testing.T) {
	got := normalizeTXTRecords([]string{
		"txtvers=1",
		"accessType=https",
		"accessPort=86",
		"model=TS-X64",
	})
	want := []string{"accessType=https,accessPort=86,model=TS-X64"}
	if !slices.Equal(got, want) {
		t.Fatalf("unexpected banner lines: got %v want %v", got, want)
	}
}
