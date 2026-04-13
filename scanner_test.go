package main

import "testing"

func TestNormalizeDiscoveredServiceType(t *testing.T) {
	got := normalizeDiscoveredServiceType("_http._tcp.local.", "local")
	if got != "_http._tcp" {
		t.Fatalf("unexpected service type: %q", got)
	}
}
