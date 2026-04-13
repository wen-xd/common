package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	var (
		rawCIDR       string
		rawPorts      string
		rawInterfaces string
		domain        string
		enumTimeout   time.Duration
		browseTimeout time.Duration
	)

	flag.StringVar(&rawCIDR, "cidr", "", "Target CIDR/IP list, for example 192.168.1.0/24 or 192.168.1.10,192.168.1.0/24")
	flag.StringVar(&rawPorts, "ports", "1-65535", "Target ports, for example 80,443,5000-6000")
	flag.StringVar(&rawInterfaces, "ifaces", "", "Optional interface names, comma separated")
	flag.StringVar(&domain, "domain", "local", "mDNS domain")
	flag.DurationVar(&enumTimeout, "enum-timeout", 2*time.Second, "Timeout for DNS-SD service type enumeration")
	flag.DurationVar(&browseTimeout, "browse-timeout", 3*time.Second, "Timeout for browsing each service type")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s -cidr 192.168.1.0/24 -ports 1-10000\n", os.Args[0])
		fmt.Fprintln(flag.CommandLine.Output(), "Discovers mDNS assets in directly connected local networks and prints service banners.")
		flag.PrintDefaults()
	}
	flag.Parse()

	cfg, err := newScanConfig(rawCIDR, rawPorts, rawInterfaces, domain, enumTimeout, browseTimeout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}

	if !cfg.InterfaceScopeMatched {
		fmt.Fprintf(os.Stderr, "warning: no local multicast interface overlaps %q, using all multicast interfaces instead\n", cfg.RawCIDR)
	}

	assets, err := scan(context.Background(), cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "scan failed:", err)
		os.Exit(1)
	}

	if len(assets) == 0 {
		fmt.Fprintln(os.Stdout, "no matching mDNS assets found")
		return
	}

	if err := renderText(os.Stdout, assets, cfg.CIDRs); err != nil {
		fmt.Fprintln(os.Stderr, "render failed:", err)
		os.Exit(1)
	}
}
