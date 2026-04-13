package main

import (
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"
)

type portRange struct {
	Start int
	End   int
}

type portMatcher struct {
	Ranges []portRange
}

func (m portMatcher) Contains(port int) bool {
	if port <= 0 {
		return false
	}
	for _, rng := range m.Ranges {
		if port >= rng.Start && port <= rng.End {
			return true
		}
	}
	return false
}

type scanConfig struct {
	RawCIDR               string
	CIDRs                 []*net.IPNet
	Ports                 portMatcher
	Domain                string
	EnumTimeout           time.Duration
	BrowseTimeout         time.Duration
	Interfaces            []net.Interface
	InterfaceScopeMatched bool
}

func newScanConfig(rawCIDR, rawPorts, rawInterfaces, domain string, enumTimeout, browseTimeout time.Duration) (scanConfig, error) {
	if strings.TrimSpace(rawCIDR) == "" {
		return scanConfig{}, fmt.Errorf("cidr is required")
	}
	if enumTimeout <= 0 {
		return scanConfig{}, fmt.Errorf("enum-timeout must be greater than zero")
	}
	if browseTimeout <= 0 {
		return scanConfig{}, fmt.Errorf("browse-timeout must be greater than zero")
	}

	cidrs, err := parseCIDRTargets(rawCIDR)
	if err != nil {
		return scanConfig{}, err
	}
	ports, err := parsePortMatcher(rawPorts)
	if err != nil {
		return scanConfig{}, err
	}
	interfaces, matched, err := selectInterfaces(cidrs, rawInterfaces)
	if err != nil {
		return scanConfig{}, err
	}

	return scanConfig{
		RawCIDR:               rawCIDR,
		CIDRs:                 cidrs,
		Ports:                 ports,
		Domain:                normalizeDomain(domain),
		EnumTimeout:           enumTimeout,
		BrowseTimeout:         browseTimeout,
		Interfaces:            interfaces,
		InterfaceScopeMatched: matched,
	}, nil
}

func parseCIDRTargets(raw string) ([]*net.IPNet, error) {
	items := splitAndTrim(raw)
	if len(items) == 0 {
		return nil, fmt.Errorf("at least one cidr or ip must be provided")
	}

	var cidrs []*net.IPNet
	for _, item := range items {
		if ip := net.ParseIP(item); ip != nil {
			cidrs = append(cidrs, hostNet(ip))
			continue
		}

		_, network, err := net.ParseCIDR(item)
		if err != nil {
			return nil, fmt.Errorf("invalid cidr %q: %w", item, err)
		}
		cidrs = append(cidrs, network)
	}
	return cidrs, nil
}

func parsePortMatcher(raw string) (portMatcher, error) {
	items := splitAndTrim(raw)
	if len(items) == 0 {
		return portMatcher{}, fmt.Errorf("at least one port or port range must be provided")
	}

	var ranges []portRange
	for _, item := range items {
		if !strings.Contains(item, "-") {
			value, err := strconv.Atoi(item)
			if err != nil {
				return portMatcher{}, fmt.Errorf("invalid port %q: %w", item, err)
			}
			if value < 1 || value > 65535 {
				return portMatcher{}, fmt.Errorf("port %d out of range", value)
			}
			ranges = append(ranges, portRange{Start: value, End: value})
			continue
		}

		parts := strings.SplitN(item, "-", 2)
		if len(parts) != 2 {
			return portMatcher{}, fmt.Errorf("invalid port range %q", item)
		}
		start, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		if err != nil {
			return portMatcher{}, fmt.Errorf("invalid port range start %q: %w", item, err)
		}
		end, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			return portMatcher{}, fmt.Errorf("invalid port range end %q: %w", item, err)
		}
		if start < 1 || end < 1 || start > 65535 || end > 65535 || start > end {
			return portMatcher{}, fmt.Errorf("invalid port range %q", item)
		}
		ranges = append(ranges, portRange{Start: start, End: end})
	}

	slices.SortFunc(ranges, func(a, b portRange) int {
		if a.Start != b.Start {
			return a.Start - b.Start
		}
		return a.End - b.End
	})

	return portMatcher{Ranges: ranges}, nil
}

func selectInterfaces(cidrs []*net.IPNet, rawInterfaces string) ([]net.Interface, bool, error) {
	all, err := net.Interfaces()
	if err != nil {
		return nil, false, fmt.Errorf("list interfaces: %w", err)
	}

	ifacesByName := make(map[string]net.Interface, len(all))
	for _, iface := range all {
		ifacesByName[iface.Name] = iface
	}

	if requested := splitAndTrim(rawInterfaces); len(requested) > 0 {
		selected := make([]net.Interface, 0, len(requested))
		for _, name := range requested {
			iface, ok := ifacesByName[name]
			if !ok {
				return nil, false, fmt.Errorf("interface %q not found", name)
			}
			if !supportsMDNS(iface) {
				return nil, false, fmt.Errorf("interface %q is not up or does not support multicast", name)
			}
			selected = append(selected, iface)
		}
		return selected, true, nil
	}

	var candidates []net.Interface
	var scoped []net.Interface
	for _, iface := range all {
		if !supportsMDNS(iface) {
			continue
		}
		candidates = append(candidates, iface)
		if interfaceOverlapsTargets(iface, cidrs) {
			scoped = append(scoped, iface)
		}
	}

	if len(scoped) > 0 {
		return scoped, true, nil
	}
	if len(candidates) == 0 {
		return nil, false, fmt.Errorf("no multicast-capable interfaces found")
	}
	return candidates, false, nil
}

func supportsMDNS(iface net.Interface) bool {
	return iface.Flags&net.FlagUp != 0 && iface.Flags&net.FlagMulticast != 0
}

func interfaceOverlapsTargets(iface net.Interface, cidrs []*net.IPNet) bool {
	if len(cidrs) == 0 {
		return true
	}

	addrs, err := iface.Addrs()
	if err != nil {
		return false
	}

	for _, addr := range addrs {
		var ifaceNet *net.IPNet
		switch value := addr.(type) {
		case *net.IPNet:
			ifaceNet = value
		case *net.IPAddr:
			ifaceNet = hostNet(value.IP)
		default:
			continue
		}

		for _, target := range cidrs {
			if netsOverlap(ifaceNet, target) {
				return true
			}
		}
	}
	return false
}

func netsOverlap(a, b *net.IPNet) bool {
	if a == nil || b == nil {
		return false
	}
	if sameIPFamily(a.IP, b.IP) && (a.Contains(b.IP) || b.Contains(a.IP)) {
		return true
	}
	return false
}

func sameIPFamily(a, b net.IP) bool {
	return (a.To4() != nil) == (b.To4() != nil)
}

func normalizeDomain(domain string) string {
	domain = strings.TrimSpace(domain)
	if domain == "" {
		return "local"
	}
	return strings.TrimSuffix(domain, ".")
}

func hostNet(ip net.IP) *net.IPNet {
	bits := 128
	if ip.To4() != nil {
		ip = ip.To4()
		bits = 32
	}
	return &net.IPNet{
		IP:   ip,
		Mask: net.CIDRMask(bits, bits),
	}
}

func splitAndTrim(raw string) []string {
	items := strings.Split(raw, ",")
	out := make([]string, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		out = append(out, item)
	}
	return out
}
