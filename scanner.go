package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"

	"github.com/grandcat/zeroconf"
)

const serviceDiscoveryType = "_services._dns-sd._udp"

func scan(ctx context.Context, cfg scanConfig) ([]asset, error) {
	serviceTypes, err := discoverServiceTypes(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if len(serviceTypes) == 0 {
		return nil, nil
	}

	entries, err := browseAllServices(ctx, cfg, serviceTypes)
	if err != nil {
		return nil, err
	}

	return groupAssets(entries, cfg), nil
}

func discoverServiceTypes(ctx context.Context, cfg scanConfig) ([]string, error) {
	resolver, err := newResolver(cfg)
	if err != nil {
		return nil, err
	}

	browseCtx, cancel := context.WithTimeout(ctx, cfg.EnumTimeout)
	defer cancel()

	entries := make(chan *zeroconf.ServiceEntry)
	if err := resolver.Browse(browseCtx, serviceDiscoveryType, cfg.Domain, entries); err != nil {
		return nil, fmt.Errorf("browse service types: %w", err)
	}

	serviceSet := make(map[string]struct{})
	for entry := range entries {
		serviceType := normalizeDiscoveredServiceType(entry.Instance, cfg.Domain)
		if serviceType == "" {
			continue
		}
		serviceSet[serviceType] = struct{}{}
	}

	serviceTypes := make([]string, 0, len(serviceSet))
	for serviceType := range serviceSet {
		serviceTypes = append(serviceTypes, serviceType)
	}
	slices.Sort(serviceTypes)
	return serviceTypes, nil
}

func browseAllServices(ctx context.Context, cfg scanConfig, serviceTypes []string) ([]*zeroconf.ServiceEntry, error) {
	workers := 4
	if len(serviceTypes) < workers {
		workers = len(serviceTypes)
	}
	if workers == 0 {
		return nil, nil
	}

	type result struct {
		Entries []*zeroconf.ServiceEntry
		Err     error
	}

	jobs := make(chan string)
	results := make(chan result, len(serviceTypes))

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for serviceType := range jobs {
				entries, err := browseService(ctx, cfg, serviceType)
				results <- result{Entries: entries, Err: err}
			}
		}()
	}

	for _, serviceType := range serviceTypes {
		jobs <- serviceType
	}
	close(jobs)

	wg.Wait()
	close(results)

	seen := make(map[string]struct{})
	var out []*zeroconf.ServiceEntry
	var errs []error
	for result := range results {
		if result.Err != nil {
			errs = append(errs, result.Err)
			continue
		}
		for _, entry := range result.Entries {
			key := entryKey(entry)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, entry)
		}
	}

	if len(out) == 0 && len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return out, nil
}

func browseService(ctx context.Context, cfg scanConfig, serviceType string) ([]*zeroconf.ServiceEntry, error) {
	resolver, err := newResolver(cfg)
	if err != nil {
		return nil, err
	}

	browseCtx, cancel := context.WithTimeout(ctx, cfg.BrowseTimeout)
	defer cancel()

	entries := make(chan *zeroconf.ServiceEntry)
	if err := resolver.Browse(browseCtx, serviceType, cfg.Domain, entries); err != nil {
		return nil, fmt.Errorf("browse %s: %w", serviceType, err)
	}

	var out []*zeroconf.ServiceEntry
	for entry := range entries {
		if !entryMatchesCIDRs(entry, cfg.CIDRs) {
			continue
		}
		out = append(out, cloneEntry(entry))
	}
	return out, nil
}

func newResolver(cfg scanConfig) (*zeroconf.Resolver, error) {
	var options []zeroconf.ClientOption
	if len(cfg.Interfaces) > 0 {
		options = append(options, zeroconf.SelectIfaces(cfg.Interfaces))
	}
	resolver, err := zeroconf.NewResolver(options...)
	if err != nil {
		return nil, fmt.Errorf("new resolver: %w", err)
	}
	return resolver, nil
}

func groupAssets(entries []*zeroconf.ServiceEntry, cfg scanConfig) []asset {
	type mutableAsset struct {
		asset
		seenService map[string]struct{}
		seenPTR     map[string]struct{}
	}

	assetsByKey := make(map[string]*mutableAsset)
	for _, entry := range entries {
		key := assetKey(entry, cfg.CIDRs)
		if key == "" {
			continue
		}

		current, ok := assetsByKey[key]
		if !ok {
			current = &mutableAsset{
				asset: asset{
					Hostname: trimTrailingDot(entry.HostName),
					IPv4:     uniqueIPs(entry.AddrIPv4),
					IPv6:     uniqueIPs(entry.AddrIPv6),
				},
				seenService: make(map[string]struct{}),
				seenPTR:     make(map[string]struct{}),
			}
			assetsByKey[key] = current
		}

		current.Hostname = bestHostname(current.Hostname, trimTrailingDot(entry.HostName))
		current.IPv4 = mergeIPs(current.IPv4, entry.AddrIPv4)
		current.IPv6 = mergeIPs(current.IPv6, entry.AddrIPv6)

		svc := buildService(entry, cfg.Domain)
		serviceKey := fmt.Sprintf("%s|%s|%s|%d", svc.PTR, svc.Name, svc.Hostname, svc.Port)
		if _, exists := current.seenService[serviceKey]; !exists {
			current.seenService[serviceKey] = struct{}{}
			current.Services = append(current.Services, svc)
		}
		if svc.PTR != "" {
			current.seenPTR[svc.PTR] = struct{}{}
		}
	}

	out := make([]asset, 0, len(assetsByKey))
	for _, current := range assetsByKey {
		filtered := filterServicesByPort(current.Services, cfg.Ports)
		if len(filtered) == 0 {
			continue
		}
		current.Services = filtered
		current.PTR = current.PTR[:0]
		for _, svc := range current.Services {
			if svc.PTR != "" {
				current.PTR = appendIfMissing(current.PTR, svc.PTR)
			}
		}
		sortServices(current.Services)
		slices.Sort(current.PTR)
		out = append(out, current.asset)
	}

	slices.SortFunc(out, func(a, b asset) int {
		return strings.Compare(primaryAssetID(a, cfg.CIDRs), primaryAssetID(b, cfg.CIDRs))
	})
	return out
}

func filterServicesByPort(services []service, matcher portMatcher) []service {
	var matched []service
	var metadata []service
	for _, svc := range services {
		if svc.Port <= 0 {
			metadata = append(metadata, svc)
			continue
		}
		if matcher.Contains(svc.Port) {
			matched = append(matched, svc)
		}
	}
	if len(matched) == 0 {
		return nil
	}
	return append(matched, metadata...)
}

func sortServices(services []service) {
	slices.SortFunc(services, func(a, b service) int {
		switch {
		case a.Port <= 0 && b.Port > 0:
			return 1
		case a.Port > 0 && b.Port <= 0:
			return -1
		case a.Port != b.Port:
			return a.Port - b.Port
		case a.Label != b.Label:
			return strings.Compare(a.Label, b.Label)
		default:
			return strings.Compare(a.Name, b.Name)
		}
	})
}

func buildService(entry *zeroconf.ServiceEntry, domain string) service {
	return service{
		Name:     entry.Instance,
		Hostname: trimTrailingDot(entry.HostName),
		Port:     entry.Port,
		TTL:      entry.TTL,
		IPv4:     uniqueIPs(entry.AddrIPv4),
		IPv6:     uniqueIPs(entry.AddrIPv6),
		Service:  entry.Service,
		PTR:      servicePTR(entry.Service, domain),
		Label:    friendlyServiceLabel(entry.Service),
		Banner:   normalizeTXTRecords(entry.Text),
	}
}

func normalizeDiscoveredServiceType(raw, domain string) string {
	value := trimTrailingDot(strings.TrimSpace(raw))
	if value == "" {
		return ""
	}

	domainSuffix := "." + normalizeDomain(domain)
	if strings.HasSuffix(strings.ToLower(value), strings.ToLower(domainSuffix)) {
		value = value[:len(value)-len(domainSuffix)]
	}

	if strings.HasSuffix(value, "._tcp") || strings.HasSuffix(value, "._udp") {
		return value
	}
	return ""
}

func entryMatchesCIDRs(entry *zeroconf.ServiceEntry, cidrs []*net.IPNet) bool {
	if len(cidrs) == 0 {
		return true
	}
	for _, ip := range entry.AddrIPv4 {
		if ipInTargets(ip, cidrs) {
			return true
		}
	}
	for _, ip := range entry.AddrIPv6 {
		if ipInTargets(ip, cidrs) {
			return true
		}
	}
	return false
}

func ipInTargets(ip net.IP, cidrs []*net.IPNet) bool {
	for _, cidr := range cidrs {
		if sameIPFamily(ip, cidr.IP) && cidr.Contains(ip) {
			return true
		}
	}
	return false
}

func assetKey(entry *zeroconf.ServiceEntry, cidrs []*net.IPNet) string {
	hostname := trimTrailingDot(entry.HostName)
	if hostname != "" {
		return strings.ToLower(hostname)
	}

	if ip := firstMatchingIP(entry.AddrIPv4, cidrs); ip != nil {
		return ip.String()
	}
	if ip := firstMatchingIP(entry.AddrIPv6, cidrs); ip != nil {
		return ip.String()
	}
	if len(entry.AddrIPv4) > 0 {
		return entry.AddrIPv4[0].String()
	}
	if len(entry.AddrIPv6) > 0 {
		return entry.AddrIPv6[0].String()
	}
	return ""
}

func firstMatchingIP(ips []net.IP, cidrs []*net.IPNet) net.IP {
	for _, ip := range ips {
		if ipInTargets(ip, cidrs) {
			return ip
		}
	}
	if len(ips) > 0 {
		return ips[0]
	}
	return nil
}

func primaryAssetID(item asset, cidrs []*net.IPNet) string {
	if ip := firstMatchingIP(item.IPv4, cidrs); ip != nil {
		return ip.String()
	}
	if ip := firstMatchingIP(item.IPv6, cidrs); ip != nil {
		return ip.String()
	}
	if item.Hostname != "" {
		return item.Hostname
	}
	return ""
}

func entryKey(entry *zeroconf.ServiceEntry) string {
	parts := []string{
		entry.Service,
		entry.Instance,
		trimTrailingDot(entry.HostName),
		fmt.Sprintf("%d", entry.Port),
		strings.Join(stringifyIPs(entry.AddrIPv4), ","),
		strings.Join(stringifyIPs(entry.AddrIPv6), ","),
		strings.Join(normalizeTXTRecords(entry.Text), "|"),
	}
	return strings.Join(parts, "|")
}

func cloneEntry(entry *zeroconf.ServiceEntry) *zeroconf.ServiceEntry {
	cloned := *entry
	cloned.Text = append([]string(nil), entry.Text...)
	cloned.AddrIPv4 = append([]net.IP(nil), entry.AddrIPv4...)
	cloned.AddrIPv6 = append([]net.IP(nil), entry.AddrIPv6...)
	return &cloned
}

func bestHostname(current, candidate string) string {
	switch {
	case current == "":
		return candidate
	case strings.HasSuffix(strings.ToLower(current), ".local") && !strings.HasSuffix(strings.ToLower(candidate), ".local"):
		return current
	case strings.HasSuffix(strings.ToLower(candidate), ".local") && !strings.HasSuffix(strings.ToLower(current), ".local"):
		return candidate
	default:
		return current
	}
}

func servicePTR(serviceType, domain string) string {
	serviceType = trimTrailingDot(serviceType)
	if serviceType == "" {
		return ""
	}
	return serviceType + "." + normalizeDomain(domain)
}

func friendlyServiceLabel(serviceType string) string {
	serviceType = trimTrailingDot(serviceType)
	parts := strings.Split(serviceType, ".")
	if len(parts) > 0 {
		return strings.TrimPrefix(parts[0], "_")
	}
	return strings.TrimPrefix(serviceType, "_")
}

func normalizeTXTRecords(records []string) []string {
	skipKeys := map[string]struct{}{
		"txtv":    {},
		"txtvers": {},
	}

	seen := make(map[string]struct{})
	filtered := make([]string, 0, len(records))
	for _, record := range records {
		record = strings.TrimSpace(record)
		if record == "" {
			continue
		}
		if idx := strings.Index(record, "="); idx > 0 {
			key := strings.ToLower(record[:idx])
			if _, skip := skipKeys[key]; skip {
				continue
			}
		}
		if _, exists := seen[record]; exists {
			continue
		}
		seen[record] = struct{}{}
		filtered = append(filtered, record)
	}

	if len(filtered) <= 1 {
		return filtered
	}
	if allKeyValue(filtered) {
		return []string{strings.Join(filtered, ",")}
	}
	return filtered
}

func allKeyValue(records []string) bool {
	for _, record := range records {
		if !strings.Contains(record, "=") {
			return false
		}
	}
	return true
}

func mergeIPs(current, incoming []net.IP) []net.IP {
	return uniqueIPs(append(current, incoming...))
}

func uniqueIPs(ips []net.IP) []net.IP {
	seen := make(map[string]struct{}, len(ips))
	out := make([]net.IP, 0, len(ips))
	for _, ip := range ips {
		if ip == nil {
			continue
		}
		key := ip.String()
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, ip)
	}
	slices.SortFunc(out, func(a, b net.IP) int {
		return strings.Compare(a.String(), b.String())
	})
	return out
}

func stringifyIPs(ips []net.IP) []string {
	out := make([]string, 0, len(ips))
	for _, ip := range uniqueIPs(ips) {
		out = append(out, ip.String())
	}
	return out
}

func appendIfMissing(values []string, value string) []string {
	for _, current := range values {
		if current == value {
			return values
		}
	}
	return append(values, value)
}

func trimTrailingDot(value string) string {
	return strings.TrimSuffix(strings.TrimSpace(value), ".")
}
