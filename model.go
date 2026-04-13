package main

import "net"

type service struct {
	Name     string
	Hostname string
	Port     int
	TTL      uint32
	IPv4     []net.IP
	IPv6     []net.IP
	Service  string
	PTR      string
	Label    string
	Banner   []string
}

type asset struct {
	Hostname string
	IPv4     []net.IP
	IPv6     []net.IP
	Services []service
	PTR      []string
}
