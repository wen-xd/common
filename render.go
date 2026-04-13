package main

import (
	"fmt"
	"io"
	"net"
	"strings"
)

func renderText(w io.Writer, assets []asset, cidrs []*net.IPNet) error {
	for i, item := range assets {
		if i > 0 {
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
		}

		if _, err := fmt.Fprintf(w, "asset: %s\n", primaryAssetID(item, cidrs)); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "services:"); err != nil {
			return err
		}

		for _, svc := range item.Services {
			header := svc.Label
			if header == "" {
				header = svc.Service
			}
			if svc.Port > 0 {
				if _, err := fmt.Fprintf(w, "%d/tcp %s:\n", svc.Port, header); err != nil {
					return err
				}
			} else {
				if _, err := fmt.Fprintf(w, "%s:\n", header); err != nil {
					return err
				}
			}

			if _, err := fmt.Fprintf(w, "Name=%s\n", svc.Name); err != nil {
				return err
			}
			if len(svc.IPv4) > 0 {
				if _, err := fmt.Fprintf(w, "IPv4=%s\n", strings.Join(stringifyIPs(svc.IPv4), ",")); err != nil {
					return err
				}
			}
			if len(svc.IPv6) > 0 {
				if _, err := fmt.Fprintf(w, "IPv6=%s\n", strings.Join(stringifyIPs(svc.IPv6), ",")); err != nil {
					return err
				}
			}
			if svc.Hostname != "" {
				if _, err := fmt.Fprintf(w, "Hostname=%s\n", svc.Hostname); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintf(w, "TTL=%d\n", svc.TTL); err != nil {
				return err
			}
			for _, banner := range svc.Banner {
				if _, err := fmt.Fprintln(w, banner); err != nil {
					return err
				}
			}
		}

		if _, err := fmt.Fprintln(w, "answers:"); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(w, "PTR:"); err != nil {
			return err
		}
		for _, ptr := range item.PTR {
			if _, err := fmt.Fprintln(w, ptr); err != nil {
				return err
			}
		}
	}
	return nil
}
