# mdnsmap-cli

`mdnsmap-cli` is a standalone Go CLI for local-network asset discovery over mDNS / DNS-SD.

It accepts:

- CIDR or IP targets
- Port ranges
- Optional interface selection

It outputs grouped assets with:

- IP addresses
- Hostname
- Port
- Service type
- TXT / banner details extracted from mDNS records

## Build

```bash
go build -o mdnsmap.exe .
```

## Example

```bash
go run . -cidr 192.168.1.0/24 -ports 1-10000
```

## Notes

- mDNS is link-local multicast. The target CIDR must be on a directly connected local network.
- The scanner enumerates `_services._dns-sd._udp.local`, then browses each discovered service type and expands TXT banners.
