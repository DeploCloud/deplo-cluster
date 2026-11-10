package relay

import (
	"fmt"
	"net"
	"net/netip"
)

// ValidateListen refuses an address anyone outside the machine could reach: the relay sits behind the proxy, never on a public port.
func ValidateListen(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%q is not host:port", addr)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return fmt.Errorf("%q is not an address, only an address of this machine's own networks is allowed", host)
	}
	if ip.IsUnspecified() || !(ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast()) {
		return fmt.Errorf("the relay does not listen on %s: it only answers the proxy beside it", host)
	}
	return nil
}
