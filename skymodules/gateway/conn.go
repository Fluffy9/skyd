package gateway

import (
	"net"
	"time"

	"gitlab.com/NebulousLabs/monitor"

	"gitlab.com/skynetlabs/skyd/build"
	"gitlab.com/skynetlabs/skyd/skymodules"
)

// peerConn is a simple type that implements the skymodules.PeerConn interface.
type peerConn struct {
	net.Conn
	dialbackAddr skymodules.NetAddress
}

// RPCAddr implements the RPCAddr method of the skymodules.PeerConn interface. It
// is the address that identifies a peer.
func (pc peerConn) RPCAddr() skymodules.NetAddress {
	return pc.dialbackAddr
}

// newLocalAddr creates a TCPAddr to be used as the LocalAddr for the Dialer
func newLocalAddr(addr skymodules.NetAddress) *net.TCPAddr {
	ip := net.ParseIP(addr.Host())
	return &net.TCPAddr{
		IP:   ip,
		Zone: "",
	}
}

// staticDial will staticDial the input address and return a connection.
// staticDial appropriately handles things like clean shutdown, fast shutdown,
// and chooses the correct communication protocol.
func (g *Gateway) staticDial(addr skymodules.NetAddress) (net.Conn, error) {
	dialer := &net.Dialer{
		Cancel:  g.threads.StopChan(),
		Timeout: dialTimeout,
	}
	// For testing set the local address to the gateway address. This is to
	// prevent all the test nodes from having the same address
	if build.Release == "testing" {
		dialer.LocalAddr = newLocalAddr(g.myAddr)
	}

	conn, err := dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(connStdDeadline))

	// Monitor the conn bandwidth
	conn = connmonitor.NewMonitoredConn(conn, g.m)
	return conn, nil
}