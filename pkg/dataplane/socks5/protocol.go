package socks5

import (
	"fmt"
	"io"
	"net"
)

const (
	VersionSOCKS5 uint8 = 0x05

	AuthMethodNoAuth       uint8 = 0x00
	AuthMethodUserPass     uint8 = 0x02
	AuthMethodNoAcceptable uint8 = 0xFF

	CmdConnect uint8 = 0x01
	CmdBind    uint8 = 0x02
	CmdUDP     uint8 = 0x03

	AddrTypeIPv4   uint8 = 0x01
	AddrTypeDomain uint8 = 0x03
	AddrTypeIPv6   uint8 = 0x04

	ReplySuccess               uint8 = 0x00
	ReplyGeneralFailure        uint8 = 0x01
	ReplyConnectionNotAllowed  uint8 = 0x02
	ReplyNetworkUnreachable    uint8 = 0x03
	ReplyHostUnreachable       uint8 = 0x04
	ReplyConnectionRefused     uint8 = 0x05
	ReplyTTLExpired            uint8 = 0x06
	ReplyCommandNotSupported   uint8 = 0x07
	ReplyAddressTypeNotSupport uint8 = 0x08
)

type Request struct {
	Version  uint8
	Command  uint8
	AddrType uint8
	DestAddr string
	DestPort uint16
}

func (r *Request) Address() string {
	return fmt.Sprintf("%s:%d", r.DestAddr, r.DestPort)
}

func SendReply(w io.Writer, rep uint8, bindAddr net.Addr) error {
	var addrType uint8 = AddrTypeIPv4
	var ip net.IP = net.IPv4zero
	var port uint16 = 0

	if bindAddr != nil {
		if tcpAddr, ok := bindAddr.(*net.TCPAddr); ok {
			ip = tcpAddr.IP
			port = uint16(tcpAddr.Port)
			if ip.To4() == nil {
				addrType = AddrTypeIPv6
			}
		}
	}

	reply := []byte{VersionSOCKS5, rep, 0x00, addrType}

	if addrType == AddrTypeIPv4 {
		reply = append(reply, ip.To4()...)
	} else {
		reply = append(reply, ip.To16()...)
	}

	reply = append(reply, byte(port>>8), byte(port&0xff))

	_, err := w.Write(reply)
	return err
}

