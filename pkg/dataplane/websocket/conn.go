package websocket

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA
)

// Conn adalah net.Conn wrapper yang melakukan WebSocket framing di atas koneksi TCP/TLS.
type Conn struct {
	conn    net.Conn
	br      *bufio.Reader
	writeMu sync.Mutex
	readBuf []byte
	readPos int
}

// DialWS melakukan HTTP Upgrade ke WebSocket.
// raw       : koneksi TCP mentah ke proxy
// hostHeader: nilai Host: header (biasanya target SSH host)
// path      : path request, mis. "/"
// useTLS    : apakah bungkus TLS dulu (true untuk port 443)
// sni       : ServerName untuk TLS handshake
func DialWS(raw net.Conn, hostHeader, path string, useTLS bool, sni string, timeout time.Duration) (*Conn, error) {
	var underlying net.Conn = raw

	if useTLS {
		if sni == "" {
			sni = hostHeader
		}
		tlsCfg := &tls.Config{
			ServerName:         sni,
			InsecureSkipVerify: true,
		}
		tlsConn := tls.Client(raw, tlsCfg)
		_ = tlsConn.SetDeadline(time.Now().Add(timeout))
		if err := tlsConn.Handshake(); err != nil {
			raw.Close()
			return nil, fmt.Errorf("tls handshake failed: %w", err)
		}
		_ = tlsConn.SetDeadline(time.Time{})
		underlying = tlsConn
	}

	// Sec-WebSocket-Key acak
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		underlying.Close()
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)

	if path == "" {
		path = "/"
	}

	req := fmt.Sprintf(
		"GET %s HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"Upgrade: websocket\r\n"+
			"Connection: Upgrade\r\n"+
			"Sec-WebSocket-Key: %s\r\n"+
			"Sec-WebSocket-Version: 13\r\n"+
			"\r\n",
		path, hostHeader, key,
	)

	_ = underlying.SetWriteDeadline(time.Now().Add(timeout))
	if _, err := underlying.Write([]byte(req)); err != nil {
		underlying.Close()
		return nil, fmt.Errorf("failed sending upgrade request: %w", err)
	}
	_ = underlying.SetWriteDeadline(time.Time{})

	// Baca response upgrade
	_ = underlying.SetReadDeadline(time.Now().Add(timeout))
	br := bufio.NewReader(underlying)

	statusLine, err := br.ReadString('\n')
	if err != nil {
		underlying.Close()
		return nil, fmt.Errorf("failed reading status line: %w", err)
	}
	statusLine = strings.TrimRight(statusLine, "\r\n")
	if !strings.Contains(statusLine, " 101 ") {
		underlying.Close()
		return nil, fmt.Errorf("websocket upgrade failed: %q", statusLine)
	}

	headers := make(map[string]string)
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			underlying.Close()
			return nil, err
		}
		if line == "\r\n" || line == "\n" {
			break
		}
		parts := strings.SplitN(strings.TrimRight(line, "\r\n"), ":", 2)
		if len(parts) == 2 {
			headers[strings.ToLower(strings.TrimSpace(parts[0]))] = strings.TrimSpace(parts[1])
		}
	}

	expected := computeAccept(key)
	if got := headers["sec-websocket-accept"]; got != "" && got != expected {
		underlying.Close()
		return nil, fmt.Errorf("invalid Sec-WebSocket-Accept: %q", got)
	}

	_ = underlying.SetReadDeadline(time.Time{})

	return &Conn{
		conn: underlying,
		br:   br,
	}, nil
}

func computeAccept(key string) string {
	h := sha1.New()
	h.Write([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func (c *Conn) Read(p []byte) (int, error) {
	for {
		if c.readPos < len(c.readBuf) {
			n := copy(p, c.readBuf[c.readPos:])
			c.readPos += n
			if c.readPos >= len(c.readBuf) {
				c.readBuf = nil
				c.readPos = 0
			}
			return n, nil
		}

		op, payload, err := c.readFrame()
		if err != nil {
			return 0, err
		}

		switch op {
		case opBinary, opContinuation, opText:
			if len(payload) == 0 {
				continue
			}
			n := copy(p, payload)
			if n < len(payload) {
				c.readBuf = payload
				c.readPos = n
			}
			return n, nil
		case opPing:
			_ = c.writeFrame(opPong, payload)
			continue
		case opPong:
			continue
		case opClose:
			return 0, io.EOF
		default:
			continue
		}
	}
}

func (c *Conn) readFrame() (byte, []byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(c.br, header); err != nil {
		return 0, nil, err
	}

	opcode := header[0] & 0x0F
	masked := (header[1] & 0x80) != 0
	payloadLen := int64(header[1] & 0x7F)

	if payloadLen == 126 {
		ext := make([]byte, 2)
		if _, err := io.ReadFull(c.br, ext); err != nil {
			return 0, nil, err
		}
		payloadLen = int64(binary.BigEndian.Uint16(ext))
	} else if payloadLen == 127 {
		ext := make([]byte, 8)
		if _, err := io.ReadFull(c.br, ext); err != nil {
			return 0, nil, err
		}
		payloadLen = int64(binary.BigEndian.Uint64(ext))
	}

	var maskKey [4]byte
	if masked {
		if _, err := io.ReadFull(c.br, maskKey[:]); err != nil {
			return 0, nil, err
		}
	}

	payload := make([]byte, payloadLen)
	if payloadLen > 0 {
		if _, err := io.ReadFull(c.br, payload); err != nil {
			return 0, nil, err
		}
	}

	if masked {
		for i := range payload {
			payload[i] ^= maskKey[i%4]
		}
	}

	return opcode, payload, nil
}

func (c *Conn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := c.writeFrame(opBinary, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *Conn) writeFrame(opcode byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	length := len(payload)
	header := make([]byte, 0, 14)
	header = append(header, 0x80|opcode)

	const maskBit = byte(0x80)

	switch {
	case length < 126:
		header = append(header, maskBit|byte(length))
	case length < 65536:
		header = append(header, maskBit|126)
		ext := make([]byte, 2)
		binary.BigEndian.PutUint16(ext, uint16(length))
		header = append(header, ext...)
	default:
		header = append(header, maskBit|127)
		ext := make([]byte, 8)
		binary.BigEndian.PutUint64(ext, uint64(length))
		header = append(header, ext...)
	}

	var maskKey [4]byte
	if _, err := rand.Read(maskKey[:]); err != nil {
		return err
	}
	header = append(header, maskKey[:]...)

	masked := make([]byte, length)
	for i := 0; i < length; i++ {
		masked[i] = payload[i] ^ maskKey[i%4]
	}

	if _, err := c.conn.Write(header); err != nil {
		return err
	}
	if length > 0 {
		if _, err := c.conn.Write(masked); err != nil {
			return err
		}
	}
	return nil
}

func (c *Conn) Close() error {
	_ = c.writeFrame(opClose, []byte{0x03, 0xE8})
	return c.conn.Close()
}

func (c *Conn) LocalAddr() net.Addr                { return c.conn.LocalAddr() }
func (c *Conn) RemoteAddr() net.Addr               { return c.conn.RemoteAddr() }
func (c *Conn) SetDeadline(t time.Time) error      { return c.conn.SetDeadline(t) }
func (c *Conn) SetReadDeadline(t time.Time) error  { return c.conn.SetReadDeadline(t) }
func (c *Conn) SetWriteDeadline(t time.Time) error { return c.conn.SetWriteDeadline(t) }

func WrapConn(conn net.Conn, br *bufio.Reader) *Conn {
	return &Conn{
		conn: conn,
		br:   br,
	}
}