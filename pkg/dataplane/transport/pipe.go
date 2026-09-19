package transport

import (
	"net"
	"sync"
	"time"

	"core-proxy/pkg/common/observability"
)

type PipeOption struct {
	IdleTimeout time.Duration
}

func Relay(client, remote net.Conn, opt PipeOption) (int64, int64) {
	var writtenClient, writtenRemote int64
	var wg sync.WaitGroup

	copyStream := func(dst, src net.Conn, bytesWritten *int64) {
		defer wg.Done()
		buf := make([]byte, 32*1024)
		for {
			if opt.IdleTimeout > 0 {
				_ = src.SetReadDeadline(time.Now().Add(opt.IdleTimeout))
			}

			nr, rerr := src.Read(buf)
			if nr > 0 {
				if opt.IdleTimeout > 0 {
					_ = dst.SetWriteDeadline(time.Now().Add(opt.IdleTimeout))
				}
				nw, werr := dst.Write(buf[:nr])
				if nw > 0 {
					*bytesWritten += int64(nw)
				}
				if werr != nil {
					break
				}
			}

			if rerr != nil {
				break
			}
		}

		if tcpConn, ok := dst.(*net.TCPConn); ok {
			_ = tcpConn.CloseWrite()
		}
	}

	wg.Add(2)
	go copyStream(client, remote, &writtenClient)
	go copyStream(remote, client, &writtenRemote)

	wg.Wait()

	_ = client.Close()
	_ = remote.Close()

	observability.Debug("Transport pipe closed", "bytes_sent", writtenRemote, "bytes_received", writtenClient)
	return writtenClient, writtenRemote
}

