package payload

import (
	"fmt"
	"net"
	"strings"
)

type TargetInfo struct {
	Host     string
	Port     string
	Protocol string
}

func ExtractTargetInfo(targetAddr string) (TargetInfo, error) {
	host, port, err := net.SplitHostPort(targetAddr)
	if err != nil {
		return TargetInfo{}, fmt.Errorf("invalid target address: %w", err)
	}

	return TargetInfo{
		Host:     host,
		Port:     port,
		Protocol: "HTTP/1.1",
	}, nil
}

func (t *Template) BuildSegments(target TargetInfo) ([][]byte, error) {
	var segments [][]byte
	var currentSeg strings.Builder

	for _, token := range t.Tokens {
		switch token.Type {
		case TokenLiteral, TokenCR, TokenLF, TokenCRLF:
			currentSeg.WriteString(token.Value)
		case TokenHost:
			currentSeg.WriteString(target.Host)
		case TokenPort:
			currentSeg.WriteString(target.Port)
		case TokenHostPort:
			currentSeg.WriteString(fmt.Sprintf("%s:%s", target.Host, target.Port))
		case TokenProtocol:
			currentSeg.WriteString(target.Protocol)
		case TokenRaw:
			currentSeg.WriteString(fmt.Sprintf("CONNECT %s:%s HTTP/1.1\r\n\r\n", target.Host, target.Port))
		case TokenSplit:
			if currentSeg.Len() > 0 {
				segments = append(segments, []byte(currentSeg.String()))
				currentSeg.Reset()
			}
		}
	}

	if currentSeg.Len() > 0 {
		segments = append(segments, []byte(currentSeg.String()))
	}

	return segments, nil
}

