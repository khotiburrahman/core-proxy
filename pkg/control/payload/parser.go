package payload

import (
	"strings"
)

type TokenType int

const (
	TokenLiteral TokenType = iota
	TokenCR
	TokenLF
	TokenCRLF
	TokenHost
	TokenPort
	TokenHostPort
	TokenProtocol
	TokenSplit
	TokenRaw
)

type Token struct {
	Type  TokenType
	Value string
}

type Template struct {
	Tokens []Token
}

func ParseTemplate(rawPattern string) (*Template, error) {
	tmpl := &Template{}
	var buf strings.Builder

	i := 0
	n := len(rawPattern)

	for i < n {
		if rawPattern[i] == '[' {
			end := strings.IndexByte(rawPattern[i:], ']')
			if end != -1 {
				if buf.Len() > 0 {
					tmpl.Tokens = append(tmpl.Tokens, Token{Type: TokenLiteral, Value: buf.String()})
					buf.Reset()
				}

				tag := strings.ToLower(rawPattern[i+1 : i+end])
				switch tag {
				case "cr":
					tmpl.Tokens = append(tmpl.Tokens, Token{Type: TokenCR, Value: "\r"})
				case "lf":
					tmpl.Tokens = append(tmpl.Tokens, Token{Type: TokenLF, Value: "\n"})
				case "crlf":
					tmpl.Tokens = append(tmpl.Tokens, Token{Type: TokenCRLF, Value: "\r\n"})
				case "host":
					tmpl.Tokens = append(tmpl.Tokens, Token{Type: TokenHost})
				case "port":
					tmpl.Tokens = append(tmpl.Tokens, Token{Type: TokenPort})
				case "host_port", "hostport":
					tmpl.Tokens = append(tmpl.Tokens, Token{Type: TokenHostPort})
				case "protocol":
					tmpl.Tokens = append(tmpl.Tokens, Token{Type: TokenProtocol})
				case "split":
					tmpl.Tokens = append(tmpl.Tokens, Token{Type: TokenSplit})
				case "raw":
					tmpl.Tokens = append(tmpl.Tokens, Token{Type: TokenRaw})
				default:
					buf.WriteString(rawPattern[i : i+end+1])
				}
				i += end + 1
				continue
			}
		}

		buf.WriteByte(rawPattern[i])
		i++
	}

	if buf.Len() > 0 {
		tmpl.Tokens = append(tmpl.Tokens, Token{Type: TokenLiteral, Value: buf.String()})
	}

	return tmpl, nil
}

