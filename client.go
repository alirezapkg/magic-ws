package magicws

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/gobwas/ws"
)

type Client struct {
	conn      net.Conn
	sendChan  chan []byte
	onMessage func(data []byte)
	onClose   func()
	isClosed  bool
	mu        sync.RWMutex
}

func NewClient() *Client {
	return &Client{
		sendChan: make(chan []byte, 256),
	}
}

func (c *Client) OnMessage(fn func(data []byte)) {
	c.onMessage = fn
}

func (c *Client) OnClose(fn func()) {
	c.onClose = fn
}

func (c *Client) Connect(ctx context.Context, url string) error {
	conn, _, _, err := ws.Dial(ctx, url)
	if err != nil {
		return err
	}
	c.conn = conn

	frame := ws.NewTextFrame([]byte(ProtocolVersion))
	frame = ws.MaskFrameInPlace(frame)

	if err := ws.WriteFrame(c.conn, frame); err != nil {
		c.conn.Close()
		return errors.New("magicws client: failed to send handshake version")
	}

	go c.writeLoop()
	go c.readLoop()

	return nil
}

func (c *Client) Send(data []byte) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.isClosed {
		return
	}

	select {
	case c.sendChan <- data:
	default:
	}
}

func (c *Client) readLoop() {
	defer c.Close()

	buf := GetBuffer()
	defer PutBuffer(buf)

	for {
		header, err := ws.ReadHeader(c.conn)
		if err != nil {
			break
		}

		buf.Reset()

		if header.Length > 0 {
			_, err = io.CopyN(buf, c.conn, header.Length)
			if err != nil {
				break
			}
		}

		payload := buf.Bytes()

		if header.Masked {
			ws.Cipher(payload, header.Mask, 0)
		}

		if header.OpCode.IsControl() {
			if header.OpCode == ws.OpClose {
				break
			}
			if header.OpCode == ws.OpPing {
				pongFrame := ws.NewPongFrame(payload)
				pongFrame = ws.MaskFrameInPlace(pongFrame)
				_ = ws.WriteFrame(c.conn, pongFrame)
			}
			continue
		}

		if c.onMessage != nil && (header.OpCode == ws.OpBinary || header.OpCode == ws.OpText) {
			c.onMessage(payload)
		}
	}
}

func (c *Client) writeLoop() {
	bw := bufio.NewWriterSize(c.conn, 1024)

	for data := range c.sendChan {
		frame := ws.NewBinaryFrame(data)
		frame = ws.MaskFrameInPlace(frame)

		if err := ws.WriteFrame(bw, frame); err != nil {
			break
		}

		if len(c.sendChan) == 0 {
			if err := bw.Flush(); err != nil {
				break
			}
		}
	}
}

func (c *Client) LocalAddr() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.conn != nil {
		return c.conn.LocalAddr().String()
	}
	return ""
}

func (c *Client) Close() {
	c.mu.Lock()
	if c.isClosed {
		c.mu.Unlock()
		return
	}
	c.isClosed = true
	c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
	}

	if c.onClose != nil {
		c.onClose()
	}
}
