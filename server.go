package magicws

import (
	"io"
	"net/http"

	"github.com/gobwas/ws"
)

type Server struct {
	Users *UserManager

	onConnect    func(u *User)
	onDisconnect func(u *User)
	onMessage    func(u *User, data []byte)
}

func NewServer() *Server {
	return &Server{
		Users: NewUserManager(),
	}
}

func (s *Server) OnConnect(fn func(u *User)) {
	s.onConnect = fn
}

func (s *Server) OnDisconnect(fn func(u *User)) {
	s.onDisconnect = fn
}

func (s *Server) OnMessage(fn func(u *User, data []byte)) {
	s.onMessage = fn
}

func (s *Server) HandleWS(w http.ResponseWriter, r *http.Request) {
	conn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		return
	}

	user := NewUser(r.RemoteAddr, conn)
	go s.handleConnection(user)
}

func (s *Server) handleConnection(u *User) {
	defer func() {
		s.Users.Remove(u.ID)
		if s.onDisconnect != nil {
			s.onDisconnect(u)
		}
	}()

	buf := GetBuffer()
	defer PutBuffer(buf)

	header, err := ws.ReadHeader(u.conn)
	if err != nil {
		u.Close()
		return
	}

	_, err = io.CopyN(buf, u.conn, header.Length)
	if err != nil {
		u.Close()
		return
	}

	payload := buf.Bytes()
	if header.Masked {
		ws.Cipher(payload, header.Mask, 0)
	}

	if string(payload) != ProtocolVersion {
		u.Close()
		return
	}

	u.state = StateConnected
	s.Users.Add(u)

	if s.onConnect != nil {
		s.onConnect(u)
	}

	go func() {
		for data := range u.sendChan {
			frame := ws.NewBinaryFrame(data)
			if err := ws.WriteFrame(u.conn, frame); err != nil {
				break
			}
		}
	}()

	for {
		header, err := ws.ReadHeader(u.conn)
		if err != nil {
			break
		}

		buf.Reset()

		if header.Length > 0 {
			_, err = io.CopyN(buf, u.conn, header.Length)
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
				_ = ws.WriteFrame(u.conn, pongFrame)
			}
			continue
		}

		if s.onMessage != nil && (header.OpCode == ws.OpBinary || header.OpCode == ws.OpText) {
			s.onMessage(u, payload)
		}
	}
}
