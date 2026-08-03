package magicws

import (
	"net"
	"sync"
)

type UserState int

const (
	StatePending UserState = iota
	StateConnected
	StateDisconnected
)

type User struct {
	ID       string
	conn     net.Conn
	state    UserState
	roomID   string
	sendChan chan []byte
	mu       sync.RWMutex
}

func NewUser(id string, conn net.Conn) *User {
	return &User{
		ID:       id,
		conn:     conn,
		state:    StatePending,
		sendChan: make(chan []byte, 256),
	}
}

func (u *User) Send(data []byte) {
	u.mu.RLock()
	defer u.mu.RUnlock()

	if u.state != StateConnected {
		return
	}

	select {
	case u.sendChan <- data:
	default:
	}
}

func (u *User) SetRoom(roomID string) {
	u.mu.Lock()
	u.roomID = roomID
	u.mu.Unlock()
}

func (u *User) GetRoom() string {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.roomID
}

func (u *User) LeaveRoom() {
	u.SetRoom("")
}

func (u *User) GetState() UserState {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.state
}

func (u *User) Close() {
	u.mu.Lock()
	if u.state == StateDisconnected {
		u.mu.Unlock()
		return
	}
	u.state = StateDisconnected
	u.mu.Unlock()

	if u.conn != nil {
		u.conn.Close()
	}
}
