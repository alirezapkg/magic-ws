package magicws

import (
	"net"
	"sync/atomic"
)

type UserState int32

const (
	StatePending UserState = iota
	StateConnected
	StateDisconnected
)

type User struct {
	ID       string
	conn     net.Conn
	state    int32
	roomID   uint32
	sendChan chan []byte
}

func NewUser(id string, conn net.Conn) *User {
	return &User{
		ID:       id,
		conn:     conn,
		state:    int32(StatePending),
		sendChan: make(chan []byte, 256),
	}
}

func (u *User) GetState() UserState {
	return UserState(atomic.LoadInt32(&u.state))
}

func (u *User) SetState(state UserState) {
	atomic.StoreInt32(&u.state, int32(state))
}

func (u *User) GetRoom() uint32 {
	return atomic.LoadUint32(&u.roomID)
}

func (u *User) SetRoom(roomID uint32) {
	atomic.StoreUint32(&u.roomID, roomID)
}

func (u *User) Send(data []byte) {
	if u.GetState() != StateConnected {
		return
	}

	select {
	case u.sendChan <- data:
	default:
		// Drop packet on channel full
	}
}

func (u *User) Close() {
	if atomic.CompareAndSwapInt32(&u.state, int32(StateConnected), int32(StateDisconnected)) ||
		atomic.CompareAndSwapInt32(&u.state, int32(StatePending), int32(StateDisconnected)) {
		if u.conn != nil {
			_ = u.conn.Close()
		}
	}
}
