package magicws

import (
	"sync"
)

type UserManager struct {
	mu    sync.RWMutex
	users map[string]*User
}

func NewUserManager() *UserManager {
	return &UserManager{
		users: make(map[string]*User),
	}
}

func (um *UserManager) Add(u *User) {
	um.mu.Lock()
	um.users[u.ID] = u
	um.mu.Unlock()
}

func (um *UserManager) Get(id string) (*User, bool) {
	um.mu.RLock()
	defer um.mu.RUnlock()
	u, ok := um.users[id]
	return u, ok
}

func (um *UserManager) Remove(id string) {
	um.mu.Lock()
	u, exists := um.users[id]
	if exists {
		delete(um.users, id)
	}
	um.mu.Unlock()

	if exists {
		u.Close()
	}
}

func (um *UserManager) SendToRoom(roomID string, data []byte) {
	if roomID == "" {
		return
	}

	um.mu.RLock()
	defer um.mu.RUnlock()

	for _, u := range um.users {
		u.mu.RLock()
		inRoom := u.roomID == roomID
		u.mu.RUnlock()

		if inRoom {
			u.Send(data)
		}
	}
}

func (um *UserManager) SendToRoomExcept(roomID string, exceptUserID string, data []byte) {
	if roomID == "" {
		return
	}

	um.mu.RLock()
	defer um.mu.RUnlock()

	for _, u := range um.users {
		if u.ID == exceptUserID {
			continue
		}

		u.mu.RLock()
		inRoom := u.roomID == roomID
		u.mu.RUnlock()

		if inRoom {
			u.Send(data)
		}
	}
}

func (um *UserManager) Broadcast(data []byte) {
	um.mu.RLock()
	defer um.mu.RUnlock()

	for _, u := range um.users {
		u.Send(data)
	}
}

func (um *UserManager) ClearRoom(roomID string) {
	if roomID == "" {
		return
	}

	um.mu.RLock()
	defer um.mu.RUnlock()

	for _, u := range um.users {
		u.mu.Lock()
		if u.roomID == roomID {
			u.roomID = ""
		}
		u.mu.Unlock()
	}
}
