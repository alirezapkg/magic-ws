package magicws

import (
	"sync"
)

type UserManager struct {
	mu         sync.RWMutex
	users      map[string]*User
	roomGroups map[uint32][]*User
}

func NewUserManager() *UserManager {
	return &UserManager{
		users:      make(map[string]*User),
		roomGroups: make(map[uint32][]*User),
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
		oldRoom := u.GetRoom()
		if oldRoom != 0 {
			um.removeFromRoomGroup(u, oldRoom)
		}
	}
	um.mu.Unlock()

	if exists {
		u.Close()
	}
}

func (um *UserManager) SetUserRoom(u *User, roomID uint32) {
	um.mu.Lock()
	defer um.mu.Unlock()

	oldRoom := u.GetRoom()
	if oldRoom == roomID {
		return
	}

	if oldRoom != 0 {
		um.removeFromRoomGroup(u, oldRoom)
	}

	u.SetRoom(roomID)

	if roomID != 0 {
		um.roomGroups[roomID] = append(um.roomGroups[roomID], u)
	}
}

func (um *UserManager) removeFromRoomGroup(u *User, roomID uint32) {
	group := um.roomGroups[roomID]
	for i, user := range group {
		if user.ID == u.ID {
			group[i] = group[len(group)-1]
			um.roomGroups[roomID] = group[:len(group)-1]
			break
		}
	}
}

func (um *UserManager) SendToRoom(roomID uint32, data []byte) {
	if roomID == 0 {
		return
	}

	um.mu.RLock()
	users := um.roomGroups[roomID]
	for _, u := range users {
		u.Send(data)
	}
	um.mu.RUnlock()
}

func (um *UserManager) SendToRoomExcept(roomID uint32, exceptUserID string, data []byte) {
	if roomID == 0 {
		return
	}

	um.mu.RLock()
	users := um.roomGroups[roomID]
	for _, u := range users {
		if u.ID != exceptUserID {
			u.Send(data)
		}
	}
	um.mu.RUnlock()
}

func (um *UserManager) Broadcast(data []byte) {
	um.mu.RLock()
	targets := make([]*User, 0, len(um.users))
	for _, u := range um.users {
		targets = append(targets, u)
	}
	um.mu.RUnlock()

	for _, u := range targets {
		u.Send(data)
	}
}

func (um *UserManager) ClearRoom(roomID uint32) {
	if roomID == 0 {
		return
	}

	um.mu.Lock()
	users := um.roomGroups[roomID]
	delete(um.roomGroups, roomID)
	for _, u := range users {
		u.SetRoom(0)
	}
	um.mu.Unlock()
}
