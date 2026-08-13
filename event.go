package magicws

import (
	"encoding/binary"
	"hash/fnv"
	"sync"
)

type EventID uint32

type Event struct {
	ID   EventID
	Name string
}

func NewEvent(name string) Event {
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return Event{
		ID:   EventID(h.Sum32()),
		Name: name,
	}
}

type EventHandler func(payload []byte)
type UserEventHandler func(u *User, payload []byte)

func PackEventID(id EventID, payload []byte) []byte {
	out := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(out[:4], uint32(id))
	copy(out[4:], payload)
	return out
}

// --------------------------------------------------
// Server Event Router
// --------------------------------------------------

type ServerEventManager struct {
	server *Server
	mu     sync.RWMutex
	mapH   map[EventID]UserEventHandler
}

func NewServerEvents(s *Server) *ServerEventManager {
	em := &ServerEventManager{
		server: s,
		mapH:   make(map[EventID]UserEventHandler),
	}

	s.OnMessage(func(u *User, data []byte) {
		em.dispatch(u, data)
	})

	return em
}

func (em *ServerEventManager) On(e Event, handler UserEventHandler) {
	em.mu.Lock()
	em.mapH[e.ID] = handler
	em.mu.Unlock()
}

func (em *ServerEventManager) dispatch(u *User, data []byte) {
	if len(data) < 4 {
		return
	}

	eventID := EventID(binary.BigEndian.Uint32(data[:4]))
	payload := data[4:]

	em.mu.RLock()
	handler, exists := em.mapH[eventID]
	em.mu.RUnlock()

	if exists && handler != nil {
		handler(u, payload)
	}
}

func (em *ServerEventManager) Emit(u *User, e Event, payload []byte) {
	msg := PackEventID(e.ID, payload)
	u.Send(msg)
}

func (em *ServerEventManager) ToRoom(roomID uint32) *RoomEmitBuilder {
	return &RoomEmitBuilder{
		em:     em,
		roomID: roomID,
	}
}

type RoomEmitBuilder struct {
	em     *ServerEventManager
	roomID uint32
}

func (b *RoomEmitBuilder) Emit(e Event, payload []byte) {
	msg := PackEventID(e.ID, payload)
	b.em.server.Users.SendToRoom(b.roomID, msg)
}

// --------------------------------------------------
// Client Event Router
// --------------------------------------------------

type ClientEventManager struct {
	client *Client
	mu     sync.RWMutex
	mapH   map[EventID]EventHandler
}

func NewClientEvents(c *Client) *ClientEventManager {
	em := &ClientEventManager{
		client: c,
		mapH:   make(map[EventID]EventHandler),
	}

	c.OnMessage(func(data []byte) {
		em.dispatch(data)
	})

	return em
}

func (em *ClientEventManager) On(e Event, handler EventHandler) {
	em.mu.Lock()
	em.mapH[e.ID] = handler
	em.mu.Unlock()
}

func (em *ClientEventManager) dispatch(data []byte) {
	if len(data) < 4 {
		return
	}

	eventID := EventID(binary.BigEndian.Uint32(data[:4]))
	payload := data[4:]

	em.mu.RLock()
	handler, exists := em.mapH[eventID]
	em.mu.RUnlock()

	if exists && handler != nil {
		handler(payload)
	}
}

func (em *ClientEventManager) Emit(e Event, payload []byte) {
	msg := PackEventID(e.ID, payload)
	em.client.Send(msg)
}
