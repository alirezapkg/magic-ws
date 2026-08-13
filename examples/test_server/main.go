package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	magicws "github.com/alirezapkg/magic-ws"
)

var (
	EventPlayerMove  = magicws.NewEvent("player_move")
	EventChatMessage = magicws.NewEvent("chat_message")
)

func main() {
	server := magicws.NewServer()
	serverEvents := magicws.NewServerEvents(server)

	server.OnConnect(func(u *magicws.User) {

		log.Printf("[+] Client connected: %s", u.ID)

		const targetRoom uint32 = 101
		server.Users.SetUserRoom(u, targetRoom)

		log.Printf("[->] Moved user %s to Room %d", u.ID, targetRoom)

		welcomeMsg := []byte("Welcome to MagicWS Server!")
		serverEvents.Emit(u, EventChatMessage, welcomeMsg)
	})

	serverEvents.On(EventPlayerMove, func(u *magicws.User, payload []byte) {

		log.Printf("[Move] Received from %s: %s", u.ID, string(payload))

		roomID := u.GetRoom()
		if roomID != 0 {
			echoPayload := []byte(fmt.Sprintf("Server Echo -> %s", string(payload)))
			serverEvents.ToRoom(roomID).Emit(EventPlayerMove, echoPayload)
		}
	})

	serverEvents.On(EventChatMessage, func(u *magicws.User, payload []byte) {

		log.Printf("[Chat] Received from %s: %s", u.ID, string(payload))

		responsePayload := []byte(fmt.Sprintf("Server Ack: %s", string(payload)))
		serverEvents.Emit(u, EventChatMessage, responsePayload)
	})

	http.HandleFunc("/ws", server.HandleWS)

	fmt.Println("=== 🚀 MagicWS Server listening on ws://localhost:8080/ws ===")
	fmt.Println("Waiting for Client connection...")

	go func() {
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nShutting down server...")
}
