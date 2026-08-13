package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	magicws "github.com/alirezapkg/magic-ws"
)

const (
	TotalClients = 5000
	TotalRooms   = 50
	DurationSec  = 10
	TickRateMS   = 50
)

var (
	EventPlayerMove   = magicws.NewEvent("player_move")
	EventChatMessage  = magicws.NewEvent("chat_message")
	EventPlayerAction = magicws.NewEvent("player_action")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	server := magicws.NewServer()

	serverEvents := magicws.NewServerEvents(server)

	var totalReceived uint64
	var roomCounter uint64

	server.OnConnect(func(u *magicws.User) {
		rID := uint32((atomic.AddUint64(&roomCounter, 1) % TotalRooms) + 1)
		server.Users.SetUserRoom(u, rID)
	})

	serverEvents.On(EventPlayerMove, func(u *magicws.User, payload []byte) {
		atomic.AddUint64(&totalReceived, 1)

		roomID := u.GetRoom()
		if roomID != 0 {
			serverEvents.ToRoom(roomID).Emit(EventPlayerMove, payload)
		}
	})

	serverEvents.On(EventChatMessage, func(u *magicws.User, payload []byte) {
		atomic.AddUint64(&totalReceived, 1)
		serverEvents.Emit(u, EventChatMessage, payload)
	})

	serverEvents.On(EventPlayerAction, func(u *magicws.User, payload []byte) {
		atomic.AddUint64(&totalReceived, 1)
	})

	http.HandleFunc("/ws", server.HandleWS)
	go func() {
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(500 * time.Millisecond)

	fmt.Printf("=== 🚀 Starting Multi-Event Engine Stress Test (%d Clients, %d Rooms) ===\n", TotalClients, TotalRooms)

	var wg sync.WaitGroup
	clients := make([]*magicws.Client, TotalClients)
	clientEvents := make([]*magicws.ClientEventManager, TotalClients)
	var clientReceived uint64

	startConnect := time.Now()
	batchSize := 500

	for i := 0; i < TotalClients; i += batchSize {
		end := i + batchSize
		if end > TotalClients {
			end = TotalClients
		}

		for j := i; j < end; j++ {
			wg.Add(1)
			clientID := j

			go func(id int) {
				defer wg.Done()
				c := magicws.NewClient()

				ce := magicws.NewClientEvents(c)

				ce.On(EventPlayerMove, func(payload []byte) {
					atomic.AddUint64(&clientReceived, 1)
				})

				ce.On(EventChatMessage, func(payload []byte) {
					atomic.AddUint64(&clientReceived, 1)
				})

				ce.On(EventPlayerAction, func(payload []byte) {
					atomic.AddUint64(&clientReceived, 1)
				})

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				err := c.Connect(ctx, "ws://localhost:8080/ws")
				if err != nil {
					log.Printf("Client %d failed to connect: %v", id, err)
					return
				}

				clients[id] = c
				clientEvents[id] = ce
			}(clientID)
		}
		wg.Wait()
		time.Sleep(10 * time.Millisecond)
	}

	fmt.Printf("[+] All %d Clients connected successfully in %v\n", TotalClients, time.Since(startConnect))

	stopChan := make(chan struct{})
	var totalSent uint64

	for i := 0; i < TotalClients; i++ {
		ce := clientEvents[i]
		if ce == nil {
			continue
		}

		go func(eventClient *magicws.ClientEventManager, clientIdx int) {
			ticker := time.NewTicker(TickRateMS * time.Millisecond)
			defer ticker.Stop()

			payloadMove := []byte("pos_x:123.45,pos_y:678.90")
			payloadChat := []byte("Hello world from client!")
			payloadAction := []byte("attack_skill_id:99")

			step := 0

			for {
				select {
				case <-stopChan:
					return
				case <-ticker.C:
					switch step % 3 {
					case 0:
						eventClient.Emit(EventPlayerMove, payloadMove)
					case 1:
						eventClient.Emit(EventChatMessage, payloadChat)
					case 2:
						eventClient.Emit(EventPlayerAction, payloadAction)
					}
					step++

					atomic.AddUint64(&totalSent, 1)
				}
			}
		}(ce, i)
	}

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	time.Sleep(time.Duration(DurationSec) * time.Second)
	close(stopChan)

	runtime.ReadMemStats(&m2)

	fmt.Println("\n=== 📊 MULTI-EVENT SYSTEM STRESS TEST RESULTS ===")
	fmt.Printf("Duration:            %d seconds\n", DurationSec)
	fmt.Printf("Total Sent:          %d packets\n", totalSent)
	fmt.Printf("Server Received:     %d packets\n", totalReceived)
	fmt.Printf("Clients Received:    %d packets\n", clientReceived)
	fmt.Printf("Throughput (TPS):    %.2f msg/sec\n", float64(clientReceived)/float64(DurationSec))
	fmt.Printf("Allocated Memory:    %.2f MB\n", float64(m2.TotalAlloc-m1.TotalAlloc)/(1024*1024))
	fmt.Printf("GC Runs during test: %d\n", m2.NumGC-m1.NumGC)

	for _, c := range clients {
		if c != nil {
			c.Close()
		}
	}
}
