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

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	server := magicws.NewServer()

	var totalReceived uint64
	var roomCounter uint64

	server.OnConnect(func(u *magicws.User) {
		rID := uint32((atomic.AddUint64(&roomCounter, 1) % TotalRooms) + 1)
		server.Users.SetUserRoom(u, rID)
	})

	server.OnMessage(func(u *magicws.User, data []byte) {
		atomic.AddUint64(&totalReceived, 1)

		roomID := u.GetRoom()
		if roomID != 0 {
			server.Users.SendToRoom(roomID, data)
		}
	})

	http.HandleFunc("/ws", server.HandleWS)
	go func() {
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(500 * time.Millisecond)

	fmt.Printf("=== 🚀 Starting Heavy Stress Test (%d Clients, %d Rooms) ===\n", TotalClients, TotalRooms)

	var wg sync.WaitGroup
	clients := make([]*magicws.Client, TotalClients)
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

				c.OnMessage(func(data []byte) {
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
			}(clientID)
		}
		wg.Wait()
		time.Sleep(10 * time.Millisecond)
	}

	fmt.Printf("[+] All %d Clients connected successfully in %v\n", TotalClients, time.Since(startConnect))

	stopChan := make(chan struct{})
	var totalSent uint64

	for i := 0; i < TotalClients; i++ {
		c := clients[i]
		if c == nil {
			continue
		}

		go func(cl *magicws.Client) {
			ticker := time.NewTicker(TickRateMS * time.Millisecond)
			defer ticker.Stop()

			payload := []byte("pos_x:123.45,pos_y:678.90")

			for {
				select {
				case <-stopChan:
					return
				case <-ticker.C:
					cl.Send(payload)
					atomic.AddUint64(&totalSent, 1)
				}
			}
		}(c)
	}

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)

	time.Sleep(time.Duration(DurationSec) * time.Second)
	close(stopChan)

	runtime.ReadMemStats(&m2)

	fmt.Println("\n=== 📊 5K STRESS TEST RESULTS ===")
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
