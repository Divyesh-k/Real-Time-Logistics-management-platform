package websocket

import (
	"sync"

	gorilla "github.com/gorilla/websocket"
)

type Hub struct {
	clients map[*gorilla.Conn]bool
	mu      sync.Mutex
}

func NewHub() *Hub {
	return &Hub{
		clients: make(map[*gorilla.Conn]bool),
	}
}

func (h *Hub) AddClient(conn *gorilla.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.clients[conn] = true
}

func (h *Hub) RemoveClient(conn *gorilla.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.clients, conn)
}

func (h *Hub) Broadcast(message []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for client := range h.clients {
		err := client.WriteMessage(gorilla.TextMessage, message)

		if err != nil {
			client.Close()
			delete(h.clients, client)
		}
	}
}
