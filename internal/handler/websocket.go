package handler

import (
	"net/http"
	"real-time-logistics-management-platform/internal/websocket"

	gorilla "github.com/gorilla/websocket"
)

type WebSocketHandler struct {
	hub *websocket.Hub
}

func NewWebSocketHandler(
	hub *websocket.Hub,
) *WebSocketHandler {
	return &WebSocketHandler{
		hub: hub,
	}
}

var upgrader = gorilla.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func (h *WebSocketHandler) Connect(
	w http.ResponseWriter,
	r *http.Request,
) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	h.hub.AddClient(conn)

	defer func() {
		h.hub.RemoveClient(conn)
		conn.Close()
	}()

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}
