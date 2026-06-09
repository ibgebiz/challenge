package ws

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	// Assessment scope: allow any origin.
	CheckOrigin: func(_ *http.Request) bool { return true },
}

// Handler upgrades HTTP connections to WebSocket and streams status events for a
// given notification id or batch id.
type Handler struct{ Hub *Hub }

// NewHandler constructs a WebSocket Handler.
func NewHandler(h *Hub) *Handler { return &Handler{Hub: h} }

// Stream handles GET /ws/notifications?id=<id> (or ?batch=<batchID>). It streams
// JSON status events until the client disconnects.
func (h *Handler) Stream(c *gin.Context) {
	key := c.Query("id")
	if key == "" {
		key = c.Query("batch")
	}
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id or batch query parameter required"})
		return
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return // Upgrade already wrote an error response
	}
	defer func() { _ = conn.Close() }()

	events := h.Hub.Subscribe(key)
	defer h.Hub.Unsubscribe(key, events)

	// Detect client disconnect by reading in a separate goroutine.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-done:
			return
		case <-c.Request.Context().Done():
			return
		case e, ok := <-events:
			if !ok {
				return
			}
			if err := conn.WriteJSON(e); err != nil {
				return
			}
		}
	}
}
