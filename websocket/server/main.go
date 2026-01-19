package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fatih/color"
	"github.com/gorilla/websocket"
	"golang.org/x/time/rate"
)

// =============================================================================
// Configuration
// =============================================================================

const (
	// Server settings
	ServerAddr      = ":8443"
	ReadBufferSize  = 4096
	WriteBufferSize = 4096

	// Timeout settings
	HandshakeTimeout = 10 * time.Second
	ReadTimeout      = 60 * time.Second
	WriteTimeout     = 10 * time.Second
	PingInterval     = 30 * time.Second
	PongWait         = 60 * time.Second

	// Connection limits
	MaxMessageSize = 8192 // 8KB
	MaxConnections = 10000

	// Rate limiting
	RateLimit = 100 // messages per second per client
	RateBurst = 200
)

// Certificate paths
var (
	ServerCertFile string
	ServerKeyFile  string
	CACertFile     string
	ClientCertFile string
)

func init() {
	projectRoot := getProjectRoot()
	ServerCertFile = filepath.Join(projectRoot, "certs", "server", "server-cert.pem")
	ServerKeyFile = filepath.Join(projectRoot, "certs", "server", "server-key.pem")
	CACertFile = filepath.Join(projectRoot, "certs", "ca", "ca-cert.pem")
	ClientCertFile = filepath.Join(projectRoot, "certs", "client", "client-cert.pem")
}

func getProjectRoot() string {
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			cwd, _ := os.Getwd()
			return filepath.Join(cwd, "..", "..")
		}
		dir = parent
	}
}

// =============================================================================
// Message Types
// =============================================================================

type MessageType string

const (
	MsgTypeText      MessageType = "text"
	MsgTypeBroadcast MessageType = "broadcast"
	MsgTypePing      MessageType = "ping"
	MsgTypePong      MessageType = "pong"
	MsgTypeStats     MessageType = "stats"
)

type Message struct {
	Type      MessageType `json:"type"`
	From      string      `json:"from"`
	To        string      `json:"to,omitempty"`
	Content   string      `json:"content"`
	Timestamp int64       `json:"timestamp"`
}

// =============================================================================
// Client Connection
// =============================================================================

type Client struct {
	ID         string
	Conn       *websocket.Conn
	Send       chan []byte
	Hub        *Hub
	limiter    *rate.Limiter
	lastActive time.Time
	mu         sync.RWMutex
}

func NewClient(id string, conn *websocket.Conn, hub *Hub) *Client {
	return &Client{
		ID:         id,
		Conn:       conn,
		Send:       make(chan []byte, 256),
		Hub:        hub,
		limiter:    rate.NewLimiter(RateLimit, RateBurst),
		lastActive: time.Now(),
	}
}

// ReadPump xử lý incoming messages từ client
func (c *Client) ReadPump() {
	defer func() {
		c.Hub.Unregister <- c
		c.Conn.Close()
		color.Yellow("[Client %s] Disconnected", c.ID)
	}()

	c.Conn.SetReadLimit(MaxMessageSize)
	c.Conn.SetReadDeadline(time.Now().Add(PongWait))
	c.Conn.SetPongHandler(func(string) error {
		c.Conn.SetReadDeadline(time.Now().Add(PongWait))
		c.updateLastActive()
		return nil
	})

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("[Client %s] Error: %v", c.ID, err)
			}
			break
		}

		// Rate limiting
		if !c.limiter.Allow() {
			log.Printf("[Client %s] Rate limit exceeded", c.ID)
			continue
		}

		c.updateLastActive()

		// Parse message
		var msg Message
		if err := json.Unmarshal(message, &msg); err != nil {
			log.Printf("[Client %s] Invalid message format: %v", c.ID, err)
			continue
		}

		msg.From = c.ID
		msg.Timestamp = time.Now().Unix()

		// Handle different message types
		c.Hub.HandleMessage(c, &msg)
	}
}

// WritePump xử lý outgoing messages tới client
func (c *Client) WritePump() {
	ticker := time.NewTicker(PingInterval)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.Send:
			c.Conn.SetWriteDeadline(time.Now().Add(WriteTimeout))
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.Conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(WriteTimeout))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Client) updateLastActive() {
	c.mu.Lock()
	c.lastActive = time.Now()
	c.mu.Unlock()
}

func (c *Client) GetLastActive() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastActive
}

// =============================================================================
// Hub - Quản lý tất cả connections
// =============================================================================

type Hub struct {
	// Registered clients
	Clients map[string]*Client
	mu      sync.RWMutex

	// Channels
	Register   chan *Client
	Unregister chan *Client
	Broadcast  chan []byte

	// Stats
	TotalConnections   atomic.Uint64
	TotalMessages      atomic.Uint64
	TotalBroadcasts    atomic.Uint64
	CurrentConnections atomic.Int64
}

func NewHub() *Hub {
	return &Hub{
		Clients:    make(map[string]*Client),
		Register:   make(chan *Client, 10),
		Unregister: make(chan *Client, 10),
		Broadcast:  make(chan []byte, 256),
	}
}

func (h *Hub) Run() {
	color.Cyan("[Hub] Started")

	// Cleanup goroutine - remove inactive clients
	go h.cleanupInactiveClients()

	// Stats reporting
	go h.reportStats()

	for {
		select {
		case client := <-h.Register:
			h.registerClient(client)

		case client := <-h.Unregister:
			h.unregisterClient(client)

		case message := <-h.Broadcast:
			h.broadcastMessage(message)
		}
	}
}

func (h *Hub) registerClient(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.Clients) >= MaxConnections {
		log.Printf("[Hub] Max connections reached, rejecting client %s", client.ID)
		client.Conn.Close()
		return
	}

	h.Clients[client.ID] = client
	h.TotalConnections.Add(1)
	h.CurrentConnections.Add(1)

	color.Green("[Hub] Client registered: %s (Total: %d)",
		client.ID, h.CurrentConnections.Load())

	// Send welcome message
	welcome := Message{
		Type:      MsgTypeText,
		From:      "server",
		Content:   fmt.Sprintf("Welcome %s! Connected clients: %d", client.ID, len(h.Clients)),
		Timestamp: time.Now().Unix(),
	}
	if data, err := json.Marshal(welcome); err == nil {
		client.Send <- data
	}
}

func (h *Hub) unregisterClient(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.Clients[client.ID]; ok {
		delete(h.Clients, client.ID)
		close(client.Send)
		h.CurrentConnections.Add(-1)

		color.Yellow("[Hub] Client unregistered: %s (Remaining: %d)",
			client.ID, h.CurrentConnections.Load())
	}
}

func (h *Hub) broadcastMessage(message []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	h.TotalBroadcasts.Add(1)

	for _, client := range h.Clients {
		select {
		case client.Send <- message:
		default:
			// Client slow to read, skip
			log.Printf("[Hub] Client %s buffer full, skipping message", client.ID)
		}
	}
}

func (h *Hub) HandleMessage(client *Client, msg *Message) {
	h.TotalMessages.Add(1)

	switch msg.Type {
	case MsgTypeBroadcast:
		// Broadcast to all clients
		if data, err := json.Marshal(msg); err == nil {
			h.Broadcast <- data
		}

	case MsgTypeText:
		// Direct message
		if msg.To != "" {
			h.sendDirectMessage(client, msg)
		} else {
			// Echo back
			if data, err := json.Marshal(msg); err == nil {
				client.Send <- data
			}
		}

	case MsgTypeStats:
		// Send server stats
		h.sendStats(client)

	case MsgTypePing:
		// Respond with pong
		pong := Message{
			Type:      MsgTypePong,
			From:      "server",
			Timestamp: time.Now().Unix(),
		}
		if data, err := json.Marshal(pong); err == nil {
			client.Send <- data
		}
	}
}

func (h *Hub) sendDirectMessage(from *Client, msg *Message) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if to, ok := h.Clients[msg.To]; ok {
		if data, err := json.Marshal(msg); err == nil {
			select {
			case to.Send <- data:
			default:
				log.Printf("[Hub] Failed to send to %s (buffer full)", msg.To)
			}
		}
	}
}

func (h *Hub) sendStats(client *Client) {
	stats := Message{
		Type: MsgTypeStats,
		From: "server",
		Content: fmt.Sprintf("Total Connections: %d | Current: %d | Messages: %d | Broadcasts: %d",
			h.TotalConnections.Load(),
			h.CurrentConnections.Load(),
			h.TotalMessages.Load(),
			h.TotalBroadcasts.Load(),
		),
		Timestamp: time.Now().Unix(),
	}
	if data, err := json.Marshal(stats); err == nil {
		client.Send <- data
	}
}

func (h *Hub) cleanupInactiveClients() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		h.mu.RLock()
		var inactive []*Client
		for _, client := range h.Clients {
			if time.Since(client.GetLastActive()) > 5*time.Minute {
				inactive = append(inactive, client)
			}
		}
		h.mu.RUnlock()

		for _, client := range inactive {
			color.Yellow("[Hub] Removing inactive client: %s", client.ID)
			h.Unregister <- client
		}
	}
}

func (h *Hub) reportStats() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		color.Cyan("[Stats] Connections: %d | Total Messages: %d | Broadcasts: %d",
			h.CurrentConnections.Load(),
			h.TotalMessages.Load(),
			h.TotalBroadcasts.Load(),
		)
	}
}

// =============================================================================
// WebSocket Upgrader với TLS configuration
// =============================================================================

var upgrader = websocket.Upgrader{
	ReadBufferSize:   ReadBufferSize,
	WriteBufferSize:  WriteBufferSize,
	HandshakeTimeout: HandshakeTimeout,
	CheckOrigin: func(r *http.Request) bool {
		// Production: validate origin properly
		return true
	},
}

// =============================================================================
// HTTP Handlers
// =============================================================================

func (h *Hub) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Upgrade HTTP connection to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WS] Upgrade error: %v", err)
		return
	}

	// Generate client ID
	clientID := fmt.Sprintf("client_%d", time.Now().UnixNano())

	// Create client
	client := NewClient(clientID, conn, h)
	h.Register <- client

	// Start read/write pumps
	go client.WritePump()
	go client.ReadPump()
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
		"time":   time.Now().Format(time.RFC3339),
	})
}

// =============================================================================
// TLS Configuration
// =============================================================================

func createTLSConfig(mutualTLS bool) (*tls.Config, error) {
	// Load server certificate
	cert, err := tls.LoadX509KeyPair(ServerCertFile, ServerKeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server certificate: %v", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13, // TLS 1.3 only
		MaxVersion:   tls.VersionTLS13,

		// Strong cipher suites với Perfect Forward Secrecy
		CipherSuites: []uint16{
			tls.TLS_AES_256_GCM_SHA384,       // TLS 1.3
			tls.TLS_CHACHA20_POLY1305_SHA256, // TLS 1.3
			tls.TLS_AES_128_GCM_SHA256,       // TLS 1.3
		},

		// Curve preferences for ECDHE (Perfect Forward Secrecy)
		CurvePreferences: []tls.CurveID{
			tls.X25519,    // Modern, fast
			tls.CurveP256, // Widely supported
		},

		PreferServerCipherSuites: true,
	}

	// Mutual TLS Authentication (optional)
	if mutualTLS {
		caCert, err := ioutil.ReadFile(CACertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %v", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}

		tlsConfig.ClientCAs = caCertPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert

		color.Cyan("[TLS] Mutual TLS authentication enabled")
	}

	return tlsConfig, nil
}

// =============================================================================
// Main
// =============================================================================

func main() {
	color.Cyan("=== May chu WebSocket voi SSL/TLS ===")
	color.Cyan("Hieu nang cao & Bao mat")
	fmt.Println()

	// Kiem tra chung chi SSL
	if _, err := os.Stat(ServerCertFile); os.IsNotExist(err) {
		color.Red("[LOI] Khong tim thay chung chi may chu!")
		color.Yellow("Chay: cd certs/scripts && ./generate-all.sh")
		os.Exit(1)
	}

	// Kiem tra tuy chon mutual TLS
	mutualTLS := false
	if len(os.Args) > 1 && os.Args[1] == "--mutual-tls" {
		mutualTLS = true
	}

	// Tao cau hinh TLS
	tlsConfig, err := createTLSConfig(mutualTLS)
	if err != nil {
		color.Red("[LOI] Tao cau hinh TLS that bai: %v", err)
		os.Exit(1)
	}

	// In thong tin TLS
	color.Green("[TLS] Cau hinh:")
	color.White("  - Phien ban: TLS 1.3")
	color.White("  - Bo ma hoa: TLS_AES_256_GCM_SHA384, TLS_CHACHA20_POLY1305_SHA256")
	color.White("  - Perfect Forward Secrecy: Bat (X25519, P256)")
	color.White("  - Mutual TLS: %v", mutualTLS)
	fmt.Println()

	// Create hub
	hub := NewHub()
	go hub.Run()

	// Setup HTTP routes
	http.HandleFunc("/ws", hub.handleWebSocket)
	http.HandleFunc("/health", handleHealth)

	// Create HTTPS server
	server := &http.Server{
		Addr:         ServerAddr,
		TLSConfig:    tlsConfig,
		ReadTimeout:  ReadTimeout,
		WriteTimeout: WriteTimeout,
		IdleTimeout:  120 * time.Second,
	}

	// Khoi dong may chu
	color.Green("[May chu] Dang khoi dong WSS tren https://localhost%s", ServerAddr)
	color.White("  - Dia chi WebSocket: wss://localhost%s/ws", ServerAddr)
	color.White("  - Kiem tra suc khoe: https://localhost%s/health", ServerAddr)
	color.White("  - So ket noi toi da: %d", MaxConnections)
	color.White("  - Gioi han toc do: %d tin nhan/s moi client", RateLimit)
	fmt.Println()
	color.Cyan("[May chu] Nhan Ctrl+C de dung")
	fmt.Println()

	// Start HTTPS server
	if err := server.ListenAndServeTLS("", ""); err != nil {
		color.Red("[ERROR] Server failed: %v", err)
		os.Exit(1)
	}
}
