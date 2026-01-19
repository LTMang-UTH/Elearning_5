package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/fatih/color"
	"github.com/gorilla/websocket"
)

// =============================================================================
// Configuration
// =============================================================================

var (
	ServerURL      = "wss://localhost:8443/ws"
	CACertFile     string
	ClientCertFile string
	ClientKeyFile  string
)

func init() {
	// Get absolute path to project root
	projectRoot := getProjectRoot()
	CACertFile = filepath.Join(projectRoot, "certs", "ca", "ca-cert.pem")
	ClientCertFile = filepath.Join(projectRoot, "certs", "client", "client-cert.pem")
	ClientKeyFile = filepath.Join(projectRoot, "certs", "client", "client-key.pem")
}

func getProjectRoot() string {
	// Try to find project root by looking for go.mod
	dir, _ := os.Getwd()
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			// Reached root, fallback to current dir
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
// WebSocket Client
// =============================================================================

type Client struct {
	conn      *websocket.Conn
	done      chan struct{}
	interrupt chan os.Signal
}

func NewClient(mutualTLS bool) (*Client, error) {
	// Create TLS config
	tlsConfig, err := createTLSConfig(mutualTLS)
	if err != nil {
		return nil, fmt.Errorf("failed to create TLS config: %v", err)
	}

	// Parse server URL
	u, err := url.Parse(ServerURL)
	if err != nil {
		return nil, fmt.Errorf("invalid server URL: %v", err)
	}

	// Create WebSocket dialer
	dialer := &websocket.Dialer{
		TLSClientConfig:  tlsConfig,
		HandshakeTimeout: 10 * time.Second,
	}

	// Connect to server
	color.Cyan("[Khach hang] Dang ket noi toi %s...", ServerURL)
	conn, resp, err := dialer.Dial(u.String(), nil)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("connection failed (HTTP %d): %v", resp.StatusCode, err)
		}
		return nil, fmt.Errorf("connection failed: %v", err)
	}
	defer resp.Body.Close()

	color.Green("[Khach hang] Ket noi thanh cong!")
	color.White("  - Phien ban TLS: %s", tlsVersionString(conn.UnderlyingConn()))
	color.White("  - Bo ma hoa: %s", cipherSuiteString(conn.UnderlyingConn()))
	fmt.Println()

	client := &Client{
		conn:      conn,
		done:      make(chan struct{}),
		interrupt: make(chan os.Signal, 1),
	}

	signal.Notify(client.interrupt, os.Interrupt)

	return client, nil
}

func (c *Client) ReadMessages() {
	defer close(c.done)

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				color.Red("[Khach hang] Loi doc du lieu: %v", err)
			}
			return
		}

		var msg Message
		if err := json.Unmarshal(message, &msg); err != nil {
			color.Red("[Khach hang] Loi phan tich tin nhan: %v", err)
			continue
		}

		c.displayMessage(&msg)
	}
}

func (c *Client) displayMessage(msg *Message) {
	timestamp := time.Unix(msg.Timestamp, 0).Format("15:04:05")

	switch msg.Type {
	case MsgTypeText:
		if msg.From == "server" {
			color.Green("[%s] [%s] %s", timestamp, msg.From, msg.Content)
		} else {
			color.Cyan("[%s] [%s] %s", timestamp, msg.From, msg.Content)
		}

	case MsgTypeBroadcast:
		color.Yellow("[%s] [PHAT TIN] [%s] %s", timestamp, msg.From, msg.Content)

	case MsgTypePong:
		color.Magenta("[%s] [PONG] May chu da phan hoi", timestamp)

	case MsgTypeStats:
		color.White("[%s] [THONG KE] %s", timestamp, msg.Content)

	default:
		color.White("[%s] [%s] %s", timestamp, msg.Type, msg.Content)
	}
}

func (c *Client) SendMessage(msg *Message) error {
	msg.Timestamp = time.Now().Unix()
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return fmt.Errorf("failed to send message: %v", err)
	}

	return nil
}

func (c *Client) Close() {
	color.Yellow("[Khach hang] Dang dong ket noi...")
	c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	time.Sleep(100 * time.Millisecond)
	c.conn.Close()
}

func (c *Client) InteractiveMode() {
	// Start reading messages
	go c.ReadMessages()

	// Send periodic stats requests
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.SendMessage(&Message{
					Type: MsgTypeStats,
				})
			case <-c.done:
				return
			}
		}
	}()

	// Interactive loop
	fmt.Println()
	color.Cyan("=== Che do tuong tac ===")
	color.White("Lenh:")
	color.White("  text <noidung>       - Gui tin nhan text")
	color.White("  broadcast <noidung>  - Phat tin cho tat ca client")
	color.White("  ping                 - Kiem tra ket noi may chu")
	color.White("  stats                - Lay thong ke tu may chu")
	color.White("  quit                 - Thoat")
	fmt.Println()

	for {
		select {
		case <-c.interrupt:
			color.Yellow("[Khach hang] Nhan tin hieu ngat")
			return

		case <-c.done:
			return

		default:
			var input string
			fmt.Print("> ")
			if _, err := fmt.Scanln(&input); err != nil {
				continue
			}

			if input == "quit" {
				return
			}

			var msg Message
			switch {
			case input == "ping":
				msg = Message{Type: MsgTypePing}

			case input == "stats":
				msg = Message{Type: MsgTypeStats}

			case len(input) > 5 && input[:4] == "text":
				msg = Message{
					Type:    MsgTypeText,
					Content: input[5:],
				}

			case len(input) > 10 && input[:9] == "broadcast":
				msg = Message{
					Type:    MsgTypeBroadcast,
					Content: input[10:],
				}

			default:
				// Default to text message
				msg = Message{
					Type:    MsgTypeText,
					Content: input,
				}
			}

			if err := c.SendMessage(&msg); err != nil {
				color.Red("[Khach hang] Gui tin that bai: %v", err)
			}
		}
	}
}

// =============================================================================
// TLS Configuration
// =============================================================================

func createTLSConfig(mutualTLS bool) (*tls.Config, error) {
	// Load CA certificate
	caCert, err := ioutil.ReadFile(CACertFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %v", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	tlsConfig := &tls.Config{
		RootCAs:    caCertPool,
		MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13,
	}

	// Load client certificate for mutual TLS
	if mutualTLS {
		cert, err := tls.LoadX509KeyPair(ClientCertFile, ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %v", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
		color.Cyan("[TLS] Using client certificate for mutual TLS")
	}

	return tlsConfig, nil
}

func tlsVersionString(conn interface{}) string {
	if tlsConn, ok := conn.(*tls.Conn); ok {
		state := tlsConn.ConnectionState()
		switch state.Version {
		case tls.VersionTLS10:
			return "TLS 1.0"
		case tls.VersionTLS11:
			return "TLS 1.1"
		case tls.VersionTLS12:
			return "TLS 1.2"
		case tls.VersionTLS13:
			return "TLS 1.3"
		}
	}
	return "Unknown"
}

func cipherSuiteString(conn interface{}) string {
	if tlsConn, ok := conn.(*tls.Conn); ok {
		state := tlsConn.ConnectionState()
		return tls.CipherSuiteName(state.CipherSuite)
	}
	return "Unknown"
}

// =============================================================================
// Main
// =============================================================================

func main() {
	color.Cyan("=== Khach hang WebSocket bao mat ===")
	fmt.Println()

	// Check if CA certificate exists
	if _, err := os.Stat(CACertFile); os.IsNotExist(err) {
		color.Red("[LOI] Khong tim thay chung chi CA!")
		color.Yellow("Chay: cd certs/scripts && ./generate-all.sh")
		os.Exit(1)
	}

	// Parse command line for mutual TLS option
	mutualTLS := false
	if len(os.Args) > 1 && os.Args[1] == "--mutual-tls" {
		mutualTLS = true
	}

	// Create client
	client, err := NewClient(mutualTLS)
	if err != nil {
		color.Red("[LOI] Ket noi that bai: %v", err)
		os.Exit(1)
	}
	defer client.Close()

	// Run interactive mode
	client.InteractiveMode()

	// Wait for graceful shutdown
	<-client.done
	color.Green("[Khach hang] Da ngat ket noi")
}
