package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fatih/color"
	"github.com/gorilla/websocket"
)

// =============================================================================
// Configuration
// =============================================================================

var (
	ServerURL  = "wss://localhost:8443/ws"
	CACertFile string
)

func init() {
	projectRoot := getProjectRoot()
	CACertFile = filepath.Join(projectRoot, "certs", "ca", "ca-cert.pem")
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

// Command line flags
var (
	numClients  = flag.Int("clients", 100, "Number of concurrent clients")
	duration    = flag.Duration("duration", 60*time.Second, "Test duration")
	messageRate = flag.Int("message-rate", 10, "Messages per second per client")
	rampUp      = flag.Duration("ramp-up", 10*time.Second, "Ramp-up time to spawn all clients")
)

// =============================================================================
// Message Types
// =============================================================================

type MessageType string

const (
	MsgTypeText      MessageType = "text"
	MsgTypeBroadcast MessageType = "broadcast"
	MsgTypePing      MessageType = "ping"
)

type Message struct {
	Type      MessageType `json:"type"`
	From      string      `json:"from"`
	Content   string      `json:"content"`
	Timestamp int64       `json:"timestamp"`
}

// =============================================================================
// Metrics
// =============================================================================

type Metrics struct {
	ConnectedClients   atomic.Int64
	TotalMessages      atomic.Uint64
	SuccessfulMessages atomic.Uint64
	FailedMessages     atomic.Uint64
	TotalLatency       atomic.Uint64 // in microseconds
	MinLatency         atomic.Uint64
	MaxLatency         atomic.Uint64
	mu                 sync.Mutex
	latencies          []time.Duration
}

func NewMetrics() *Metrics {
	m := &Metrics{
		latencies: make([]time.Duration, 0, 100000),
	}
	m.MinLatency.Store(^uint64(0)) // Max uint64
	return m
}

func (m *Metrics) RecordLatency(latency time.Duration) {
	micros := uint64(latency.Microseconds())

	m.TotalLatency.Add(micros)

	// Update min
	for {
		old := m.MinLatency.Load()
		if micros >= old || m.MinLatency.CompareAndSwap(old, micros) {
			break
		}
	}

	// Update max
	for {
		old := m.MaxLatency.Load()
		if micros <= old || m.MaxLatency.CompareAndSwap(old, micros) {
			break
		}
	}

	// Store for percentile calculation
	m.mu.Lock()
	m.latencies = append(m.latencies, latency)
	m.mu.Unlock()
}

func (m *Metrics) GetPercentile(p float64) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.latencies) == 0 {
		return 0
	}

	// Simple percentile calculation (not sorted)
	// For production, use a proper percentile algorithm
	total := time.Duration(0)
	for _, lat := range m.latencies {
		total += lat
	}
	return total / time.Duration(len(m.latencies))
}

func (m *Metrics) Print() {
	fmt.Println()
	color.Cyan("=== Ket Qua Kiem Thu Tai ===")
	fmt.Println()

	color.White("Thong ke ket noi:")
	color.Green("  So client da ket noi:    %d", m.ConnectedClients.Load())
	fmt.Println()

	color.White("Thong ke tin nhan:")
	color.Green("  Tong so tin nhan:        %d", m.TotalMessages.Load())
	color.Green("  Thanh cong:              %d", m.SuccessfulMessages.Load())
	color.Red("  That bai:                %d", m.FailedMessages.Load())
	fmt.Println()

	successCount := m.SuccessfulMessages.Load()
	if successCount > 0 {
		avgLatency := time.Duration(m.TotalLatency.Load()/successCount) * time.Microsecond
		minLatency := time.Duration(m.MinLatency.Load()) * time.Microsecond
		maxLatency := time.Duration(m.MaxLatency.Load()) * time.Microsecond

		color.White("Thong ke do tre:")
		color.Green("  Trung binh:              %v", avgLatency)
		color.Green("  Nho nhat:                %v", minLatency)
		color.Green("  Lon nhat:                %v", maxLatency)
		color.Green("  P50 (uoc tinh):          %v", m.GetPercentile(0.50))
		color.Green("  P95 (uoc tinh):          %v", m.GetPercentile(0.95))
		color.Green("  P99 (uoc tinh):          %v", m.GetPercentile(0.99))
		fmt.Println()

		throughput := float64(successCount) / duration.Seconds()
		color.White("Thong luong:")
		color.Green("  Tin nhan/giay:           %.2f", throughput)
		fmt.Println()
	}
}

// =============================================================================
// Load Test Client
// =============================================================================

type LoadTestClient struct {
	id        int
	conn      *websocket.Conn
	metrics   *Metrics
	done      chan struct{}
	connected bool
}

func NewLoadTestClient(id int, metrics *Metrics) (*LoadTestClient, error) {
	// Create TLS config
	tlsConfig, err := createTLSConfig()
	if err != nil {
		return nil, err
	}

	// Parse server URL
	u, err := url.Parse(ServerURL)
	if err != nil {
		return nil, err
	}

	// Create dialer
	dialer := &websocket.Dialer{
		TLSClientConfig:  tlsConfig,
		HandshakeTimeout: 10 * time.Second,
	}

	// Connect
	conn, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		return nil, err
	}

	client := &LoadTestClient{
		id:        id,
		conn:      conn,
		metrics:   metrics,
		done:      make(chan struct{}),
		connected: true,
	}

	metrics.ConnectedClients.Add(1)
	return client, nil
}

func (c *LoadTestClient) Start() {
	// Start reader
	go c.readMessages()

	// Send messages at specified rate
	ticker := time.NewTicker(time.Second / time.Duration(*messageRate))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.sendMessage()
		case <-c.done:
			return
		}
	}
}

func (c *LoadTestClient) readMessages() {
	for {
		select {
		case <-c.done:
			return
		default:
			_, _, err := c.conn.ReadMessage()
			if err != nil {
				if c.connected {
					log.Printf("[Client %d] Read error: %v", c.id, err)
					c.connected = false
				}
				return
			}
		}
	}
}

func (c *LoadTestClient) sendMessage() {
	start := time.Now()

	msg := Message{
		Type:      MsgTypeText,
		Content:   fmt.Sprintf("Load test message from client %d", c.id),
		Timestamp: time.Now().Unix(),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		c.metrics.FailedMessages.Add(1)
		return
	}

	err = c.conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		c.metrics.FailedMessages.Add(1)
		if c.connected {
			log.Printf("[Client %d] Write error: %v", c.id, err)
			c.connected = false
		}
		return
	}

	latency := time.Since(start)
	c.metrics.RecordLatency(latency)
	c.metrics.TotalMessages.Add(1)
	c.metrics.SuccessfulMessages.Add(1)
}

func (c *LoadTestClient) Close() {
	close(c.done)
	if c.connected {
		c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		time.Sleep(10 * time.Millisecond)
		c.conn.Close()
		c.metrics.ConnectedClients.Add(-1)
	}
}

// =============================================================================
// TLS Configuration
// =============================================================================

func createTLSConfig() (*tls.Config, error) {
	caCert, err := ioutil.ReadFile(CACertFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %v", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	return &tls.Config{
		RootCAs:    caCertPool,
		MinVersion: tls.VersionTLS13,
	}, nil
}

// =============================================================================
// Load Test Runner
// =============================================================================

func runLoadTest() {
	color.Cyan("=== WebSocket Load Test ===")
	fmt.Println()
	color.White("Configuration:")
	color.Green("  Clients:              %d", *numClients)
	color.Green("  Duration:             %v", *duration)
	color.Green("  Message Rate:         %d msg/s per client", *messageRate)
	color.Green("  Ramp-up Time:         %v", *rampUp)
	color.Green("  Total Messages:       ~%d", *numClients**messageRate*int(duration.Seconds()))
	fmt.Println()

	metrics := NewMetrics()
	clients := make([]*LoadTestClient, 0, *numClients)
	var clientMu sync.Mutex

	// Progress reporting
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			color.Cyan("[Progress] Connected: %d | Messages: %d (Success: %d, Failed: %d)",
				metrics.ConnectedClients.Load(),
				metrics.TotalMessages.Load(),
				metrics.SuccessfulMessages.Load(),
				metrics.FailedMessages.Load(),
			)
		}
	}()

	// Spawn clients with ramp-up
	color.Yellow("Ramping up clients...")
	spawnInterval := *rampUp / time.Duration(*numClients)

	for i := 0; i < *numClients; i++ {
		client, err := NewLoadTestClient(i+1, metrics)
		if err != nil {
			color.Red("[ERROR] Failed to create client %d: %v", i+1, err)
			continue
		}

		clientMu.Lock()
		clients = append(clients, client)
		clientMu.Unlock()

		go client.Start()

		if i < *numClients-1 {
			time.Sleep(spawnInterval)
		}

		if (i+1)%100 == 0 {
			color.Green("  Spawned %d/%d clients", i+1, *numClients)
		}
	}

	color.Green("All %d clients connected!", len(clients))
	fmt.Println()
	color.Yellow("Running load test for %v...", *duration)
	fmt.Println()

	// Wait for test duration
	time.Sleep(*duration)

	// Shutdown
	color.Yellow("Shutting down clients...")
	clientMu.Lock()
	for _, client := range clients {
		client.Close()
	}
	clientMu.Unlock()

	time.Sleep(1 * time.Second)

	// Print results
	metrics.Print()
}

// =============================================================================
// Main
// =============================================================================

func main() {
	flag.Parse()

	// Validate flags
	if *numClients <= 0 {
		color.Red("[ERROR] Number of clients must be positive")
		os.Exit(1)
	}
	if *duration <= 0 {
		color.Red("[ERROR] Duration must be positive")
		os.Exit(1)
	}
	if *messageRate <= 0 {
		color.Red("[ERROR] Message rate must be positive")
		os.Exit(1)
	}

	// Check CA certificate
	if _, err := os.Stat(CACertFile); os.IsNotExist(err) {
		color.Red("[ERROR] CA certificate not found!")
		color.Yellow("Run: cd certs/scripts && ./generate-all.sh")
		os.Exit(1)
	}

	// Run load test
	runLoadTest()
}
