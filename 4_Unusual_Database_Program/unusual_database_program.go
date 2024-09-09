package main

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

const (
	maxPacketSize = 1000
	versionInfo   = "K/V Store v1.0"
)

var (
	store = make(map[string]string)
	mu    sync.Mutex
)

func main() {
	addr := net.UDPAddr{
		Port: 8080,
		IP:   net.ParseIP("0.0.0.0"),
	}
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		log.Fatalf("Error starting server: %v", err)
		return
	}
	defer conn.Close()
	log.Println("Server started on port 8080")

	buf := make([]byte, maxPacketSize)
	for {
		n, clientAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("Error reading from UDP: %v", err)
			continue
		}
		log.Printf("Received request from %s: %s", clientAddr, string(buf[:n]))
		go handleRequest(conn, clientAddr, buf[:n])
	}
}

func handleRequest(conn *net.UDPConn, clientAddr *net.UDPAddr, data []byte) {
	request := string(data)
	if strings.Contains(request, "=") {
		handleInsert(request)
	} else {
		handleRetrieve(conn, clientAddr, request)
	}
}

func handleInsert(request string) {
	parts := strings.SplitN(request, "=", 2)
	key := parts[0]
	value := parts[1]

	mu.Lock()
	defer mu.Unlock()

	if key != "version" {
		store[key] = value
		log.Printf("Inserted key: %s, value: %s", key, value)
	} else {
		log.Printf("Ignored attempt to modify special key: %s", key)
	}
}

func handleRetrieve(conn *net.UDPConn, clientAddr *net.UDPAddr, key string) {
	mu.Lock()
	defer mu.Unlock()

	var response string
	if key == "version" {
		response = fmt.Sprintf("version=%s", versionInfo)
	} else {
		value, exists := store[key]
		if exists {
			response = fmt.Sprintf("%s=%s", key, value)
		} else {
			response = fmt.Sprintf("%s=", key)
		}
	}

	if len(response) < maxPacketSize {
		conn.WriteToUDP([]byte(response), clientAddr)
		log.Printf("Sent response to %s: %s", clientAddr, response)
	} else {
		log.Printf("Response too large to send to %s: %s", clientAddr, response)
	}
}
