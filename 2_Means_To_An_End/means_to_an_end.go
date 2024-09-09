package main

import (
	"encoding/binary"
	"io"
	"log"
	"net"
	"sync"
)

type PriceData struct {
	timestamp int32
	price     int32
}

type ClientData struct {
	mu     sync.Mutex
	prices []PriceData
}

var clients = make(map[net.Conn]*ClientData)
var clientsMu sync.Mutex

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
	defer listener.Close()
	log.Println("Server started on :8080")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		log.Printf("Accepted connection from %s", conn.RemoteAddr())
		clientsMu.Lock()
		clients[conn] = &ClientData{}
		clientsMu.Unlock()
		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	defer func() {
		clientsMu.Lock()
		delete(clients, conn)
		clientsMu.Unlock()
		conn.Close()
		log.Printf("Closed connection from %s", conn.RemoteAddr())
	}()

	for {
		message := make([]byte, 9)
		_, err := io.ReadFull(conn, message)
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading from connection: %v", err)
			}
			return
		}

		log.Printf("Received message from %s: %v", conn.RemoteAddr(), message)

		switch message[0] {
		case 'I':
			handleInsert(conn, message[1:])
		case 'Q':
			handleQuery(conn, message[1:])
		default:
			log.Printf("Undefined behavior for message: %v", message)
			return
		}
	}
}

func handleInsert(conn net.Conn, data []byte) {
	timestamp := int32(binary.BigEndian.Uint32(data[:4]))
	price := int32(binary.BigEndian.Uint32(data[4:]))

	log.Printf("Handling insert from %s: timestamp=%d, price=%d", conn.RemoteAddr(), timestamp, price)

	clientsMu.Lock()
	clientData := clients[conn]
	clientsMu.Unlock()

	clientData.mu.Lock()
	clientData.prices = append(clientData.prices, PriceData{timestamp, price})
	clientData.mu.Unlock()
}

func handleQuery(conn net.Conn, data []byte) {
	mintime := int32(binary.BigEndian.Uint32(data[:4]))
	maxtime := int32(binary.BigEndian.Uint32(data[4:]))

	log.Printf("Handling query from %s: mintime=%d, maxtime=%d", conn.RemoteAddr(), mintime, maxtime)

	if mintime > maxtime {
		log.Printf("Invalid query from %s: mintime > maxtime", conn.RemoteAddr())
		sendResponse(conn, 0)
		return
	}

	clientsMu.Lock()
	clientData := clients[conn]
	clientsMu.Unlock()

	clientData.mu.Lock()
	defer clientData.mu.Unlock()

	var sum int64
	var count int64
	for _, priceData := range clientData.prices {
		if priceData.timestamp >= mintime && priceData.timestamp <= maxtime {
			sum += int64(priceData.price)
			count++
		}
	}

	var mean int32
	if count == 0 {
		mean = 0
	} else {
		mean = int32(sum / count)
	}

	log.Printf("Query result for %s: mean=%d", conn.RemoteAddr(), mean)
	sendResponse(conn, mean)
}

func sendResponse(conn net.Conn, response int32) {
	respBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(respBytes, uint32(response))
	_, err := conn.Write(respBytes)
	if err != nil {
		log.Printf("Error sending response to %s: %v", conn.RemoteAddr(), err)
	} else {
		log.Printf("Sent response to %s: %d", conn.RemoteAddr(), response)
	}
}
