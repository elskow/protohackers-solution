package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

type Client struct {
	conn net.Conn
	name string
}

var (
	clients   = make(map[net.Conn]*Client)
	clientsMu sync.Mutex
	broadcast = make(chan Message)
	join      = make(chan *Client)
	leave     = make(chan *Client)
)

type Message struct {
	sender  *Client
	content string
}

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
	defer listener.Close()
	log.Println("Server started on :8080")

	go handleBroadcasts()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		log.Printf("Accepted connection from %s", conn.RemoteAddr())
		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	defer func() {
		conn.Close()
		log.Printf("Closed connection from %s", conn.RemoteAddr())
	}()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	writer.WriteString("Welcome to budgetchat! What shall I call you?\n")
	writer.Flush()

	name, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Error reading name from %s: %v", conn.RemoteAddr(), err)
		return
	}
	name = strings.TrimSpace(name)

	if !isValidName(name) {
		writer.WriteString("Invalid name. Disconnecting.\n")
		writer.Flush()
		log.Printf("Invalid name from %s: %s", conn.RemoteAddr(), name)
		return
	}

	client := &Client{conn: conn, name: name}
	join <- client

	writer.WriteString(fmt.Sprintf("* The room contains: %s\n", getUserList(client)))
	writer.Flush()
	log.Printf("%s joined the chat with name: %s", conn.RemoteAddr(), name)

	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			leave <- client
			log.Printf("%s disconnected", conn.RemoteAddr())
			return
		}
		message = strings.TrimSpace(message)
		if message != "" {
			log.Printf("Received message from %s: %s", name, message)
			broadcast <- Message{sender: client, content: fmt.Sprintf("[%s] %s", client.name, message)}
		}
	}
}

func handleBroadcasts() {
	for {
		select {
		case msg := <-broadcast:
			clientsMu.Lock()
			for _, client := range clients {
				if client != msg.sender {
					client.conn.Write([]byte(msg.content + "\n"))
				}
			}
			clientsMu.Unlock()
			log.Printf("Broadcasted message: %s", msg.content)
		case client := <-join:
			clientsMu.Lock()
			clients[client.conn] = client
			for _, c := range clients {
				if c != client {
					c.conn.Write([]byte(fmt.Sprintf("* %s has entered the room\n", client.name)))
				}
			}
			clientsMu.Unlock()
			log.Printf("%s has entered the room", client.name)
		case client := <-leave:
			clientsMu.Lock()
			delete(clients, client.conn)
			for _, c := range clients {
				c.conn.Write([]byte(fmt.Sprintf("* %s has left the room\n", client.name)))
			}
			clientsMu.Unlock()
			log.Printf("%s has left the room", client.name)
		}
	}
}

func isValidName(name string) bool {
	if len(name) < 1 || len(name) > 16 {
		return false
	}
	for _, r := range name {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return true
}

func getUserList(exclude *Client) string {
	clientsMu.Lock()
	defer clientsMu.Unlock()
	names := []string{}
	for _, client := range clients {
		if client != exclude {
			names = append(names, client.name)
		}
	}
	return strings.Join(names, ", ")
}
