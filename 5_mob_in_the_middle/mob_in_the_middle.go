package main

import (
	"bufio"
	"io"
	"log"
	"net"
	"regexp"
	"strings"
)

const (
	tonysAddress   = "7YWHMfk9JZe0LM0g1ZauHuiSxhI"
	serverEndpoint = "chat.protohackers.com:16963"
)

func main() {
	PORT := ":8080"
	l, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Println(err)
		return
	}
	defer l.Close()

	log.Printf("Listening on port %s\n", PORT)

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println(err)
			continue
		}

		proxy := createProxyConnection(conn)
		go proxy.handleConnections()
	}
}

type ProxyConnection struct {
	client     net.Conn
	clientData chan string
	server     net.Conn
	serverData chan string
}

func (proxy *ProxyConnection) Close() {
	log.Printf("Closing connections: client %s, server %s\n", proxy.client.RemoteAddr(), proxy.server.RemoteAddr())
	proxy.server.Close()
	proxy.client.Close()
}

func (proxy *ProxyConnection) receiveFromServer() {
	reader := bufio.NewReader(proxy.server)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading from server: %s\n", err)
			}
			break
		}
		log.Printf("Received from server: %s\n", strings.TrimSpace(message))
		proxy.serverData <- strings.TrimSpace(message)
	}
	close(proxy.serverData)
}

func (proxy *ProxyConnection) receiveFromClient() {
	reader := bufio.NewReader(proxy.client)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading from client: %s\n", err)
			}
			break
		}
		log.Printf("Received from client: %s\n", strings.TrimSpace(message))
		proxy.clientData <- strings.TrimSpace(message)
	}
	close(proxy.clientData)
}

func createProxyConnection(client net.Conn) *ProxyConnection {
	upstream, err := net.Dial("tcp", serverEndpoint)
	if err != nil {
		log.Println("Could not connect to server endpoint.")
		panic(err)
	}

	log.Printf("Client connected: %s\n", client.RemoteAddr())
	log.Printf("Connected to upstream server: %s\n", upstream.RemoteAddr())

	proxy := ProxyConnection{
		client:     client,
		clientData: make(chan string, 100), // Buffered channel to reduce blocking
		server:     upstream,
		serverData: make(chan string, 100), // Buffered channel to reduce blocking
	}

	go proxy.receiveFromServer()
	go proxy.receiveFromClient()

	return &proxy
}

func (proxy *ProxyConnection) handleConnections() {
	clientWriter := bufio.NewWriter(proxy.client)
	serverWriter := bufio.NewWriter(proxy.server)

	for {
		select {
		case clientData := <-proxy.clientData:
			if clientData == "" {
				goto disconnect
			}
			log.Printf("Forwarding to server: %s\n", clientData)
			serverWriter.WriteString(rewrite(clientData) + "\n")
			serverWriter.Flush()
		case serverData := <-proxy.serverData:
			if serverData == "" {
				goto disconnect
			}
			log.Printf("Forwarding to client: %s\n", serverData)
			clientWriter.WriteString(rewrite(serverData) + "\n")
			clientWriter.Flush()
		}
	}
disconnect:
	proxy.Close()
}

func rewrite(message string) string {
	bogusRx := regexp.MustCompile(`^7[0-9a-zA-Z]{25,34}$`)

	s := strings.Split(message, " ")
	for i, word := range s {
		if bogusRx.MatchString(word) {
			log.Printf("Rewriting boguscoin address: %s -> %s\n", word, tonysAddress)
			s[i] = tonysAddress
		}
	}

	return strings.Join(s, " ")
}
