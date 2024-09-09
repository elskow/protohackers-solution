package main

import (
	"io"
	"log"
	"net"
)

func main() {
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
	defer ln.Close()
	log.Println("Server started on :8080")

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		log.Println("Accepted new connection")

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		log.Println("Closed connection")
	}()

	_, err := io.Copy(conn, conn)
	if err != nil {
		log.Printf("Error echoing data: %v", err)
	}
}
