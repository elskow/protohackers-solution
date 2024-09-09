package main

import (
	"bufio"
	"encoding/json"
	"log"
	"net"
	"os"
)

func isPrime(n int) bool {
	if n <= 1 {
		return false
	}
	for i := 2; i*i <= n; i++ {
		if n%i == 0 {
			return false
		}
	}
	return true
}

func main() {
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Println("Error starting server:", err)
		os.Exit(1)
	}
	defer ln.Close()
	log.Println("Server started on :8080")
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}
		log.Println("Accepted connection from", conn.RemoteAddr())
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			log.Println("Error reading from connection:", err)
			return
		}
		log.Println("Received request:", string(line))
		var request map[string]interface{}
		err = json.Unmarshal(line, &request)
		if err != nil {
			log.Println("Malformed request:", string(line))
			writer.Write([]byte(`{"error":"malformed request"}` + "\n"))
			writer.Flush()
			return
		}
		method, methodOk := request["method"].(string)
		number, numberOk := request["number"].(float64)
		if !methodOk || method != "isPrime" || !numberOk {
			log.Println("Invalid request:", request)
			writer.Write([]byte(`{"error":"invalid request"}` + "\n"))
			writer.Flush()
			return
		}
		isPrime := isPrime(int(number))
		response := map[string]interface{}{
			"method": "isPrime",
			"prime":  isPrime,
		}
		responseBytes, err := json.Marshal(response)
		if err != nil {
			log.Println("Error marshalling response:", err)
			return
		}
		log.Println("Sending response:", string(responseBytes))
		writer.Write(responseBytes)
		writer.Write([]byte("\n"))
		writer.Flush()
	}
}
