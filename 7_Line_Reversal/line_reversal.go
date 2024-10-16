package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxPacketSize     = 1000
	retransTimeout    = 3 * time.Second
	sessionExpireTime = 10 * time.Second
	maxChunkSize      = 480
)

type Session struct {
	addr       *net.UDPAddr
	recvBuffer []byte
	sendBuffer []byte
	acked      int
	received   int
	lastActive time.Time
	mu         sync.Mutex
}

var (
	sessions   = make(map[int]*Session)
	sessionsMu sync.Mutex
	unescaper  = strings.NewReplacer("\\\\", "\\", "\\/", "/")
	escaper    = strings.NewReplacer("\\", "\\\\", "/", "\\/")
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)

	addr, err := net.ResolveUDPAddr("udp", ":8080")
	if err != nil {
		log.Fatal("Failed to resolve address:", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal("Failed to listen on UDP:", err)
	}
	defer conn.Close()

	log.Println("LRCP server listening on :8080")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			cleanupSessions()
		}
	}()

	for {
		handlePacket(conn)
	}
}

func cleanupSessions() {
	sessionsMu.Lock()
	defer sessionsMu.Unlock()

	now := time.Now()
	for id, session := range sessions {
		if now.Sub(session.lastActive) > sessionExpireTime {
			log.Printf("Session %d expired", id)
			delete(sessions, id)
		}
	}
}

func handlePacket(conn *net.UDPConn) {
	buffer := make([]byte, maxPacketSize)
	conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	n, remoteAddr, err := conn.ReadFromUDP(buffer)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return // This is just a timeout, not an error
		}
		log.Println("Error reading UDP packet:", err)
		return
	}

	msg := buffer[:n]
	log.Printf("Received packet from %v: %q", remoteAddr, msg)

	if len(msg) < 2 || msg[0] != '/' || msg[len(msg)-1] != '/' {
		log.Println("Invalid packet format, ignoring")
		return
	}

	parts := splitEscaped(msg[1 : len(msg)-1])
	if len(parts) < 2 {
		log.Println("Invalid packet structure, ignoring")
		return
	}

	sessionID, err := strconv.Atoi(string(parts[1]))
	if err != nil {
		log.Println("Invalid session ID:", string(parts[1]))
		return
	}

	sessionsMu.Lock()
	session, exists := sessions[sessionID]
	if !exists {
		log.Printf("Creating new session: %d", sessionID)
		session = &Session{addr: remoteAddr, lastActive: time.Now()}
		sessions[sessionID] = session
	}
	sessionsMu.Unlock()

	session.mu.Lock()
	defer session.mu.Unlock()

	switch string(parts[0]) {
	case "connect":
		log.Printf("Received connect for session %d", sessionID)
		sendPacket(conn, remoteAddr, fmt.Sprintf("/ack/%d/0/", sessionID))
	case "data":
		if len(parts) != 4 {
			log.Println("Invalid data packet structure")
			return
		}
		pos, _ := strconv.Atoi(string(parts[2]))
		data := unescaper.Replace(string(parts[3]))
		log.Printf("Received data for session %d, pos: %d, len: %d", sessionID, pos, len(data))
		handleData(conn, session, sessionID, pos, []byte(data))
	case "ack":
		if len(parts) != 3 {
			log.Println("Invalid ack packet structure")
			return
		}
		length, _ := strconv.Atoi(string(parts[2]))
		log.Printf("Received ack for session %d, length: %d", sessionID, length)
		handleAck(conn, session, sessionID, length)
	case "close":
		log.Printf("Received close for session %d", sessionID)
		handleClose(conn, remoteAddr, sessionID)
	default:
		log.Printf("Unknown message type: %s", string(parts[0]))
	}

	session.lastActive = time.Now()
}

func splitEscaped(data []byte) [][]byte {
	var parts [][]byte
	var current []byte
	escaped := false

	for _, b := range data {
		if escaped {
			current = append(current, b)
			escaped = false
		} else if b == '\\' {
			escaped = true
		} else if b == '/' {
			if len(current) > 0 {
				parts = append(parts, current)
				current = []byte{}
			}
		} else {
			current = append(current, b)
		}
	}

	if len(current) > 0 {
		parts = append(parts, current)
	}

	return parts
}

func handleData(conn *net.UDPConn, session *Session, sessionID, pos int, data []byte) {
	if pos > session.received {
		log.Printf("Data out of order for session %d. Expected: %d, Got: %d", sessionID, session.received, pos)
		sendPacket(conn, session.addr, fmt.Sprintf("/ack/%d/%d/", sessionID, session.received))
		return
	}

	if pos+len(data) > session.received {
		newData := data[session.received-pos:]
		session.recvBuffer = append(session.recvBuffer, newData...)
		session.received += len(newData)
		log.Printf("Processed %d new bytes for session %d", len(newData), sessionID)
		processLines(conn, session, sessionID)
	}

	sendPacket(conn, session.addr, fmt.Sprintf("/ack/%d/%d/", sessionID, session.received))

	go func() {
		time.Sleep(retransTimeout)
		session.mu.Lock()
		defer session.mu.Unlock()
		if session.received > session.acked {
			log.Printf("Retransmitting data for session %d", sessionID)
			sendData(conn, session, sessionID)
		}
	}()
}

func handleAck(conn *net.UDPConn, session *Session, sessionID, length int) {
	if length > session.acked {
		log.Printf("Acknowledging %d bytes for session %d", length-session.acked, sessionID)

		if length > session.acked+len(session.sendBuffer) {
			log.Printf("Warning: Client acknowledged more data than sent for session %d. Closing session.", sessionID)
			handleClose(conn, session.addr, sessionID)
			return
		}

		if length-session.acked <= len(session.sendBuffer) {
			session.sendBuffer = session.sendBuffer[length-session.acked:]
		} else {
			session.sendBuffer = []byte{}
		}
		session.acked = length
	}

	if len(session.sendBuffer) > 0 {
		log.Printf("Sending remaining %d bytes for session %d", len(session.sendBuffer), sessionID)
		sendData(conn, session, sessionID)
	}
}

func handleClose(conn *net.UDPConn, addr *net.UDPAddr, sessionID int) {
	sessionsMu.Lock()
	defer sessionsMu.Unlock()

	if _, exists := sessions[sessionID]; exists {
		delete(sessions, sessionID)
		log.Printf("Closed session %d", sessionID)
		sendPacket(conn, addr, fmt.Sprintf("/close/%d/", sessionID))
	}
}

func processLines(conn *net.UDPConn, session *Session, sessionID int) {
	lines := 0
	for {
		idx := bytes.IndexByte(session.recvBuffer, '\n')
		if idx == -1 {
			break
		}

		line := session.recvBuffer[:idx]
		session.recvBuffer = session.recvBuffer[idx+1:]

		reversedLine := reverseBytes(line)
		reversedLine = append(reversedLine, '\n')
		session.sendBuffer = append(session.sendBuffer, reversedLine...)
		lines++
	}

	if lines > 0 {
		log.Printf("Processed %d lines for session %d", lines, sessionID)
		sendData(conn, session, sessionID)
	}
}

func sendData(conn *net.UDPConn, session *Session, sessionID int) {
	chunks := 0
	for off := 0; off < len(session.sendBuffer); off += maxChunkSize {
		end := off + maxChunkSize
		if end > len(session.sendBuffer) {
			end = len(session.sendBuffer)
		}
		chunk := session.sendBuffer[off:end]
		sendPacket(conn, session.addr, fmt.Sprintf("/data/%d/%d/%s/", sessionID, session.acked+off, escaper.Replace(string(chunk))))
		chunks++
	}
	log.Printf("Sent %d chunks for session %d", chunks, sessionID)
}

func sendPacket(conn *net.UDPConn, addr *net.UDPAddr, message string) {
	_, err := conn.WriteToUDP([]byte(message), addr)
	if err != nil {
		log.Println("Error sending UDP packet:", err)
	} else {
		log.Printf("Sent packet to %v: %q", addr, message)
	}
}

func reverseBytes(s []byte) []byte {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
	return s
}
