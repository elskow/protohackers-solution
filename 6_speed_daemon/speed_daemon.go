package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"log"
	"net"
	"sort"
	"sync"
	"time"
)

const (
	// Message types
	MsgError         = 0x10
	MsgPlate         = 0x20
	MsgTicket        = 0x21
	MsgWantHeartbeat = 0x40
	MsgHeartbeat     = 0x41
	MsgIAmCamera     = 0x80
	MsgIAmDispatcher = 0x81

	// Constants
	DayInSeconds = 86400
)

type Camera struct {
	road  uint16
	mile  uint16
	limit uint16
}

type Dispatcher struct {
	roads []uint16
}

type Observation struct {
	plate     string
	timestamp uint32
	mile      uint16
}

type Ticket struct {
	plate      string
	road       uint16
	mile1      uint16
	timestamp1 uint32
	mile2      uint16
	timestamp2 uint32
	speed      uint16
}

var (
	cameras      = make(map[net.Conn]*Camera)
	dispatchers  = make(map[net.Conn]*Dispatcher)
	observations = make(map[uint16]map[string][]Observation) // Map of road to plates to observations
	tickets      = make(map[uint16][]Ticket)                 // Stored tickets per road
	ticketsSent  = make(map[string]map[uint32]bool)          // Track sent tickets per plate per day
	mu           sync.Mutex
)

func main() {
	PORT := ":8080"
	l, err := net.Listen("tcp", PORT)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	log.Printf("Listening on port %s\n", PORT)

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		msgType, err := reader.ReadByte()
		if err != nil {
			log.Println("Error reading message type:", err)
			return
		}

		switch msgType {
		case MsgIAmCamera:
			handleIAmCamera(conn, reader)
		case MsgIAmDispatcher:
			handleIAmDispatcher(conn, reader)
		case MsgPlate:
			handlePlate(conn, reader)
		case MsgWantHeartbeat:
			handleWantHeartbeat(conn, reader)
		default:
			sendError(conn, "illegal msg")
			return
		}
	}
}

func handleIAmCamera(conn net.Conn, reader *bufio.Reader) {
	var road, mile, limit uint16
	binary.Read(reader, binary.BigEndian, &road)
	binary.Read(reader, binary.BigEndian, &mile)
	binary.Read(reader, binary.BigEndian, &limit)

	mu.Lock()
	if _, ok := cameras[conn]; ok {
		mu.Unlock()
		sendError(conn, "already identified")
		return
	}
	if _, ok := dispatchers[conn]; ok {
		mu.Unlock()
		sendError(conn, "already identified as dispatcher")
		return
	}
	cameras[conn] = &Camera{road: road, mile: mile, limit: limit}
	if _, ok := observations[road]; !ok {
		observations[road] = make(map[string][]Observation)
	}
	mu.Unlock()

	log.Printf("Camera connected: road=%d, mile=%d, limit=%d\n", road, mile, limit)
}

func handleIAmDispatcher(conn net.Conn, reader *bufio.Reader) {
	numroads, _ := reader.ReadByte()
	roads := make([]uint16, numroads)
	for i := 0; i < int(numroads); i++ {
		binary.Read(reader, binary.BigEndian, &roads[i])
	}

	mu.Lock()
	if _, ok := dispatchers[conn]; ok {
		mu.Unlock()
		sendError(conn, "already identified")
		return
	}
	if _, ok := cameras[conn]; ok {
		mu.Unlock()
		sendError(conn, "already identified as camera")
		return
	}
	dispatchers[conn] = &Dispatcher{roads: roads}
	mu.Unlock()

	log.Printf("Dispatcher connected: roads=%v\n", roads)

	// Send stored tickets for the dispatcher's roads
	for _, road := range roads {
		sendStoredTickets(road)
	}
}

func handlePlate(conn net.Conn, reader *bufio.Reader) {
	mu.Lock()
	camera, ok := cameras[conn]
	mu.Unlock()
	if !ok {
		sendError(conn, "not a camera")
		return
	}

	plateLen, _ := reader.ReadByte()
	plate := make([]byte, plateLen)
	reader.Read(plate)

	var timestamp uint32
	binary.Read(reader, binary.BigEndian, &timestamp)

	mu.Lock()
	observations[camera.road][string(plate)] = append(observations[camera.road][string(plate)], Observation{
		plate:     string(plate),
		timestamp: timestamp,
		mile:      camera.mile,
	})
	mu.Unlock()

	log.Printf("Plate observed: plate=%s, timestamp=%d, mile=%d\n", plate, timestamp, camera.mile)

	checkSpeed(camera.road, string(plate))
}

func handleWantHeartbeat(conn net.Conn, reader *bufio.Reader) {
	var interval uint32
	binary.Read(reader, binary.BigEndian, &interval)

	if interval > 0 {
		go func() {
			ticker := time.NewTicker(time.Duration(interval) * 100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					_, err := conn.Write([]byte{MsgHeartbeat})
					if err != nil {
						return
					}
				case <-time.After(time.Second):
					if _, err := conn.Write([]byte{}); err != nil {
						return
					}
				}
			}
		}()
	}

	log.Printf("Heartbeat requested: interval=%d deciseconds\n", interval)
}

func sendError(conn net.Conn, msg string) {
	var buf bytes.Buffer
	buf.WriteByte(MsgError)
	buf.WriteByte(byte(len(msg)))
	buf.WriteString(msg)
	conn.Write(buf.Bytes())
	conn.Close()
}

func checkSpeed(road uint16, plate string) {
	mu.Lock()
	defer mu.Unlock()

	obs := observations[road][plate]
	if len(obs) < 2 {
		return
	}

	// Sort observations by timestamp
	sort.Slice(obs, func(i, j int) bool {
		return obs[i].timestamp < obs[j].timestamp
	})

	camera := findCameraByRoad(road)
	if camera == nil {
		return
	}

	// Find all valid ticket possibilities
	var validTickets []Ticket

	for i := 0; i < len(obs)-1; i++ {
		for j := i + 1; j < len(obs); j++ {
			if obs[i].mile != obs[j].mile && obs[i].timestamp != obs[j].timestamp {
				timeDiff := float64(obs[j].timestamp-obs[i].timestamp) / 3600.0 // Convert to hours
				distance := float64(abs(int(obs[j].mile) - int(obs[i].mile)))   // Ensure positive distance
				speed := distance / timeDiff                                    // Speed in miles per hour

				if speed > float64(camera.limit)+0.5 {
					ticket := Ticket{
						plate:      plate,
						road:       road,
						mile1:      obs[i].mile,
						timestamp1: obs[i].timestamp,
						mile2:      obs[j].mile,
						timestamp2: obs[j].timestamp,
						speed:      uint16(speed*100 + 0.5), // Round to nearest int
					}
					validTickets = append(validTickets, ticket)
				}
			}
		}
	}

	// Sort valid tickets by speed, descending
	sort.Slice(validTickets, func(i, j int) bool {
		return validTickets[i].speed > validTickets[j].speed
	})

	// Try to issue tickets, starting with the highest speed
	for _, ticket := range validTickets {
		day1 := ticket.timestamp1 / DayInSeconds
		day2 := ticket.timestamp2 / DayInSeconds
		if !ticketAlreadySent(plate, day1) && !ticketAlreadySent(plate, day2) {
			log.Printf("Ticket generated: plate=%s, road=%d, mile1=%d, timestamp1=%d, mile2=%d, timestamp2=%d, speed=%.2f mph\n",
				ticket.plate, ticket.road, ticket.mile1, ticket.timestamp1, ticket.mile2, ticket.timestamp2, float64(ticket.speed)/100.0)
			if !sendTicket(road, ticket) {
				storeTicket(road, ticket)
			}
			markTicketSent(plate, day1)
			if day1 != day2 {
				markTicketSent(plate, day2)
			}
			break // Only send one ticket per day
		}
	}
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func findCameraByRoad(road uint16) *Camera {
	for _, camera := range cameras {
		if camera.road == road {
			return camera
		}
	}
	return nil
}

func sendTicket(road uint16, ticket Ticket) bool {
	log.Printf("Attempting to send ticket for road %d: %+v\n", road, ticket)
	sent := false
	for conn, dispatcher := range dispatchers {
		for _, r := range dispatcher.roads {
			if r == road {
				var buf bytes.Buffer
				buf.WriteByte(MsgTicket)
				buf.WriteByte(byte(len(ticket.plate)))
				buf.WriteString(ticket.plate)
				binary.Write(&buf, binary.BigEndian, ticket.road)
				binary.Write(&buf, binary.BigEndian, ticket.mile1)
				binary.Write(&buf, binary.BigEndian, ticket.timestamp1)
				binary.Write(&buf, binary.BigEndian, ticket.mile2)
				binary.Write(&buf, binary.BigEndian, ticket.timestamp2)
				binary.Write(&buf, binary.BigEndian, ticket.speed)
				_, err := conn.Write(buf.Bytes())
				if err == nil {
					log.Printf("Ticket sent successfully to dispatcher for road %d: %+v\n", road, ticket)
					sent = true
					break // Successfully sent to one dispatcher, no need to try others
				}
				log.Printf("Error sending ticket to dispatcher for road %d: %v\n", road, err)
			}
		}
		if sent {
			break
		}
	}
	if !sent {
		log.Printf("No dispatcher available for road %d, ticket not sent: %+v\n", road, ticket)
		return false
	}
	return true
}

func storeTicket(road uint16, ticket Ticket) {
	tickets[road] = append(tickets[road], ticket)
	log.Printf("Ticket stored for road %d: %+v\n", road, ticket)
}

func sendStoredTickets(road uint16) {
	mu.Lock()
	storedTickets := tickets[road]
	tickets[road] = nil // Clear stored tickets after sending
	mu.Unlock()

	for _, ticket := range storedTickets {
		if sendTicket(road, ticket) {
			log.Printf("Stored ticket sent for road %d: %+v\n", road, ticket)
		} else {
			storeTicket(road, ticket) // Re-store if sending fails
		}
	}
}

func ticketAlreadySent(plate string, day uint32) bool {
	if _, ok := ticketsSent[plate]; !ok {
		return false
	}
	return ticketsSent[plate][day]
}

func markTicketSent(plate string, day uint32) {
	if _, ok := ticketsSent[plate]; !ok {
		ticketsSent[plate] = make(map[uint32]bool)
	}
	ticketsSent[plate][day] = true
}
