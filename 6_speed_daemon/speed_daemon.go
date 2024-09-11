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
	observations = make(map[string][]Observation)
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
	cameras[conn] = &Camera{road: road, mile: mile, limit: limit}
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
	dispatchers[conn] = &Dispatcher{roads: roads}
	mu.Unlock()

	log.Printf("Dispatcher connected: roads=%v\n", roads)
}

func handlePlate(conn net.Conn, reader *bufio.Reader) {
	if _, ok := cameras[conn]; !ok {
		sendError(conn, "not a camera")
		return
	}

	plateLen, _ := reader.ReadByte()
	plate := make([]byte, plateLen)
	reader.Read(plate)

	var timestamp uint32
	binary.Read(reader, binary.BigEndian, &timestamp)

	mu.Lock()
	camera := cameras[conn]
	observations[string(plate)] = append(observations[string(plate)], Observation{
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
			for range ticker.C {
				_, err := conn.Write([]byte{MsgHeartbeat})
				if err != nil {
					return
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

	obs := observations[plate]
	if len(obs) < 2 {
		return
	}

	// Sort observations by timestamp
	sort.Slice(obs, func(i, j int) bool {
		return obs[i].timestamp < obs[j].timestamp
	})

	for i := 0; i < len(obs)-1; i++ {
		for j := i + 1; j < len(obs); j++ {
			if obs[i].mile != obs[j].mile && obs[i].timestamp != obs[j].timestamp {
				timeDiff := float64(obs[j].timestamp - obs[i].timestamp)
				distance := float64(obs[j].mile - obs[i].mile)
				speed := (distance / timeDiff) * 3600

				camera := findCameraByRoad(road)
				if camera != nil && speed > float64(camera.limit) {
					ticket := Ticket{
						plate:      plate,
						road:       road,
						mile1:      obs[i].mile,
						timestamp1: obs[i].timestamp,
						mile2:      obs[j].mile,
						timestamp2: obs[j].timestamp,
						speed:      uint16(speed * 100),
					}
					log.Printf("Ticket generated: %+v\n", ticket)
					sendTicket(road, ticket)
					return // Ensure only one ticket per car per day
				}
			}
		}
	}
}

func findCameraByRoad(road uint16) *Camera {
	for _, camera := range cameras {
		if camera.road == road {
			return camera
		}
	}
	return nil
}

func sendTicket(road uint16, ticket Ticket) {
	mu.Lock()
	defer mu.Unlock()

	log.Printf("Attempting to send ticket: %+v\n", ticket)
	for conn, dispatcher := range dispatchers {
		log.Printf("Checking dispatcher: %+v\n", dispatcher)
		for _, r := range dispatcher.roads {
			if r == road {
				log.Printf("Dispatcher found for road %d: %+v\n", road, dispatcher)
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
				if err != nil {
					log.Printf("Error sending ticket to dispatcher: %v\n", err)
				} else {
					log.Printf("Ticket sent to dispatcher: %+v\n", ticket)
				}
				return
			}
		}
	}

	log.Printf("No dispatcher available for road %d, ticket stored: %+v\n", road, ticket)
}
