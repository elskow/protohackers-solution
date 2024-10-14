package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math/bits"
	"net"
	"slices"
	"strconv"
	"strings"
)

type Opcode uint8

const (
	OP_REV    Opcode = 1
	OP_XOR    Opcode = 2
	OP_XORPOS Opcode = 3
	OP_ADD    Opcode = 4
	OP_ADDPOS Opcode = 5
	OP_SUBPOS Opcode = 6
)

type CypherOp struct {
	op  Opcode
	arg uint8
}

func invertCypher(ops []CypherOp) []CypherOp {
	ops = slices.Clone(ops)
	slices.Reverse(ops)
	for i := range ops {
		switch ops[i].op {
		case OP_ADD:
			ops[i].arg = -ops[i].arg
		case OP_ADDPOS:
			ops[i].op = OP_SUBPOS
		}
	}
	return ops
}

func encrypt(ops []CypherOp, pos0 uint8, bytes []uint8) {
	for _, op := range ops {
		switch op.op {
		case OP_REV:
			for i := range bytes {
				bytes[i] = bits.Reverse8(bytes[i])
			}
		case OP_XOR:
			for i := range bytes {
				bytes[i] ^= op.arg
			}
		case OP_XORPOS:
			for i := range bytes {
				bytes[i] ^= pos0 + uint8(i)
			}
		case OP_ADD:
			for i := range bytes {
				bytes[i] += op.arg
			}
		case OP_ADDPOS:
			for i := range bytes {
				bytes[i] += pos0 + uint8(i)
			}
		case OP_SUBPOS:
			for i := range bytes {
				bytes[i] -= pos0 + uint8(i)
			}
		}
	}
}

func isNoOp(ops []CypherOp) bool {
	bytes := make([]uint8, 256)
	for i := range 256 {
		bytes[i] = uint8(i)
	}
	for pos := range 256 {
		testBytes := make([]uint8, 256)
		copy(testBytes, bytes)
		encrypt(ops, uint8(pos), testBytes)
		if !slices.Equal(testBytes, bytes) {
			return false
		}
	}
	return true
}

type StreamCypher struct {
	ops []CypherOp
	pos uint8
}

func (c *StreamCypher) encrypt(msg []byte) {
	encrypt(c.ops, c.pos, msg)
	c.pos += uint8(len(msg))
}

func toyCounter(recv chan []byte, send func([]byte)) {
	var buf []byte
	for chunk := range recv {
		buf = append(buf, chunk...)
		for {
			i := slices.Index(buf, '\n')
			if i == -1 {
				break
			}
			line := string(buf[:i])
			buf = slices.Delete(buf, 0, i+1)

			toys := strings.Split(line, ",")
			var bestToy string
			bestToyCnt := uint64(0)
			for _, toy := range toys {
				toy = strings.TrimSpace(toy)
				i := strings.Index(toy, "x ")
				if i == -1 || i == 0 {
					log.Printf("Invalid toy format: %s", toy)
					continue
				}
				cnt, err := strconv.ParseUint(toy[:i], 10, 64)
				if err != nil {
					log.Printf("Error parsing toy count: %v", err)
					continue
				}
				if cnt > bestToyCnt {
					bestToy = toy
					bestToyCnt = cnt
				}
			}
			if bestToy != "" {
				send([]byte(bestToy + "\n"))
			}
		}
	}
}

func handleConnection(conn net.Conn, vcs *VCS) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	clientAddr := conn.RemoteAddr().String()
	log.Printf("New connection from %s", clientAddr)

	for {
		conn.Write([]byte("READY\n"))
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading from %s: %v", clientAddr, err)
			}
			log.Printf("Connection closed: %s", clientAddr)
			return
		}
		cmd := strings.Fields(strings.TrimSpace(line))
		if len(cmd) == 0 {
			continue
		}

		log.Printf("Received command from %s: %s", clientAddr, strings.Join(cmd, " "))

		switch strings.ToLower(cmd[0]) {
		case "help":
			conn.Write([]byte("OK usage: HELP|GET|PUT|LIST\n"))
		case "list":
			if len(cmd) != 2 {
				conn.Write([]byte("ERR usage: LIST dir\n"))
				continue
			}
			entries, err := vcs.List(cmd[1])
			if err != nil {
				conn.Write([]byte("ERR " + err.Error() + "\n"))
				continue
			}
			conn.Write([]byte(fmt.Sprintf("OK %d\n", len(entries))))
			for _, entry := range entries {
				conn.Write([]byte(entry + "\n"))
			}
		case "get":
			if len(cmd) != 2 && len(cmd) != 3 {
				conn.Write([]byte("ERR usage: GET file [revision]\n"))
				continue
			}
			revision := -1
			if len(cmd) == 3 {
				rev, err := strconv.Atoi(strings.TrimPrefix(cmd[2], "r"))
				if err != nil {
					conn.Write([]byte("ERR invalid revision\n"))
					continue
				}
				revision = rev
			}
			data, err := vcs.Get(cmd[1], revision)
			if err != nil {
				conn.Write([]byte("ERR " + err.Error() + "\n"))
				continue
			}
			conn.Write([]byte(fmt.Sprintf("OK %d\n%s", len(data), data)))
		case "put":
			if len(cmd) != 3 {
				conn.Write([]byte("ERR usage: PUT file length newline data\n"))
				continue
			}
			length, err := strconv.Atoi(cmd[2])
			if err != nil {
				conn.Write([]byte("ERR invalid length\n"))
				continue
			}

			data := make([]byte, length)
			_, err = io.ReadFull(conn, data)
			if err != nil {
				conn.Write([]byte("ERR failed to read data\n"))
				continue
			}

			for _, b := range data {
				if (b < 32 && !slices.Contains([]byte("\t\r\n"), b)) || b >= 127 {
					conn.Write([]byte("ERR non-text character in file\n"))
					continue
				}
			}

			revision, err := vcs.Put(cmd[1], string(data))
			if err != nil {
				conn.Write([]byte("ERR " + err.Error() + "\n"))
				continue
			}
			conn.Write([]byte(fmt.Sprintf("OK r%d\n", revision)))
		default:
			conn.Write([]byte(fmt.Sprintf("ERR illegal method: %s\n", cmd[0])))
		}
	}
}

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
	defer listener.Close()

	log.Println("Listening on :8080")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go handleConnection(conn)
	}
}

type BinReader struct {
	r   net.Conn
	Err error
}

func NewBinReader(r net.Conn) *BinReader {
	return &BinReader{r: r}
}

func (r *BinReader) U8() uint8 {
	if r.Err != nil {
		return 0
	}
	buf := make([]byte, 1)
	_, r.Err = r.r.Read(buf)
	return buf[0]
}
