package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
)

type VCS struct {
	mu   sync.RWMutex
	dirs map[string]map[string][]string // dir path -> file name -> revisions
}

func NewVCS() *VCS {
	return &VCS{
		dirs: make(map[string]map[string][]string),
	}
}

func parsePath(path string) []string {
	if path == "" || path[0] != '/' {
		return nil
	}
	if path == "/" {
		return []string{}
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for _, part := range parts {
		if part == "" {
			return nil
		}
		for _, c := range part {
			if !strings.ContainsRune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-", c) {
				return nil
			}
		}
	}
	return parts
}

func parseDirPath(path string) (string, bool) {
	if len(path) > 1 {
		path = strings.TrimSuffix(path, "/")
	}
	parts := parsePath(path)
	if parts == nil {
		return "", false
	}
	return strings.Join(parts, "/"), true
}

func parseFilePath(path string) (string, string, bool) {
	parts := parsePath(path)
	if len(parts) == 0 {
		return "", "", false
	}
	i := len(parts) - 1
	return strings.Join(parts[:i], "/"), parts[i], true
}

func (vcs *VCS) Put(path string, data string) (int, error) {
	dirPath, fileName, ok := parseFilePath(path)
	if !ok {
		return 0, fmt.Errorf("illegal file name")
	}

	vcs.mu.Lock()
	defer vcs.mu.Unlock()

	if _, ok := vcs.dirs[dirPath]; !ok {
		vcs.dirs[dirPath] = make(map[string][]string)
	}
	dir := vcs.dirs[dirPath]
	if _, ok := dir[fileName]; !ok {
		dir[fileName] = make([]string, 0, 1)
	}
	revs := dir[fileName]
	if len(revs) == 0 || revs[len(revs)-1] != data {
		revs = append(revs, data)
		dir[fileName] = revs
	}
	return len(revs), nil
}

func (vcs *VCS) Get(path string, revision int) (string, error) {
	dirPath, fileName, ok := parseFilePath(path)
	if !ok {
		return "", fmt.Errorf("illegal file name")
	}

	vcs.mu.RLock()
	defer vcs.mu.RUnlock()

	dir, ok := vcs.dirs[dirPath]
	if !ok {
		return "", fmt.Errorf("no such file")
	}
	revs, ok := dir[fileName]
	if !ok {
		return "", fmt.Errorf("no such file")
	}
	if revision == -1 {
		revision = len(revs)
	}
	if revision < 1 || revision > len(revs) {
		return "", fmt.Errorf("no such revision")
	}
	return revs[revision-1], nil
}

func (vcs *VCS) List(dirPath string) ([]string, error) {
	path, ok := parseDirPath(dirPath)
	if !ok {
		return nil, fmt.Errorf("illegal dir name")
	}

	vcs.mu.RLock()
	defer vcs.mu.RUnlock()

	entries := make(map[string]string)

	dir, ok := vcs.dirs[path]
	if ok {
		for filename, revs := range dir {
			entries[filename] = fmt.Sprintf("r%d", len(revs))
		}
	}
	var prefix string
	if len(path) > 0 {
		prefix = path + "/"
	}
	for dirPath := range vcs.dirs {
		if !strings.HasPrefix(dirPath, prefix) || dirPath == path {
			continue
		}
		dirName := dirPath[len(prefix):]
		if i := strings.Index(dirName, "/"); i != -1 {
			dirName = dirName[:i]
		}
		if _, ok := entries[dirName]; !ok {
			entries[dirName+"/"] = "DIR"
		}
	}

	result := make([]string, 0, len(entries))
	for name, desc := range entries {
		result = append(result, fmt.Sprintf("%s %s", name, desc))
	}
	slices.Sort(result)
	return result, nil
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
			if revision == -1 {
				data, err := vcs.Get(cmd[1], -1)
				if err != nil {
					if err.Error() == "no such file" {
						conn.Write([]byte("ERR no such file\n"))
					} else {
						conn.Write([]byte("ERR " + err.Error() + "\n"))
					}
					continue
				}
				conn.Write([]byte(fmt.Sprintf("OK %d\n%s", len(data), data)))
			} else {
				data, err := vcs.Get(cmd[1], revision)
				if err != nil {
					if err.Error() == "no such revision" {
						conn.Write([]byte("ERR no such revision\n"))
					} else if err.Error() == "no such file" {
						conn.Write([]byte("ERR no such file\n"))
					} else {
						conn.Write([]byte("ERR " + err.Error() + "\n"))
					}
					continue
				}
				conn.Write([]byte(fmt.Sprintf("OK %d\n%s", len(data), data)))
			}
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
			_, err = io.ReadFull(reader, data)
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
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	log.Println("Starting VCS server...")

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
	defer listener.Close()

	vcs := NewVCS()

	log.Println("VCS server listening on :8080")
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go handleConnection(conn, vcs)
	}
}
