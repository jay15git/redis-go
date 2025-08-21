package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// storageEntry holds the value and an optional expiry time.
// A zero value for expiresAt means the key never expires.
type storageEntry struct {
	value     string
	expiresAt time.Time
}

var (
	// The storage map now holds storageEntry structs instead of just strings.
	storage = make(map[string]storageEntry)
	mutex   = sync.RWMutex{}
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		return
	}
	defer l.Close()

	fmt.Println("Server is listening on port 6379")

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		// This block contains the simplified RESP parser from your original code.
		// It reads the array and bulk string prefixes to build a command slice.
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading array line:", err.Error())
			}
			return
		}
		trimmedLine := strings.TrimSuffix(line, "\r\n")
		if len(trimmedLine) == 0 || trimmedLine[0] != '*' {
			// Malformed command, wait for next one
			continue
		}
		numElements, err := strconv.Atoi(trimmedLine[1:])
		if err != nil {
			// Malformed command, wait for next one
			continue
		}
		command := make([]string, 0, numElements)
		for i := 0; i < numElements; i++ {
			bulkLenLine, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading bulk string length:", err.Error())
				return
			}
			trimmedBulkLenLine := strings.TrimSuffix(bulkLenLine, "\r\n")
			if len(trimmedBulkLenLine) == 0 || trimmedBulkLenLine[0] != '$' {
				// Malformed command, wait for next one
				break
			}
			bulkLen, err := strconv.Atoi(trimmedBulkLenLine[1:])
			if err != nil {
				// Malformed command, wait for next one
				break
			}
			data := make([]byte, bulkLen+2)
			_, err = io.ReadFull(reader, data)
			if err != nil {
				fmt.Println("Error reading bulk string data:", err.Error())
				return
			}
			command = append(command, string(data[:bulkLen]))
		}

		if len(command) > 0 {
			cmd := strings.ToUpper(command[0])
			switch cmd {
			case "PING":
				conn.Write([]byte("+PONG\r\n"))
			case "ECHO":
				if len(command) > 1 {
					arg := command[1]
					response := fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
					conn.Write([]byte(response))
				}
			case "SET":
				if len(command) >= 3 {
					key := command[1]
					value := command[2]
					var expiryTime time.Time // Zero value means no expiry

					// Check for the optional 'PX' argument
					if len(command) == 5 && strings.ToUpper(command[3]) == "PX" {
						// Parse the millisecond value
						ms, err := strconv.ParseInt(command[4], 10, 64)
						if err == nil {
							// Calculate the absolute expiry time from now
							expiryDuration := time.Duration(ms) * time.Millisecond
							expiryTime = time.Now().Add(expiryDuration)
						}
					}

					mutex.Lock()
					storage[key] = storageEntry{
						value:     value,
						expiresAt: expiryTime,
					}
					mutex.Unlock()

					conn.Write([]byte("+OK\r\n"))
				} else {
					conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
				}

			case "GET":
				if len(command) >= 2 {
					key := command[1]

					mutex.RLock()
					entry, exists := storage[key]
					mutex.RUnlock()

					if !exists {
						conn.Write([]byte("$-1\r\n")) // Null bulk string for non-existent key
						continue
					}

					// **CORRECTED LOGIC STARTS HERE**
					// Check if an expiry is set and if it has passed
					if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
						// Key has expired. Passively delete it and return null.
						mutex.Lock()
						delete(storage, key)
						mutex.Unlock()
						conn.Write([]byte("$-1\r\n")) // Null bulk string for expired key
						continue
					}

					// Key exists and is not expired, return the value from the struct
					response := fmt.Sprintf("$%d\r\n%s\r\n", len(entry.value), entry.value)
					conn.Write([]byte(response))
				} else {
					conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
				}
			default:
				conn.Write([]byte("-ERR unknown command\r\n"))
			}
		}
	}
}
