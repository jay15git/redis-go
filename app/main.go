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

// DataType is an enum to represent our different value types.
type DataType int

const (
	String DataType = iota // Value will be 0
	List                   // Value will be 1
)

// storageEntry holds the data for a String type, including its expiry.
type storageEntry struct {
	value     string
	expiresAt time.Time
}

// Value is the unified struct that can hold any of our Redis data types.
// This is stored in our main map.
type Value struct {
	dataType DataType
	strData  *storageEntry // Pointer to a string entry
	listData []string      // Slice for a list
}

var (
	// CORRECTED: The storage is now a map from a key to our unified Value struct.
	storage = make(map[string]Value)
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
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading array line:", err.Error())
			}
			return
		}
		trimmedLine := strings.TrimSuffix(line, "\r\n")
		if len(trimmedLine) == 0 || trimmedLine[0] != '*' {
			continue
		}
		numElements, err := strconv.Atoi(trimmedLine[1:])
		if err != nil {
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
				break
			}
			bulkLen, err := strconv.Atoi(trimmedBulkLenLine[1:])
			if err != nil {
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
					var expiryTime time.Time

					if len(command) == 5 && strings.ToUpper(command[3]) == "PX" {
						ms, err := strconv.ParseInt(command[4], 10, 64)
						if err == nil {
							expiryDuration := time.Duration(ms) * time.Millisecond
							expiryTime = time.Now().Add(expiryDuration)
						}
					}

					mutex.Lock()
					// CORRECTED: Create the string entry, then wrap it in our main Value struct.
					storage[key] = Value{
						dataType: String,
						strData: &storageEntry{
							value:     value,
							expiresAt: expiryTime,
						},
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
						conn.Write([]byte("$-1\r\n"))
						continue
					}

					// CORRECTED: Use a switch on the data type for type-safe access.
					switch entry.dataType {
					case String:
						// Check for expiry on the string data.
						if !entry.strData.expiresAt.IsZero() && time.Now().After(entry.strData.expiresAt) {
							mutex.Lock()
							delete(storage, key)
							mutex.Unlock()
							conn.Write([]byte("$-1\r\n"))
						} else {
							// Return the string value.
							response := fmt.Sprintf("$%d\r\n%s\r\n", len(entry.strData.value), entry.strData.value)
							conn.Write([]byte(response))
						}
					default:
						// Key exists but holds the wrong type (e.g., a list).
						conn.Write([]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
					}
				} else {
					conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
				}
			case "RPUSH":
				if len(command) >= 3 {
					key := command[1]
					element := command[2]

					mutex.Lock()

					newList := []string{element}
					storage[key] = Value{
						dataType: List,
						listData: newList,
					}
					mutex.Unlock()

					response := fmt.Sprintf(":%d\r\n", len(newList))
					conn.Write([]byte(response))
				} else {
					conn.Write([]byte("-ERR wrong number of arguments for 'rpush' command\r\n"))
				}
			default:
				conn.Write([]byte("-ERR unknown command\r\n"))
			}
		}
	}
}
