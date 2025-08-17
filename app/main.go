package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
)

var (
	storage = make(map[string]string)
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
		// Launch a goroutine for each client
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Use a bufio.Reader for more controlled reading
	reader := bufio.NewReader(conn)

	for {
		// 1. Read the line that specifies the array length (e.g., "*2\r\n")
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading array line:", err.Error())
			}
			return
		}

		// Clean up the line and check the prefix
		trimmedLine := strings.TrimSuffix(line, "\r\n")
		if trimmedLine[0] != '*' {
			fmt.Println("Expected an array prefix '*'")
			return
		}

		// 2. Parse the number of elements
		numElements, err := strconv.Atoi(trimmedLine[1:])
		if err != nil {
			fmt.Println("Error parsing array length:", err.Error())
			return
		}

		// 3. Loop to read each command element
		command := make([]string, 0, numElements)
		for i := 0; i < numElements; i++ {
			// Read the bulk string length line (e.g., "$4\r\n")
			bulkLenLine, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading bulk string length:", err.Error())
				return
			}
			trimmedBulkLenLine := strings.TrimSuffix(bulkLenLine, "\r\n")
			if trimmedBulkLenLine[0] != '$' {
				fmt.Println("Expected a bulk string prefix '$'")
				return
			}
			bulkLen, err := strconv.Atoi(trimmedBulkLenLine[1:])
			if err != nil {
				fmt.Println("Error parsing bulk string length:", err.Error())
				return
			}

			// Read the actual data (e.g., "ECHO") plus the trailing "\r\n"
			data := make([]byte, bulkLen+2) // +2 for \r\n
			_, err = io.ReadFull(reader, data)
			if err != nil {
				fmt.Println("Error reading bulk string data:", err.Error())
				return
			}

			// Append the cleaned data to our command slice
			command = append(command, string(data[:bulkLen]))
		}

		// 4. Handle the parsed command
		if len(command) > 0 {
			cmd := strings.ToUpper(command[0])
			switch cmd {
			case "PING":
				conn.Write([]byte("+PONG\r\n"))
			case "ECHO":
				if len(command) > 1 {
					arg := command[1]
					// Construct the RESP Bulk String response
					response := fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
					conn.Write([]byte(response))
				}
			case "SET":
				if len(command) >= 3 {
					key := command[1]
					value := command[2]

					mutex.Lock()
					storage[key] = value
					mutex.Unlock()

					conn.Write([]byte("+OK\r\n"))
				} else {
					conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
				}

			case "GET":
				if len(command) >= 2 {
					key := command[1]

					mutex.RLock()
					value, exists := storage[key]
					mutex.RUnlock()

					if exists {
						response := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
						conn.Write([]byte(response))
					} else {
						conn.Write([]byte("$-1\r\n"))
					}
				} else {
					conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
				}
			default:
				conn.Write([]byte("-ERR unknown command\r\n"))
			}
		}
	}
}
