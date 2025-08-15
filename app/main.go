package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	fmt.Println("Redis server listening on port 6379")

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
	for {
		buff := make([]byte, 1024)

		_, err := conn.Read(buff)

		if err != nil {
			if err == io.EOF {
				fmt.Println("client disconnected")
			} else {
				fmt.Println("error reading from client: ", err.Error())
			}
			return
		}
		conn.Write([]byte("+PONG\r\n"))
	}
}
