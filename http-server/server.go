package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
)

// Ensures gofmt doesn't remove the "net" and "os" imports above (feel free to remove this!)
var (
	_ = net.Listen
	_ = os.Exit
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:4221")
	if err != nil {
		fmt.Println("Failed to bind to port 4221")
		os.Exit(1)
	}
	defer l.Close()
	for {
		conn, err := l.Accept() // if out will handle only one conncetion, and concurr just for that one code.
		if err != nil {
			fmt.Printf("error accepting connecton", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	requestBuffer := make([]byte, 1024)
	n, err := conn.Read(requestBuffer)
	if err != nil {
		fmt.Printf("Error reading request", err)
		return
	}
	request := string(requestBuffer[:n])
	method := strings.Split(request, " ")[0]
	path := strings.Split(request, " ")[1]
	lines := strings.Split(request, "\r\n")
	headers := make(map[string]string)
	bodyStart := 0
	for i, line := range lines[1:] {
		if line == "" {
			bodyStart = i + 2
			break
		}
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) == 2 {
			headers[parts[0]] = parts[1]
		}
	}
	user_agent := headers["User-Agent"]
	var body string
	if bodyStart < len(lines) {
		body = strings.Join(lines[bodyStart:], "\r\n")
	}
	switch method {
	case "POST":
		switch {
		case strings.HasPrefix(path, "/files/"):
			dir := os.Args[2]
			filename := strings.Split(path, "/")[2]
			filePath := filepath.Join(dir, filename)
			err := os.WriteFile(filePath, []byte(body), 0o644)
			if err != nil {
				conn.Write([]byte("HTTP/1.1 500 Internal Server Error\r\n\r\n"))
				return
			}
			conn.Write([]byte("HTTP/1.1 201 Created\r\n\r\n"))
		default:
			conn.Write([]byte("HTTP/1.1 404 Not Found\r\n\r\n"))
		}
	case "GET":
		switch {
		case path == "/":
			conn.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))
		case strings.HasPrefix(path, "/echo/"):
			message := strings.Split(path, "/")[2]
			conn.Write([]byte(fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n%s", len(message), message)))
		case strings.HasPrefix(path, "/files/"):
			dir := os.Args[2]
			filename := strings.Split(path, "/")[2]
			fmt.Print(filename)
			filePath := filepath.Join(dir, filename)
			if _, err := os.Stat(filePath); os.IsNotExist(err) {
				conn.Write([]byte("HTTP/1.1 404 Not Found\r\n\r\n"))
				return
			}
			data, err := os.ReadFile(filePath)
			if err != nil {
				conn.Write([]byte("HTTP/1.1 500 Internal Server Error\r\n\r\n"))
				return
			}
			response := fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: %d\r\n\r\n%s", len(data), data)
			conn.Write([]byte(response))
		case path == "/user-agent":
			conn.Write([]byte(fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n%s", len(user_agent), user_agent)))
		default:
			conn.Write([]byte("HTTP/1.1 404 Not Found\r\n\r\n"))
		}
	default:
		conn.Write([]byte("HTTP/1.1 403 Bad request Method not supported \r\n\r\n"))
	}
}
