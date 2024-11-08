package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	dir        string
	dbfilename string
}

var config Config

type StoreItem struct {
	value     string
	hasExpiry bool
	expiresAt time.Time
}

type Store struct {
	mu   sync.RWMutex
	data map[string]StoreItem
}

var store = &Store{
	data: make(map[string]StoreItem),
}

func (s *Store) Set(key, value string, px int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := StoreItem{
		value:     value,
		hasExpiry: px > 0,
	}
	if px > 0 {
		item.expiresAt = time.Now().Add(time.Duration(px) * time.Millisecond)
	}
	s.data[key] = item
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	item, exists := s.data[key]
	if !exists {
		return "", false
	}
	if item.hasExpiry && time.Now().After(item.expiresAt) {
		go s.deleteKey(key)
		return "", false
	}
	return item.value, true
}

func (s *Store) deleteKey(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

func main() {
	flag.StringVar(&config.dir, "dir", "", "Directory for RDB file")
	flag.StringVar(&config.dbfilename, "dbfilename", "", "Name of RDB file")
	flag.Parse()

	// Load RDB file at startup
	reader := NewReader(config.dir, config.dbfilename)
	pairs, err := reader.Read()
	if err != nil {
		log.Printf("Error reading RDB file: %v", err)
	} else if pairs != nil {
		for _, pair := range pairs {
			var px int64 = 0
			if pair.HasExpiry {
				px = time.Until(pair.ExpiresAt).Milliseconds()
				if px <= 0 {
					continue
				}
			}
			store.Set(pair.Key, pair.Value, px)
		}
	}

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			os.Exit(1)
		}
		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		firstLine, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading first line: %v\n", err)
			}
			return
		}

		if !strings.HasPrefix(firstLine, "*") {
			log.Println("Invalid RESP format: expected array")
			return
		}

		arrayLen, err := strconv.Atoi(strings.TrimSuffix(firstLine[1:], "\r\n"))
		if err != nil {
			log.Printf("Error parsing array length: %v\n", err)
			return
		}

		elements := make([]string, 0, arrayLen)
		for i := 0; i < arrayLen; i++ {
			bulkLen, err := reader.ReadString('\n')
			if err != nil {
				log.Printf("Error reading bulk length: %v\n", err)
				return
			}

			if !strings.HasPrefix(bulkLen, "$") {
				log.Println("Invalid RESP format: expected bulk string")
				return
			}

			length, err := strconv.Atoi(strings.TrimSuffix(bulkLen[1:], "\r\n"))
			if err != nil {
				log.Printf("Error parsing bulk length: %v\n", err)
				return
			}

			bulkData := make([]byte, length+2)
			_, err = io.ReadFull(reader, bulkData)
			if err != nil {
				log.Printf("Error reading bulk data: %v\n", err)
				return
			}

			elements = append(elements, string(bulkData[:length]))
		}

		if len(elements) > 0 {
			command := strings.ToUpper(elements[0])
			switch command {
			case "PING":
				_, err = conn.Write([]byte("+PONG\r\n"))
			case "ECHO":
				if len(elements) < 2 {
					_, err = conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
					continue
				}
				response := fmt.Sprintf("$%d\r\n%s\r\n", len(elements[1]), elements[1])
				_, err = conn.Write([]byte(response))
			case "SET":
				if len(elements) < 3 {
					_, err = conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
					continue
				}

				key, value := elements[1], elements[2]
				var px int64 = 0

				if len(elements) > 3 && strings.ToUpper(elements[3]) == "PX" {
					if len(elements) < 5 {
						_, err = conn.Write([]byte("-ERR syntax error\r\n"))
						continue
					}
					px, err = strconv.ParseInt(elements[4], 10, 64)
					if err != nil {
						_, err = conn.Write([]byte("-ERR invalid expire time in 'set' command\r\n"))
						continue
					}
				}

				store.Set(key, value, px)
				_, err = conn.Write([]byte("+OK\r\n"))
			case "GET":
				if len(elements) != 2 {
					_, err = conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
					continue
				}
				value, exists := store.Get(elements[1])
				if !exists {
					_, err = conn.Write([]byte("$-1\r\n"))
				} else {
					response := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
					_, err = conn.Write([]byte(response))
				}
			case "CONFIG":
				if len(elements) >= 3 && strings.ToUpper(elements[1]) == "GET" {
					handleConfigGet(conn, elements[2])
				} else {
					_, err = conn.Write([]byte("-ERR Wrong number of arguments\r\n"))
				}
			case "KEYS":
				if len(elements) != 2 {
					_, err = conn.Write([]byte("-ERR wrong number of arguments for 'keys' command\r\n"))
					continue
				}

				pattern := elements[1]
				if pattern != "*" {
					_, err = conn.Write([]byte("-ERR unsupported pattern\r\n"))
					continue
				}

				// Get all keys from store
				keys := getAllKeys(store)
				response := fmt.Sprintf("*%d\r\n", len(keys))
				for _, key := range keys {
					response += fmt.Sprintf("$%d\r\n%s\r\n", len(key), key)
				}

				_, err = conn.Write([]byte(response))
			default:
				_, err = conn.Write([]byte("-ERR unknown command\r\n"))
			}

			if err != nil {
				log.Printf("Error writing response: %v\n", err)
				return
			}
		}
	}
}

func handleConfigGet(conn net.Conn, param string) {
	var value string
	switch strings.ToLower(param) {
	case "dir":
		value = config.dir
	case "dbfilename":
		value = config.dbfilename
	default:
		value = ""
	}
	response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(param), param,
		len(value), value)

	_, err := conn.Write([]byte(response))
	if err != nil {
		log.Printf("Error writing config GET response: %v", err)
	}
}

func getAllKeys(s *Store) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	now := time.Now()

	for key, item := range s.data {
		if item.hasExpiry && now.After(item.expiresAt) {
			go s.deleteKey(key)
			continue
		}
		keys = append(keys, key)
	}

	return keys
}
