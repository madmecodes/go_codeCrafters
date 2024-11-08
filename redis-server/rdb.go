package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	RDBHeader     = "REDIS0011"
	TypeString    = 0
	MetadataStart = 0xFA
	DatabaseStart = 0xFE
	ExpireTimeMs  = 0xFC
	ExpireTime    = 0xFD
	EOF           = 0xFF
	ResizeDB      = 0xFB
)

type KeyValue struct {
	Key       string
	Value     string
	HasExpiry bool
	ExpiresAt time.Time
}

type Reader struct {
	filepath string
}

func NewReader(dir, filename string) *Reader {
	return &Reader{
		filepath: filepath.Join(dir, filename),
	}
}

func (r *Reader) Read() ([]KeyValue, error) {
	file, err := os.Open(r.filepath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to open RDB file: %v", err)
	}
	defer file.Close()

	// Read and validate the header
	header := make([]byte, len(RDBHeader))
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, fmt.Errorf("failed to read header: %v", err)
	}
	if string(header) != RDBHeader {
		return nil, fmt.Errorf("invalid RDB header")
	}

	var pairs []KeyValue
	opcode := make([]byte, 1)

	for {
		if _, err := file.Read(opcode); err != nil {
			if err == io.EOF {
				return pairs, nil
			}
			return nil, fmt.Errorf("failed to read opcode: %v", err)
		}

		switch opcode[0] {
		case MetadataStart:
			// Skip metadata
			if err := r.skipMetadata(file); err != nil {
				return nil, fmt.Errorf("failed to skip metadata: %v", err)
			}

		case DatabaseStart:
			// Read database number
			if _, err := r.readLength(file); err != nil {
				return nil, fmt.Errorf("failed to read database number: %v", err)
			}

			// Check for ResizeDB
			if _, err := file.Read(opcode); err != nil {
				return nil, fmt.Errorf("failed to read after database number: %v", err)
			}

			if opcode[0] == ResizeDB {
				// Skip hash table sizes
				if _, err := r.readLength(file); err != nil {
					return nil, err
				}
				if _, err := r.readLength(file); err != nil {
					return nil, err
				}
				if _, err := file.Read(opcode); err != nil {
					return nil, err
				}
			}

			// Read key-value pairs
			for opcode[0] != DatabaseStart && opcode[0] != EOF {
				pair := KeyValue{}

				// Handle expiry
				switch opcode[0] {
				case ExpireTime:
					var expires uint32
					if err := binary.Read(file, binary.LittleEndian, &expires); err != nil {
						return nil, err
					}
					pair.ExpiresAt = time.Unix(int64(expires), 0)
					pair.HasExpiry = true
					if _, err := file.Read(opcode); err != nil {
						return nil, err
					}
				case ExpireTimeMs:
					var expires uint64
					if err := binary.Read(file, binary.LittleEndian, &expires); err != nil {
						return nil, err
					}
					pair.ExpiresAt = time.UnixMilli(int64(expires))
					pair.HasExpiry = true
					if _, err := file.Read(opcode); err != nil {
						return nil, err
					}
				}

				if opcode[0] != TypeString {
					return nil, fmt.Errorf("unsupported value type: %d", opcode[0])
				}

				// Read key and value strings
				key, err := r.readString(file)
				if err != nil {
					return nil, fmt.Errorf("failed to read key: %v", err)
				}
				pair.Key = key

				value, err := r.readString(file)
				if err != nil {
					return nil, fmt.Errorf("failed to read value: %v", err)
				}
				pair.Value = value

				// Add the parsed pair to the list
				pairs = append(pairs, pair)

				// Read the next opcode
				if _, err := file.Read(opcode); err != nil {
					return nil, fmt.Errorf("failed to read next opcode: %v", err)
				}
			}

		case EOF:
			return pairs, nil

		default:
			return nil, fmt.Errorf("unsupported opcode: %d", opcode[0])
		}
	}
}

func (r *Reader) skipMetadata(file *os.File) error {
	// Skip the metadata key
	if err := r.skipString(file); err != nil {
		return err
	}
	// Skip the metadata value
	if err := r.skipString(file); err != nil {
		return err
	}
	return nil
}

func (r *Reader) skipString(file *os.File) error {
	length, err := r.readLength(file)
	if err != nil {
		return err
	}
	_, err = file.Seek(int64(length), io.SeekCurrent)
	return err
}

func (r *Reader) readString(file *os.File) (string, error) {
	length, err := r.readLength(file)
	if err != nil {
		return "", err
	}

	data := make([]byte, length)
	_, err = io.ReadFull(file, data)
	if err != nil {
		return "", fmt.Errorf("failed to read string data: %v", err)
	}

	return string(data), nil
}

func (r *Reader) readLength(file *os.File) (int, error) {
	var length int32
	if err := binary.Read(file, binary.LittleEndian, &length); err != nil {
		return 0, fmt.Errorf("failed to read length: %v", err)
	}
	return int(length), nil
}
