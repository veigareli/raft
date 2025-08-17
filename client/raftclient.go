package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"strings"

	"github.com/mkyas/miniraft"
	"google.golang.org/protobuf/proto"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: go run raftclient.go server-host:server-port")
	}

	serverAddr := os.Args[1]

	fmt.Println("Connected to Raft server. Type commands (or 'exit' to quit).")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		scanner.Scan()
		command := strings.TrimSpace(scanner.Text())

		if command == "exit" {
			fmt.Println("Exiting client.")
			break
		}

		if !isValidCommand(command) {
			fmt.Println("Invalid command. Use only letters, digits, dashes, or underscores.")
			continue
		}

		raftMsg := &miniraft.Raft{
			Message: &miniraft.Raft_CommandName{CommandName: command},
		}

		data, err := proto.Marshal(raftMsg)
		if err != nil {
			continue
		}

		sendMessage(serverAddr, data)
	}
}

func isValidCommand(command string) bool {
	validCommand := regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	return validCommand.MatchString(command)
}

func sendMessage(target string, data []byte) bool {
	addr, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		return false
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return false
	}
	defer conn.Close()

	_, err = conn.Write(data)
	if err != nil {
		return false
	}
	return true
}
