package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	pb "Handin-4/pb"
	"google.golang.org/grpc"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run main.go <node_id> <port> <peer1,peer2,...>")
		os.Exit(1)
	}

	nodeID := os.Args[1]
	port := os.Args[2]
	var peers []string
	if len(os.Args) >= 4 {
		peers = strings.Split(os.Args[3], ",")
	}

	node := NewNode(nodeID, port, peers)

	// Start gRPC server
	go func() {
		lis, err := net.Listen("tcp", ":"+port)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		s := grpc.NewServer()
		pb.RegisterRicartServer(s, node)
		log.Printf("[%s] Listening on port %s", nodeID, port)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	// Give time for all nodes to start
	time.Sleep(2 * time.Second)

	// Periodically attempt to enter CS
	for {
		node.RequestCriticalSection()
		node.WaitForCS()
		time.Sleep(5 * time.Second)
	}
}
