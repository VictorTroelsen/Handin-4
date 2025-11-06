package Handin_4

import (
	"sync"

	"google.golang.org/grpc"
)

type Node struct {
	id              string
	timestamp       int64
	state           string
	mutex           sync.Mutex
	criticalSection chan bool
	server          *grpc.Server
	peers           []string
}
