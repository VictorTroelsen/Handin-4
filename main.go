package main

import (
	"flag"
	"log"
	"time"
)

func main() {
	id := flag.Int("id", 1, "node id")
	addr := flag.String("addr", "127.0.0.1:5001", "listen addr")
	peers := flag.String("peers", "1=127.0.0.1:5001,2=127.0.0.1:5002,3=127.0.0.1:5003", "comma list id=addr")
	auto := flag.Bool("auto", true, "auto demo")
	flag.Parse()

	peerMap := parsePeers(*peers, *id, *addr)
	n := NewNode(*id, *addr, peerMap)

	if err := n.StartGRPC(); err != nil {
		log.Fatalf("start grpc: %v", err)
	}
	if err := n.DialPeers(); err != nil {
		log.Fatalf("dial peers: %v", err)
	}
	log.Printf("[N%d] peers: %+v", *id, peerMap)

	if *auto {
		// Run forever with a little stagger so logs interleave nicely
		for {
			// jitter so nodes don’t always collide the same way
			time.Sleep(time.Duration(400+150*(*id)) * time.Millisecond)
			n.SimulateCriticalSection(800 * time.Millisecond)
			time.Sleep(300 * time.Millisecond)
		}
	}

	select {}
}
