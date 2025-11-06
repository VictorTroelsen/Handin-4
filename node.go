package main

import (
	"context"
	"log"
	"sync"
	"time"

	pb "Handin-4/pb"
	"google.golang.org/grpc"
)

type LamportClock struct {
	mu   sync.Mutex
	time int64
}

func (lc *LamportClock) Increment() int64 {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.time++
	return lc.time
}

func (lc *LamportClock) Update(received int64) int64 {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if received > lc.time {
		lc.time = received
	}
	lc.time++
	return lc.time
}

func (lc *LamportClock) Time() int64 {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return lc.time
}

type Node struct {
	pb.UnimplementedRicartServer

	id         string
	port       string
	peers      []string
	clock      LamportClock
	wantCS     bool
	replyCount int
	deferred   []string
	mu         sync.Mutex
}

func NewNode(id, port string, peers []string) *Node {
	return &Node{
		id:    id,
		port:  port,
		peers: peers,
	}
}

func (n *Node) RequestCS(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.clock.Update(req.Timestamp)
	log.Printf("[NODE %s] Received Request from %s (ts=%d)", n.id, req.Message, req.Timestamp)

	shouldDefer := n.wantCS && (req.Timestamp > n.clock.Time() ||
		(req.Timestamp == n.clock.Time() && req.Message > n.id))

	if shouldDefer {
		log.Printf("[NODE %s] Deferring reply to %s", n.id, req.Message)
		n.deferred = append(n.deferred, req.Message)
		return &pb.Response{Granted: false, NodeId: n.id}, nil
	}

	log.Printf("[NODE %s] Granting reply to %s", n.id, req.Message)
	return &pb.Response{Granted: true, NodeId: n.id}, nil
}

func (n *Node) ReleaseCS(ctx context.Context, rel *pb.Release) (*pb.Response, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	log.Printf("[NODE %s] Received Release from %s", n.id, rel.NodeId)

	for _, d := range n.deferred {
		go n.sendReply(d)
	}
	n.deferred = nil
	return &pb.Response{Granted: true, NodeId: n.id}, nil
}

func (n *Node) sendReply(target string) {
	conn, err := grpc.Dial(target, grpc.WithInsecure())
	if err != nil {
		log.Printf("[NODE %s] Failed to dial %s: %v", n.id, target, err)
		return
	}
	defer conn.Close()

	client := pb.NewRicartClient(conn)
	_, err = client.RequestCS(context.Background(), &pb.Request{
		Message:   n.id,
		Timestamp: n.clock.Increment(),
	})
	if err != nil {
		log.Printf("[NODE %s] Error sending deferred reply to %s: %v", n.id, target, err)
	}
}

func (n *Node) RequestCriticalSection() {
	n.mu.Lock()
	n.wantCS = true
	n.replyCount = 0
	ts := n.clock.Increment()
	n.mu.Unlock()

	log.Printf("[NODE %s] Requesting CS at Lamport=%d", n.id, ts)

	for _, peer := range n.peers {
		go func(peer string) {
			conn, err := grpc.Dial(peer, grpc.WithInsecure())
			if err != nil {
				log.Printf("[NODE %s] Failed to dial peer %s: %v", n.id, peer, err)
				return
			}
			defer conn.Close()

			client := pb.NewRicartClient(conn)
			resp, err := client.RequestCS(context.Background(), &pb.Request{
				Message:   n.id,
				Timestamp: ts,
			})
			if err != nil {
				log.Printf("[NODE %s] Error contacting %s: %v", n.id, peer, err)
				return
			}

			if resp.Granted {
				n.mu.Lock()
				n.replyCount++
				n.mu.Unlock()
				log.Printf("[NODE %s] Received granted reply from %s", n.id, peer)
			} else {
				log.Printf("[NODE %s] Received denied reply from %s", n.id, peer)
			}
		}(peer)
	}
}

func (n *Node) WaitForCS() {
	for {
		n.mu.Lock()
		done := n.replyCount == len(n.peers)
		n.mu.Unlock()
		if done {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	log.Printf("[NODE %s] ENTERED critical section", n.id)
	time.Sleep(2 * time.Second)
	n.ReleaseCriticalSection()
}

func (n *Node) ReleaseCriticalSection() {
	n.mu.Lock()
	n.wantCS = false
	n.mu.Unlock()

	log.Printf("[NODE %s] Releasing critical section", n.id)

	for _, peer := range n.peers {
		go func(peer string) {
			conn, err := grpc.Dial(peer, grpc.WithInsecure())
			if err != nil {
				log.Printf("[NODE %s] Failed to dial peer %s: %v", n.id, peer, err)
				return
			}
			defer conn.Close()

			client := pb.NewRicartClient(conn)
			_, err = client.ReleaseCS(context.Background(), &pb.Release{NodeId: n.id})
			if err != nil {
				log.Printf("[NODE %s] Error sending release to %s: %v", n.id, peer, err)
			}
		}(peer)
	}
}
