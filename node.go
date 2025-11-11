package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "Handin-4/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type State int

const (
	Released State = iota
	Wanted
	Held
)

type Node struct {
	pb.UnimplementedRicartServer

	id    int
	addr  string
	peers map[int]string // id -> addr

	// grpc
	server  *grpc.Server
	clients map[int]pb.RicartClient

	// RA
	mu      sync.Mutex
	cv      *sync.Cond
	state   State
	clock   int64
	wantTs  int64
	waiters int // how many goroutines are currently blocked in RequestCS
}

func NewNode(id int, addr string, peers map[int]string) *Node {
	n := &Node{
		id:      id,
		addr:    addr,
		peers:   peers,
		clients: make(map[int]pb.RicartClient),
		state:   Released,
	}
	n.cv = sync.NewCond(&n.mu)
	return n
}

func (n *Node) tick() int64 { n.clock++; return n.clock }
func (n *Node) update(ts int64) {
	if ts >= n.clock {
		n.clock = ts + 1
	} else {
		n.clock++
	}
}

func (n *Node) StartGRPC() error {
	lis, err := net.Listen("tcp", n.addr)
	if err != nil {
		return err
	}
	n.server = grpc.NewServer()
	pb.RegisterRicartServer(n.server, n)
	log.Printf("[N%d] listening on %s", n.id, n.addr)
	go func() {
		if err := n.server.Serve(lis); err != nil {
			log.Fatalf("[N%d] grpc serve error: %v", n.id, err)
		}
	}()
	return nil
}

func (n *Node) DialPeers() error {
	for pid, paddr := range n.peers {
		cc, err := grpc.Dial(paddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial %d @ %s: %w", pid, paddr, err)
		}
		n.clients[pid] = pb.NewRicartClient(cc)
	}
	return nil
}

// ---------------- RPCs (YOUR PROTO) ----------------

func (n *Node) RequestCS(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	fromID, _ := strconv.Atoi(strings.TrimSpace(req.GetMessage()))
	n.mu.Lock()
	n.update(req.GetTimestamp())
	myClockNow := n.clock

	log.Printf("[N%d t=%d] <- REQUEST from N%d(t=%d)", n.id, myClockNow, fromID, req.GetTimestamp())

	// Decide whether to grant now or defer by blocking the handler.
	for {
		grant := false
		switch n.state {
		case Released:
			grant = true
		case Held:
			grant = false
		case Wanted:
			// Compare (my.wantTs, my.id) vs (req.ts, fromID)
			if n.wantTs > req.GetTimestamp() {
				grant = true
			} else if n.wantTs == req.GetTimestamp() && n.id > fromID {
				grant = true
			} else {
				grant = false
			}
		}

		if grant {
			n.tick() // sending a logical event
			n.mu.Unlock()
			return &pb.Response{Granted: true, NodeId: strconv.Itoa(n.id)}, nil
		}

		// Defer: wait until our state changes (Release wakes us).
		n.waiters++
		n.cv.Wait()
		n.waiters--
		// loop and re-evaluate
	}
}

func (n *Node) ReleaseCS(ctx context.Context, rel *pb.Release) (*pb.Response, error) {
	// Not strictly needed for correctness in this blocking design,
	// but we keep it for logging/completeness.
	log.Printf("[N%d] <- RELEASE notice from N%s", n.id, rel.GetNodeId())
	return &pb.Response{Granted: true, NodeId: strconv.Itoa(n.id)}, nil
}

// ---------------- Node-side RA API ----------------

func (n *Node) RequestCriticalSection() {
	n.mu.Lock()
	n.state = Wanted
	n.wantTs = n.tick()
	ts := n.wantTs
	n.mu.Unlock()

	log.Printf("[N%d t=%d] broadcasting REQUEST to %d peers", n.id, ts, len(n.peers))

	// Ask all peers; all must grant.
	var wg sync.WaitGroup
	errCh := make(chan error, len(n.peers))

	for pid, c := range n.clients {
		wg.Add(1)
		go func(pid int, c pb.RicartClient) {
			defer wg.Done()
			_, err := c.RequestCS(context.Background(), &pb.Request{
				Message:   strconv.Itoa(n.id),
				Timestamp: ts,
			})
			if err != nil {
				errCh <- fmt.Errorf("peer %d: %w", pid, err)
			}
		}(pid, c)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			log.Printf("[N%d] error waiting replies: %v", n.id, err)
		}
	}

	n.mu.Lock()
	n.state = Held
	now := n.tick()
	n.mu.Unlock()
	log.Printf("[N%d t=%d] >>> ENTER CS", n.id, now)
}

func (n *Node) ReleaseCriticalSection() {
	n.mu.Lock()
	if n.state != Held {
		n.mu.Unlock()
		return
	}
	n.state = Released
	now := n.tick()
	// Wake any deferred request handlers.
	n.cv.Broadcast()
	w := n.waiters
	n.mu.Unlock()

	log.Printf("[N%d t=%d] <<< EXIT CS; woke %d waiter(s)", n.id, now, w)

	// Optional: notify peers (matches your proto but not required)
	for pid, c := range n.clients {
		go func(pid int, c pb.RicartClient) {
			_, _ = c.ReleaseCS(context.Background(), &pb.Release{NodeId: strconv.Itoa(n.id)})
		}(pid, c)
	}
}

func (n *Node) SimulateCriticalSection(work time.Duration) {
	n.RequestCriticalSection()
	log.Printf("[N%d] [CS] doing sensitive work...", n.id)
	time.Sleep(work)
	n.ReleaseCriticalSection()
}

// Parse peers flag like: "1=127.0.0.1:5001,2=127.0.0.1:5002,3=127.0.0.1:5003"
func parsePeers(flag string, selfID int, selfAddr string) map[int]string {
	out := map[int]string{}
	parts := strings.Split(flag, ",")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		var id int
		var addr string
		if _, err := fmt.Sscanf(p, "%d=%s", &id, &addr); err != nil {
			log.Fatalf("bad peers entry %q: %v", p, err)
		}
		if id == selfID {
			addr = selfAddr
		}
		out[id] = addr
	}
	delete(out, selfID)
	return out
}
