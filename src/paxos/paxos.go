package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type PrepareArgs struct {
	Seq  int
	N    int
	From int // We will check if this is really needed later added for debugging
}

type PrepareReply struct {
	Ok  bool
	N_a int
	V_a interface{}
}

type AcceptArgs struct {
	Seq  int
	N    int
	V    interface{}
	From int // We will check if this is really needed later added for debugging
}

type AcceptReply struct {
	Ok bool
}

type DecidedArgs struct {
	Seq int
	V   interface{}
}

type UpdateDoneArgs struct {
	Peer int
	Seq  int
}

type UpdateDoneReply struct {
	Ok bool
}

type Instance struct {
	n_p, n_a int
	v_a      interface{}
	decided  bool
	v        interface{}
}

// Extend Paxos structure to hold acceptor's state and other necessary data.
type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Acceptor's state
	n_p int
	n_a int
	v_a interface{}

	// Proposer state
	n       int
	v       interface{}
	decided bool

	instances map[int]*Instance
	doneSeqs  map[int]int // map of instance id to its highest processed sequence
	forgotten map[int]bool
	// Your data here.
}

func (px *Paxos) getInstance(seq int) *Instance {
	if _, exists := px.instances[seq]; !exists {
		px.instances[seq] = &Instance{}
	}
	return px.instances[seq]
}

func (px *Paxos) UpdateDone(args *UpdateDoneArgs, reply *UpdateDoneReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	// Update the done sequence number for the specified peer.
	px.doneSeqs[args.Peer] = args.Seq
	reply.Ok = true
	return nil
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			// fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
// Implement the proposer logic inside Start()
func (px *Paxos) Start(seq int, v interface{}) {
	px.mu.Lock()
	//defer px.mu.Unlock()
	inst := px.getInstance(seq)
	if inst.decided {
		px.mu.Unlock()
		return
	}
	px.mu.Unlock()

	go func() {
		// Initial value for proposal

		for {
			px.mu.Lock()
			var test = inst.decided
			px.mu.Unlock()

			if test {
				// Exit the loop if the instance is decided
				break
			}
			// Ensure uniqueness by incrementing by the number of peers
			px.mu.Lock()
			px.n += len(px.peers)
			n := px.n
			px.mu.Unlock()

			countPrepareOk := 0
			highestNa := -1
			var highestVa interface{}

			for _, peer := range px.peers {
				//px.log("Sending Prepare request for seq %d with n=%d", seq)
				args := &PrepareArgs{Seq: seq, N: n, From: px.me} // <-- Include Seq here
				var reply PrepareReply
				time.Sleep(80 * time.Microsecond)
				if call(peer, "Paxos.Prepare", args, &reply) && reply.Ok {
					countPrepareOk++
					if reply.N_a > highestNa {
						highestNa = reply.N_a
						highestVa = reply.V_a
					}
				}
			}

			if countPrepareOk <= len(px.peers)/2 {
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)))
				continue
			}

			if highestNa > 0 {
				v = highestVa
			}

			countAcceptOk := 0
			for _, peer := range px.peers {
				args := &AcceptArgs{Seq: seq, N: n, V: v, From: px.me} // <-- Include Seq here
				var reply AcceptReply

				if call(peer, "Paxos.Accept", args, &reply) && reply.Ok {
					countAcceptOk++
				}
			}

			if countAcceptOk > len(px.peers)/2 {
				args := &DecidedArgs{Seq: seq, V: v}
				for _, peer := range px.peers {
					call(peer, "Paxos.Decide", args, nil)
				}
				px.mu.Lock()
				inst.decided = true
				inst.v = v
				px.mu.Unlock()
			} else {
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(30)))
			}
			px.Min()
		}
	}()
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	inst := px.getInstance(args.Seq)
	if args.N > inst.n_p {
		inst.n_p = args.N
		reply.Ok = true
		reply.N_a = inst.n_a
		reply.V_a = inst.v_a
	} else {
		reply.Ok = false
	}

	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	inst := px.getInstance(args.Seq)
	if args.N >= inst.n_p {
		inst.n_p = args.N
		inst.n_a = args.N
		inst.v_a = args.V
		reply.Ok = true
	} else {
		reply.Ok = false
	}

	return nil
}

func (px *Paxos) Decide(args *DecidedArgs, _ *struct{}) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	inst := px.getInstance(args.Seq)
	inst.decided = true
	inst.v = args.V
	return nil
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	px.doneSeqs[px.me] = seq
	px.mu.Unlock()

	// Notify all other peers.
	args := &UpdateDoneArgs{
		Peer: px.me,
		Seq:  seq,
	}
	for i, peer := range px.peers {
		if i != px.me {
			var reply UpdateDoneReply
			call(peer, "Paxos.UpdateDone", args, &reply)
		}
	}
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	maxSeq := -1
	for seq := range px.instances {
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	return maxSeq
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	minSeq := int(^uint(0) >> 1) // set to max int value initially
	for i := 0; i < len(px.peers); i++ {
		seq, exists := px.doneSeqs[i]
		if !exists {
			// If we haven't heard from a peer, set its sequence to -1.
			seq = -1
		}
		if seq < minSeq {
			minSeq = seq
		}
	}

	for seq := range px.instances {
		if seq <= minSeq {
			delete(px.instances, seq)
			px.forgotten[seq] = true
		}
	}
	return minSeq + 1
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()

	if px.forgotten[seq] {
		return Forgotten, nil
	}

	inst := px.getInstance(seq)
	if inst.decided {
		return Decided, inst.v
	}
	return Pending, nil
}

// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

// has this peer been asked to shut down?
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.instances = make(map[int]*Instance)
	px.doneSeqs = make(map[int]int)
	px.forgotten = make(map[int]bool)
	// Your initialization code here.

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
