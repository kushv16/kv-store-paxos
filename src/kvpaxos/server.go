package kvpaxos

import (
	"cse-513/src/paxos"
	"encoding/gob"
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

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpID      int64
	Operation string
	Key       string
	Value     string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	db                sync.Map
	processedRequests []int64 // Slice to track processed OpIDs
	seq               int
}

func (kv *KVPaxos) putHelper(Key string, Value string, OpID int64) {
	_, ok := kv.db.Load(Key)
	if !ok {
		kv.db.Store(Key, Value)
	} else {
		kv.db.Store(Key, Value)
	}
	kv.processedRequests = append(kv.processedRequests, OpID)
}

func (kv *KVPaxos) appendHelper(Key string, Value string, OpID int64) {
	val, ok := kv.db.Load(Key)
	if !ok {
		kv.db.Store(Key, Value)
	} else {
		vals := val.(string)
		found := false
		for _, processedOpID := range kv.processedRequests {
			if processedOpID == OpID {
				found = true
				break
			}
		}
		if !found {
			kv.db.Store(Key, vals+Value)
		}
	}
	kv.processedRequests = append(kv.processedRequests, OpID)
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{args.OpID, "Get", args.Key, ""}
	kv.SynchronizeServers(op)

	val, ok := kv.db.Load(args.Key)
	if !ok {
		reply.Value = ""
	} else {
		reply.Value = val.(string)
	}

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{args.OpID, args.Op, args.Key, args.Value}
	kv.SynchronizeServers(op)

	if op.Operation == "Put" {
		kv.putHelper(args.Key, args.Value, args.OpID)
	} else if op.Operation == "Append" {
		kv.appendHelper(args.Key, args.Value, args.OpID)
	}
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	//DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

// to synchronize servers which were not part of paxos agreement and hence have stale data
func (kv *KVPaxos) SynchronizeServers(currentOp Op) {
	sleepDuration := 20 * time.Millisecond
	needToPropose := true
	for {
		status, op := kv.px.Status(kv.seq)
		if status != paxos.Decided {
			if needToPropose {
				kv.px.Start(kv.seq, currentOp)
				needToPropose = false
			}
			time.Sleep(sleepDuration)
			sleepDuration += 10 * time.Millisecond
		} else {
			op := op.(Op)
			if currentOp.OpID == op.OpID {
				kv.px.Done(kv.seq)
				kv.seq++
				return
			}
			if op.Operation == "Put" {
				kv.putHelper(op.Key, op.Value, op.OpID)
			} else if op.Operation == "Append" {
				kv.appendHelper(op.Key, op.Value, op.OpID)
			}
			kv.seq++
			needToPropose = true
		}
	}
	kv.px.Done(kv.seq)
	kv.seq++
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.seq = 0

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
