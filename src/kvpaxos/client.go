package kvpaxos

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net/rpc"
	"time"
)

type Clerk struct {
	servers []string
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{key, nrand()}
	reply := &GetReply{}

	for {
		for i := 0; i < len(ck.servers); i++ {
			successfulCall := call(ck.servers[i], "KVPaxos.Get", args, reply)
			if successfulCall {
				return reply.Value
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// shared by Put and Append.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{key, value, op, nrand()}
	reply := &PutAppendReply{}

	for {
		for i := 0; i < len(ck.servers); i++ {
			successfulCall := call(ck.servers[i], "KVPaxos.PutAppend", args, reply)
			if successfulCall {
				return
			}

			time.Sleep(100 * time.Millisecond)
		}
	}
}
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
