package kvpaxos

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"sync"
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
	Key    string
	Value  string
	DoHash bool
	Rand   int64
	OpType int
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	store map[string]string
	rand  map[int64]string
	seq   int
}

//Wait for Paxos intances to complete agreement
func (kv *KVPaxos) Wait(seq int) Op {
	to := 10 * time.Millisecond
	for {
		decided, val := kv.px.Status(seq)
		if decided {
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

//Execute operation
func (kv *KVPaxos) Execute(op Op) string {
	value, _ := kv.store[op.Key]
	kv.rand[op.Rand] = value
	if op.OpType == 1 { //Put
		if op.DoHash {
			kv.store[op.Key] = NextValue(value, op.Value) //Hash
		} else {
			kv.store[op.Key] = op.Value
		}
	}
	return value
}

func (kv *KVPaxos) Update(op Op) string {
	var operation Op
	for {
		ok, val := kv.px.Status(kv.seq + 1)
		if ok {
			operation = val.(Op)
		} else {
			kv.px.Start(kv.seq+1, op)
			operation = kv.Wait(kv.seq + 1)
		}
		value := kv.Execute(operation)
		kv.seq++
		kv.px.Done(kv.seq)
		if operation == op { //agreement
			return value
		}
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, ok := kv.rand[args.Rand]
	if ok {
		reply.Value = val
		return nil
	}

	op := Op{Key: args.Key, Value: "", DoHash: false, Rand: args.Rand, OpType: 0}
	value := kv.Update(op)

	reply.Value = value
	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, ok := kv.rand[args.Rand]
	if ok {
		reply.PreviousValue = val
		return nil
	}

	op := Op{Key: args.Key, Value: args.Value, DoHash: args.DoHash, Rand: args.Rand, OpType: 1}
	value := kv.Update(op)

	reply.PreviousValue = value
	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.store = make(map[string]string)
	kv.rand = make(map[int64]string)
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
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
