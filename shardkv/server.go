package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	Operation string
	Key       string
	Value     string
	Dohash    bool
	Seq       int64
	Client    string
	Config    shardmaster.Config
	NewConfig GetShardReply
	Rand      int64
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	store   map[string]string
	rand    map[string]int64
	replies map[string]string

	last   int
	config shardmaster.Config
}

func (kv *ShardKV) check(op Op) (Err, string) {
	if op.Operation == "GET" || op.Operation == "PUT" {
		shard := key2shard(op.Key)
		if kv.gid != kv.config.Shards[shard] {
			return ErrWrongGroup, ""
		}
		seq, ok := kv.rand[op.Client]
		if ok && op.Seq <= seq {
			return OK, kv.replies[op.Client]
		}
	} else if op.Operation == "Reconfig" {
		//Reconfig
		if kv.config.Num >= op.Config.Num {
			return OK, ""
		}
	}
	return "", ""
}

func (kv *ShardKV) wait(seq int) Op {
	timeout := 10 * time.Millisecond
	for {
		decided, val := kv.px.Status(seq)
		if decided {
			return val.(Op)
		}
		time.Sleep(timeout)
		if timeout < 10*time.Second {
			timeout *= 2
		}
	}
}

func (kv *ShardKV) runGet(op Op) {
	prev, _ := kv.store[op.Key]
	kv.replies[op.Client] = prev
	kv.rand[op.Client] = op.Seq
}

func (kv *ShardKV) runPut(op Op) {
	prev, _ := kv.store[op.Key]
	kv.replies[op.Client] = prev
	kv.rand[op.Client] = op.Seq

	if op.Dohash {
		kv.store[op.Key] = NextValue(prev, op.Value)
	} else {
		kv.store[op.Key] = op.Value
	}
}

func (kv *ShardKV) runReconfig(op Op) {
	config := &op.NewConfig
	for k := range config.Store {
		kv.store[k] = config.Store[k]
	}
	for client := range config.Rand {
		seq, ok := kv.rand[client]
		if !ok || seq < config.Rand[client] {
			kv.rand[client] = config.Rand[client]
			kv.replies[client] = config.Replies[client]
		}
	}
	kv.config = op.Config
}

func (kv *ShardKV) run(op Op) {
	if op.Operation == "GET" {
		kv.runGet(op)
	} else if op.Operation == "PUT" {
		kv.runPut(op)
	} else if op.Operation == "Reconfig" {
		kv.runReconfig(op)
	}
}
func (kv *ShardKV) execute(op Op) (Err, string) {
	var operation Op
	for {
		res, val := kv.check(op)
		if res != "" {
			return res, val
		}
		decided, status := kv.px.Status(kv.last + 1)
		if decided {
			operation = status.(Op)
		} else {
			kv.px.Start(kv.last+1, op)
			operation = kv.wait(kv.last + 1)
		}
		kv.run(operation)
		kv.last++
		kv.px.Done(kv.last)
		if operation.Rand == op.Rand {
			break
		}
	}
	return OK, kv.replies[op.Client]
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{Operation: "GET", Key: args.Key, Seq: args.Seq, Client: args.Me, Rand: nrand()}
	reply.Err, reply.Value = kv.execute(op)
	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{Operation: "PUT", Key: args.Key, Value: args.Value, Dohash: args.DoHash, Seq: args.Seq, Client: args.Me, Rand: nrand()}
	reply.Err, reply.PreviousValue = kv.execute(op)
	return nil
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {
	if kv.config.Num < args.Config.Num {
		reply.Err = ErrNotReady
		return nil
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := args.Shard
	kv.execute(Op{Operation: "GetShard"})

	reply.Store = make(map[string]string)
	reply.Rand = make(map[string]int64)
	reply.Replies = make(map[string]string)

	for k := range kv.store {
		if key2shard(k) == shard {
			reply.Store[k] = kv.store[k]
		}
	}

	for client := range kv.rand {
		reply.Rand[client] = kv.rand[client]
		reply.Replies[client] = kv.replies[client]
	}
	return nil
}

func (reply *GetShardReply) Merge(other GetShardReply) {
	for k := range other.Store {
		reply.Store[k] = other.Store[k]
	}
	for c := range other.Rand {
		seq, ok := reply.Rand[c]
		if !ok || seq < other.Rand[c] {
			reply.Rand[c] = other.Rand[c]
			reply.Replies[c] = other.Replies[c]
		}
	}
}

func (kv *ShardKV) reconfigure(newConfig shardmaster.Config) bool {
	reconfig := GetShardReply{OK, make(map[string]string), make(map[string]int64), make(map[string]string)}
	oldcfg := &kv.config
	for i := 0; i < shardmaster.NShards; i++ {
		gid := oldcfg.Shards[i]
		if newConfig.Shards[i] == kv.gid && gid != kv.gid {
			args := &GetShardArgs{i, *oldcfg}
			var reply GetShardReply
			for _, s := range oldcfg.Groups[gid] {
				ok := call(s, "ShardKV.GetShard", args, &reply)
				if ok && reply.Err == OK {
					break
				}
				if ok && reply.Err == ErrNotReady {
					return false
				}
			}
			reconfig.Merge(reply)
		}
	}
	op := Op{Operation: "Reconfig", Config: newConfig, NewConfig: reconfig}
	kv.execute(op)
	return true
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	newConfig := kv.sm.Query(-1)
	for i := kv.config.Num + 1; i <= newConfig.Num; i++ {
		cfg := kv.sm.Query(i)
		if !kv.reconfigure(cfg) {
			return
		}
	}
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.store = make(map[string]string)
	kv.rand = make(map[string]int64)
	kv.replies = make(map[string]string)
	kv.last = 0

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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
