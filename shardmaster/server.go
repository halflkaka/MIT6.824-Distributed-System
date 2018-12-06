package shardmaster

import (
	"encoding/gob"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"sync"
	"syscall"
	"time"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num

	last int
}

type Op struct {
	// Your data here.
	Gid       int64
	Operation string
	Shard     int
	Num       int
	Servers   []string
	Rand      int64
}

//copy config
func copy(config Config, num int) Config {
	cf := Config{}
	cf.Num = num
	cf.Shards = config.Shards
	cf.Groups = make(map[int64][]string)
	//copy map
	for k, v := range config.Groups {
		cf.Groups[k] = v
	}
	return cf
}

/*
Find key with smallest value,
use this function to find the group with least shards
*/
func min(count map[int64]int) int64 {
	mini := math.MaxInt32
	var key int64

	for k, v := range count {
		if v < mini {
			mini = v
			key = k
		}
	}
	return key
}

//Count number of shards of each group
func countGroup(config Config) map[int64]int {
	countGroup := make(map[int64]int)
	for key, _ := range config.Groups {
		countGroup[key] = 0
	}

	for _, value := range config.Shards {
		if value != 0 {
			countGroup[value]++
		}
	}
	return countGroup
}

func (sm *ShardMaster) wait(seq int) Op {
	timeout := 10 * time.Millisecond
	for {
		decided, val := sm.px.Status(seq)
		if decided {
			return val.(Op)
		}
		time.Sleep(timeout)
		if timeout < 10*time.Second {
			timeout *= 2
		}
	}
}

//Shift load of each replica group
func (sm *ShardMaster) deliverShards(config Config, count map[int64]int, Gid int64) Config {
	cf := config
	newCount := make(map[int64]int)

	if Gid == -1 { //Leave op
		if len(count) == 0 {
			for i, _ := range cf.Shards {
				cf.Shards[i] = 0
			}
		} else {
			avg := 10 / len(count)
			for shard, gid := range cf.Shards {
				if gid == 0 {
					minGroup := min(count) //group which has least shards
					cf.Shards[shard] = minGroup
					_, ok := newCount[minGroup]
					if ok {
						newCount[minGroup]++
					} else {
						newCount[minGroup] = 1
					}
				} else {
					val, ok := newCount[gid]
					if ok {
						if val >= avg {
							delete(count, gid) //no longer deliver shard to this group
							minGroup := min(count)
							cf.Shards[shard] = minGroup //Deliver shard to minGroup
							newCount[minGroup]++
						} else {
							newCount[gid]++
						}
					} else {
						newCount[gid] = 1
					}
				}
			}
		}
	} else {
		if len(count) > 1 {
			avg := 10 / len(count)
			for shard, gid := range cf.Shards {
				val, ok := newCount[gid]
				if ok {
					if val >= avg {
						delete(count, gid)
						minGroup := min(count)
						cf.Shards[shard] = minGroup
						newCount[minGroup]++
					} else {
						newCount[gid]++
					}
				} else {
					cf.Shards[shard] = gid
					newCount[gid] = 1
				}
			}
		} else {
			for i, _ := range cf.Shards {
				cf.Shards[i] = Gid
			}
		}
	}
	return cf
}

func (sm *ShardMaster) run(op Op) {
	num := len(sm.configs)
	config := copy(sm.configs[num-1], num)

	var cf Config
	if op.Operation == "Join" {
		config.Groups[op.Gid] = op.Servers
		count := countGroup(config)
		cf = sm.deliverShards(config, count, op.Gid)
	} else if op.Operation == "Leave" {
		delete(config.Groups, op.Gid)
		for i, group := range config.Shards {
			if group == op.Gid {
				config.Shards[i] = 0
			}
		}
		count := countGroup(config)
		cf = sm.deliverShards(config, count, -1)
	} else if op.Operation == "Move" {
		config.Shards[op.Shard] = op.Gid
		cf = config
	} else {
		return
	}
	sm.configs = append(sm.configs, cf)
}

func (sm *ShardMaster) execute(op Op) {
	var operation Op
	for {
		decided, status := sm.px.Status(sm.last + 1)
		if decided {
			operation = status.(Op)
		} else {
			sm.px.Start(sm.last+1, op)
			operation = sm.wait(sm.last + 1)
		}

		sm.run(operation)
		sm.last++
		sm.px.Done(sm.last)
		if operation.Rand == op.Rand {
			return
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	gid := args.GID
	servers := args.Servers

	op := Op{Gid: gid, Operation: "Join", Servers: servers, Rand: nrand()}

	sm.execute(op)

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	gid := args.GID
	op := Op{Gid: gid, Operation: "Leave", Rand: nrand()}
	sm.execute(op)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	gid := args.GID
	shard := args.Shard

	op := Op{Gid: gid, Operation: "Move", Shard: shard, Rand: nrand()}
	sm.execute(op)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	num := args.Num
	op := Op{Num: num, Operation: "Query", Rand: nrand()}
	sm.execute(op)
	last := len(sm.configs)

	if num == -1 || num > last {
		reply.Config = sm.configs[last-1]
	} else {
		reply.Config = sm.configs[num]
	}
	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	sm.last = 0

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
