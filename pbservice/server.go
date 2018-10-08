package pbservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
	"viewservice"
)

//import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.
	store  map[string]string
	mu     sync.Mutex
	rand   map[int64]string
	view   viewservice.View
	vshost string
}

func (pb *PBServer) replicateGet(args *GetArgs, reply *GetReply) bool {
	if pb.view.Backup == "" {
		return true
	}
	backupArgs := *args
	backupArgs.Me = pb.me

	ok := call(pb.view.Backup, "PBServer.Get", backupArgs, &reply)
	if ok {
		return true
	}
	return false
}

func (pb *PBServer) replicatePut(args *PutArgs, reply *PutReply) bool {
	if pb.view.Backup == "" {
		return true
	}
	backupArgs := *args
	backupArgs.Me = pb.me

	ok := call(pb.view.Backup, "PBServer.Put", backupArgs, &reply)
	if ok {
		return true
	}
	return false
}

func (pb *PBServer) getHelper(args *GetArgs, reply *GetReply) {
	val, duplicate := pb.rand[args.Rand]
	if duplicate {
		if reply.Value == "" {
			reply.Value = val
		}
		reply.Err = ""
		return
	}
	key := args.Key
	val, ok := pb.store[key]
	if ok {
		if reply.Value == "" {
			reply.Value = val
		}
		reply.Err = ""
	} else {
		// reply.Value = ""
		if reply.Value == "" {
			reply.Value = ""
		}
		reply.Err = ErrNoKey
	}
	pb.rand[args.Rand] = reply.Value
}

func (pb *PBServer) putHelper(args *PutArgs, reply *PutReply) {
	val, duplicate := pb.rand[args.Rand]
	if duplicate {
		if reply.PreviousValue == "" {
			reply.PreviousValue = val
		}
		reply.Err = ""
		return
	}
	key := args.Key
	value := args.Value
	doHash := args.DoHash

	if doHash {
		val, ok := pb.store[key]
		if ok {
			if reply.PreviousValue == "" {
				reply.PreviousValue = val
			}
			Hash := hash(val + value)
			pb.store[key] = strconv.Itoa(int(Hash))
		} else {
			if reply.PreviousValue == "" {
				reply.PreviousValue = ""
			}
			Hash := hash(value)
			pb.store[key] = strconv.Itoa(int(Hash))
		}
	} else {
		val, ok := pb.store[key]
		if ok {
			if reply.PreviousValue == "" {
				reply.PreviousValue = val
			} else {
				if reply.PreviousValue == "" {
					reply.PreviousValue = ""
				}
			}
			pb.store[key] = value
		}
	}

	reply.Err = ""
	pb.rand[args.Rand] = reply.PreviousValue

}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Primary == pb.me {
		if pb.replicatePut(args, reply) {
			pb.putHelper(args, reply)
		} else {
			reply.PreviousValue = ""
			reply.Err = ErrWrongServer
		}
	} else if pb.view.Backup == pb.me {
		if args.Me == pb.view.Primary {
			pb.putHelper(args, reply)
		} else {
			reply.PreviousValue = ""
			reply.Err = ErrWrongServer
		}
	} else {
		reply.PreviousValue = ""
		reply.Err = ErrWrongServer
	}

	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Primary == pb.me {
		if pb.replicateGet(args, reply) {
			pb.getHelper(args, reply)
		} else {
			reply.Value = ""
			reply.Err = ErrWrongServer
		}
	} else if pb.view.Backup == pb.me {
		if args.Me == pb.view.Primary {
			pb.getHelper(args, reply)
		} else {
			reply.Value = ""
			reply.Err = ErrWrongServer
		}
	} else {
		reply.Value = ""
		reply.Err = ErrWrongServer
	}

	return nil
}

func (pb *PBServer) Copy(args *CopyArgs, reply *CopyReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	for key, value := range args.Store {
		pb.store[key] = value
	}

	for key, value := range args.Rand {
		pb.rand[key] = value
	}

	reply.Err = ""
	return nil
}

func (pb *PBServer) copyStore() {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Backup == "" {
		return
	}

	args := CopyArgs{Store: pb.store, Rand: pb.rand}
	var reply CopyReply

	ok := call(pb.view.Backup, "PBServer.Copy", args, &reply)
	if ok {
		return
	}
	fmt.Printf("Backup copy error")
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
	// Your code here.
	oldBackup := pb.view.Backup
	args := &viewservice.PingArgs{Me: pb.me, Viewnum: pb.view.Viewnum}
	var reply viewservice.PingReply

	ok := call(pb.vshost, "ViewServer.Ping", args, &reply)
	if ok {
		if reply.View.Primary == pb.me || reply.View.Backup == pb.me {
			pb.view = reply.View
		} else {
			pb.view.Viewnum = 0
		}
		if pb.view.Backup != oldBackup && pb.me == reply.View.Primary {
			pb.copyStore()
		}
		return
	}
	pb.view = viewservice.View{Viewnum: 0, Primary: "", Backup: ""}
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.
	pb.vshost = vshost
	pb.store = make(map[string]string)
	pb.rand = make(map[int64]string)
	pb.view = viewservice.View{Viewnum: 0, Primary: "", Backup: ""}

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
