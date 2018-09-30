package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	timeRecord  map[string]time.Time
	currentView View
	acked       bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	vs.timeRecord[args.Me] = time.Now()
	if args.Me == vs.currentView.Primary && args.Viewnum == vs.currentView.Viewnum { //Acked
		vs.acked = true
	}

	if args.Viewnum == 0 && vs.acked {
		if args.Me == vs.currentView.Backup { //First Ping from backup
			vs.currentView.Viewnum++
		} else if args.Me == vs.currentView.Primary { //The primary server crashed
			if vs.currentView.Backup != "" {
				vs.currentView.Primary = vs.currentView.Backup //Let backup server be the primary
				vs.currentView.Viewnum++
				vs.currentView.Backup = ""
			}
		} else {
			if vs.currentView.Primary == "" {
				if vs.currentView.Backup != "" { //Let backup server be the primary
					vs.currentView.Primary = vs.currentView.Backup
					vs.currentView.Backup = ""
				} else {
					vs.currentView.Primary = args.Me //Let current server be the primary
				}
				vs.currentView.Viewnum++
			} else if vs.currentView.Backup == "" { //Let current server be the backup
				vs.currentView.Backup = args.Me
				vs.currentView.Viewnum++
			}
		}
	}

	if args.Me == vs.currentView.Primary && args.Viewnum != vs.currentView.Viewnum { //View changed
		vs.acked = false
	}

	reply.View = vs.currentView
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.currentView
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.acked == false { //Can't change View because vs hasn't received an ack for the currentView from primary
		return
	}
	curView := vs.currentView.Viewnum //Record current View

	if vs.currentView.Primary != "" && time.Since(vs.timeRecord[vs.currentView.Primary]) > DeadPings*PingInterval { //Primary died
		if vs.currentView.Backup != "" {
			vs.currentView.Primary = vs.currentView.Backup
			vs.currentView.Viewnum++
			vs.currentView.Backup = ""
		}
	}

	if vs.currentView.Backup != "" && time.Since(vs.timeRecord[vs.currentView.Backup]) > DeadPings*PingInterval { //Backup died
		vs.currentView.Backup = ""
		vs.currentView.Viewnum++
	}

	if curView != vs.currentView.Viewnum { //View changed
		vs.acked = false
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.dead = false
	vs.timeRecord = make(map[string]time.Time)
	vs.acked = true
	vs.currentView = View{Viewnum: 0, Primary: "", Backup: ""}

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
