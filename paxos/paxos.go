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
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instance map[int]Instance
	values   map[int]interface{}
	maxSeq   int
	done     []int
}

type Instance struct {
	n_a int
	v_a interface{}
	n_p int

	local_a int
	local_p int
}

type ProposeArgs struct {
	N_p   int
	Seq   int
	Value interface{}

	Me      int
	N_local int
}

type ProposeReply struct {
	Reply   string
	Me      int
	N_local int
	N_a     int
	V_a     interface{}
}

type AcceptArgs struct {
	Value interface{}
	N_p   int
	Me    int

	Seq     int
	N_local int
}

type AcceptReply struct {
	Me    int
	N     int
	Reply string
}

type DecideArgs struct {
	V_a  interface{}
	Me   int
	Seq  int
	Done int
}

type DecideReply struct {
	Me    int
	Reply string
}

func (px *Paxos) Propose(args *ProposeArgs, reply *ProposeReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	reply.Me = px.me
	instance := px.instance[args.Seq]

	if args.N_p > instance.n_p {
		instance.n_p = args.N_p    //Update N_p = N
		instance.local_p = args.Me //Update proposer

		reply.N_a = instance.n_a //Return <Propose-OK, N_a, V_a>
		reply.N_local = instance.local_a
		reply.V_a = instance.v_a
		reply.Reply = "OK"
		px.instance[args.Seq] = instance
	} else { //Return <Reject, Np>
		reply.N_local = instance.local_p
		reply.N_a = instance.n_p
		reply.Reply = "Reject"
	}
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	reply.Me = px.me
	instance := px.instance[args.Seq]

	if args.N_p >= instance.n_p { //Update N_p = N, N_a = N, V_a = V
		instance.n_a = args.N_p
		instance.n_p = args.N_p
		instance.v_a = args.Value
		instance.local_a = args.N_local
		instance.local_p = args.N_local
		px.instance[args.Seq] = instance
		reply.N = instance.n_a
		reply.Reply = "OK"
	} else { //Return <Reject, Na>
		reply.Reply = "Reject"
		reply.N = instance.n_a
	}
	return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	reply.Me = px.me

	if px.me != args.Me { //Call from peer
		if px.done[args.Me] < args.Done { //Consensus reached
			px.done[args.Me] = args.Done
		}
	}
	px.values[args.Seq] = args.V_a
	if px.maxSeq < args.Seq { //Update maxSeq
		px.maxSeq = args.Seq
	}
	return nil
}

//
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
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
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

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	if seq < px.Min() { //Ignored
		return
	}
	go func() {
		px.mu.Lock()
		n_p := px.instance[seq].n_p
		v_a := px.instance[seq].v_a
		nextN := px.instance[seq].n_p
		px.mu.Unlock()

		for {
			status, _ := px.Status(seq)
			if status { //Done
				return
			}
			nextN++
			n_p = nextN
			num_proposeOk := 0 //Number of servers returned Propose-OK
			max_N := 0         //Higher than any n seen so far
			findHighestN := false

			for i, peer := range px.peers {
				args := &ProposeArgs{Me: px.me, N_local: px.me, N_p: n_p, Value: v, Seq: seq}
				var reply ProposeReply
				var proposeOK bool

				if i == px.me { //Local call
					px.Propose(args, &reply)
					proposeOK = true
				} else { //RPC
					proposeOK = call(peer, "Paxos.Propose", args, &reply)
				}

				if proposeOK {
					if reply.Reply == "OK" {
						num_proposeOk++
						if reply.N_a > max_N { //Find higher N
							findHighestN = true
							v_a = reply.V_a
							max_N = reply.N_a
						} else {
							nextN = reply.N_a
						}
					}
				}
			}

			if num_proposeOk > len(px.peers)/2 { //Reach majority consensus
				num_acceptOK := 0
				args := &AcceptArgs{Me: px.me, N_local: px.me, N_p: n_p, Seq: seq, Value: v}
				if findHighestN { //v' = v_a with highest n_a. Choose own v otherwise
					args.Value = v_a
				}
				for i, peer := range px.peers {
					var reply AcceptReply
					var acceptOK bool
					if i == px.me { //Local call
						px.Accept(args, &reply)
						acceptOK = true
					} else { //RPC
						acceptOK = call(peer, "Paxos.Accept", args, &reply)
					}
					if acceptOK {
						if reply.Reply == "OK" {
							num_acceptOK++
						}
					}
				}
				if num_acceptOK > len(px.peers)/2 { //Reach majority consensus
					for i, peer := range px.peers {
						args := &DecideArgs{Me: px.me, Seq: seq, V_a: v, Done: px.done[px.me]}
						if findHighestN {
							args.V_a = v_a
						}
						var reply DecideReply
						if i == px.me { //Local call
							px.Decide(args, &reply)
						} else { //RPC
							call(peer, "Paxos.Decide", args, &reply)
						}
					}
				}
			}
		}

	}()
	return
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	if px.done[px.me] < seq {
		px.done[px.me] = seq
	}
	px.mu.Unlock()
	px.Min()
	return
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.maxSeq
}

//
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
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	min := px.done[px.me]

	for _, seq := range px.done {
		if seq < min {
			min = seq
		}
	}

	for seq, _ := range px.values { //Forgetting
		if seq <= min {
			delete(px.values, seq)
			delete(px.instance, seq)
		}
	}
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.
	if seq < px.Min() {
		return false, nil
	}
	px.mu.Lock()
	defer px.mu.Unlock()

	val, ok := px.values[seq]
	if ok {
		return true, val
	}

	return false, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.values = make(map[int]interface{})
	px.instance = make(map[int]Instance)
	px.done = make([]int, len(peers))
	for i := range px.done {
		px.done[i] = -1
	}

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
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
