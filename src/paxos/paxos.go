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

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import (
    "math/rand"
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

type Paxos struct {
    mu         sync.Mutex
    l          net.Listener
    dead       int32 // for testing
    unreliable int32 // for testing
    rpcCount   int32 // for testing
    peers      []string
    me         int // index into peers[]

    values     map[int]*PaxosFsm
    min        int32
    max        int32
    peerMins       []int32
}

func (px *Paxos) getInstance(seq int) *PaxosFsm {
    px.mu.Lock()
    defer px.mu.Unlock()

    fsm, exist := px.values[seq]
    if exist {
        return fsm
    } else {
        return nil
    }
}

func (px *Paxos) createInstance(seq int, value interface{}) *PaxosFsm {
    px.mu.Lock()
    defer px.mu.Unlock()

    v, exist := px.values[seq]
    if exist {
        return v
    } else {
        atomic.StoreInt32(&px.max, int32(maxOf(px.Max(), seq)))
        fsm := makePaxosFsm(px.me, px.peers, seq, value)
        px.values[seq] = fsm

        return fsm
    }
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
    if seq < px.Min() {
        log.Printf("Start(result: tooMin, me: %d, min: %d, max: %d, seq: %d)", px.me, px.Min(), px.Max(), seq)
    } else if fsm, exists := px.values[seq]; exists {
        log.Printf("Start(result: exist, me: %d, min: %d, max: %d, seq: %d)", px.me, px.Min(), px.Max(), seq)

        fsm.SubmitValueIfNeeded(v)
    } else {
        log.Printf("Start(result: started, me: %d, min: %d, max: %d, seq: %d)", px.me, px.Min(), px.Max(), seq)

        fsm := px.createInstance(seq, v)
        fsm.Start()
    }
}


//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
    px.mu.Lock()
    defer px.mu.Unlock()

    px.peerMins[px.me] = int32(seq)
    px.syncMins(nil)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
    return int(atomic.LoadInt32(&px.max))
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
    return int(atomic.LoadInt32(&px.min)) + 1
}

func (px* Paxos) syncMins(peerMins []int) {
    old := px.Min()

    for peer, minV := range peerMins {
        px.peerMins[peer] = int32(maxOf(int(px.peerMins[peer]), minV))
    }

    atomic.StoreInt32(&px.min, minElement(px.peerMins))

    if old < px.Min() {
        log.Printf("PeerMin(me: %d, min: %d, max: %d)", px.me, px.Min(), px.Max())

        for max := px.Min(); old < max; old++ {
            delete(px.values, old)
        }
    }
}
//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
    px.mu.Lock()
    defer px.mu.Unlock()

    if seq < px.Min() {
        return Forgotten, nil
    } else {
        fsm, exist := px.values[seq]
        if exist {
            if fsm.IsDone() {
                return Decided, fsm.GetValue()
            } else {
                return Pending, nil
            }
        } else {
            log.Printf("Status(result: notFound, me: %d, seq: %d)", px.me, seq)
            return Forgotten, nil
        }
    }
}

////
// PaxosFsm RPC
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
    log.Printf("> Prepare(me: %d, from: %d, seq: %d, proposal: %d)", px.me, args.Me, args.Seq, args.Proposal)

    reply.Peer = px.me
    if args.Seq < px.Min() {
        log.Printf("< Prepare(result: seqTooLess, me: %s, min: %d, seq: %d)", px.me, px.Min(), args.Seq)
        reply.Accept = false
    } else {
        fsm := px.getInstance(args.Seq)

        if fsm == nil {
            log.Printf("< Prepare(result: newPaxos, me: %d, min: %d, max: %d, seq: %d)",
                px.me, px.Min(), px.Max(), args.Seq)

            fsm = px.createInstance(args.Seq, nil)
        }

        fsm.OnPrepare(args, reply)
    }

    return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
    log.Printf("> Accept(me: %d, from: %d, seq: %d, proposal: %d)", px.me, args.Me, args.Seq, args.Proposal)

    px.mu.Lock()
    reply.Peer = px.me
    reply.PeerMin = int(px.peerMins[px.me])
    px.mu.Unlock()

    if args.Seq < px.Min() {
        log.Printf("< Accept(result: seqTooLess, me: %s, min: %d, seq: %d)", px.me, px.Min(), args.Seq)
        reply.Accept = false
    } else {
        fsm := px.getInstance(args.Seq)

        if fsm == nil {
            log.Printf("< Prepare(result: newPaxos, me: %d, min: %d, max: %d, seq: %d)",
                px.me, px.Min(), px.Max(), args.Seq)

            fsm = px.createInstance(args.Seq, nil)
        }

        fsm.OnAccept(args, reply)
    }

    return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
    log.Printf("> Decide(me: %d, from: %d, seq: %d, proposal: %d)", px.me, args.Me, args.Seq, args.Proposal)

    px.syncMins(args.PeerMins)

    if args.Seq < px.Min() {
        log.Printf("< Decide(result: seqTooLess, me: %s, min: %d, seq: %d)", px.me, px.Min(), args.Seq)
    } else {
        fsm := px.getInstance(args.Seq)

        if fsm == nil {
            log.Printf("< Decide(result: notFound, me: %s, seq: %d)", px.me, args.Seq)
        } else {
            fsm.OnDecide(args, reply)
        }
    }

    return nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
    atomic.StoreInt32(&px.dead, 1)
    if px.l != nil {
        px.l.Close()
    }
}

func (px *Paxos) Finalize() {
    px.mu.Lock()
    defer px.mu.Unlock()

    for _, v := range px.values {
        v.Finalize()
    }

    px.values = nil
}

//
// has this peer been asked to shut down?
//
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

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
    px := &Paxos{}
    px.peers = peers
    px.me = me
    px.values = make(map[int]*PaxosFsm)
    px.peerMins = make([]int32, len(peers))
    px.min = -1
    px.max = -1

    for i := range px.peerMins {
        px.peerMins[i] = -1
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

            px.Finalize()
        }()
    }


    return px
}
