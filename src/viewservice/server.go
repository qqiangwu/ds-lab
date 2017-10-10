package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
    mu       sync.Mutex
    l        net.Listener
    dead     int32 // for testing
    rpccount int32 // for testing
    me       string

    // Your declarations here.
    monitor  Monitor
    fsm      ViewFsm
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
    vs.mu.Lock()
    defer vs.mu.Unlock()

    viewNum, exists := vs.monitor.Get(args.Me)
    if !exists {
        log.Printf("NewServer(server: %s, view: %d)", args.Me, args.Viewnum)
        vs.monitor.OnPing(args.Me, args.Viewnum)
        vs.fsm.OnNew(args.Me)
    } else if viewNum <= args.Viewnum {
        // ping
        vs.monitor.OnPing(args.Me, args.Viewnum)
        if args.Me == vs.fsm.GetView().Primary && args.Viewnum == vs.fsm.GetView().Viewnum {
            vs.fsm.OnPrimaryAck()
        }
    } else {
        log.Printf("Restarted(server: %s, view: %d, currentView: %d)", args.Me, args.Viewnum, viewNum)
        vs.monitor.OnPing(args.Me, args.Viewnum)
        vs.fsm.OnRestarted(args.Me)
    }

    reply.View = vs.fsm.GetView()

    return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
    vs.mu.Lock()
    defer vs.mu.Unlock()

    reply.View = vs.fsm.GetView()

    return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
    vs.mu.Lock()
    defer vs.mu.Unlock()

    vs.monitor.Tick()
    if expired := vs.monitor.GetExpired(); len(expired) != 0 {
        vs.fsm.OnExpire(expired)
    }
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
    atomic.StoreInt32(&vs.dead, 1)
    vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
    return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
    return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
    vs := new(ViewServer)
    vs.me = me
    vs.monitor.Init()
    vs.fsm.Init(&vs.monitor)

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
        for vs.isdead() == false {
            conn, err := vs.l.Accept()
            if err == nil && vs.isdead() == false {
                atomic.AddInt32(&vs.rpccount, 1)
                go rpcs.ServeConn(conn)
            } else if err == nil {
                conn.Close()
            }
            if err != nil && vs.isdead() == false {
                fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
                vs.Kill()
            }
        }
    }()

    // create a thread to call tick() periodically.
    go func() {
        for vs.isdead() == false {
            vs.tick()
            time.Sleep(PingInterval)
        }
    }()

    return vs
}
