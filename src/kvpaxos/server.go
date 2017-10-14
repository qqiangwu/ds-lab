package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import (
    "math/rand"
    "time"
)


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug > 0 {
        log.Printf(format, a...)
    }
    return
}

type OpType int
const (
    GET OpType  = iota
    PUT
    APPEND
)

type Op struct {
    Type    OpType
    Key     string
    Value   string
    Sender  int
    LocalId uint64
}

func equals(a *Op, b *Op) bool {
    return a.Sender == b.Sender && a.LocalId == b.LocalId
}

type KVPaxos struct {
    mu         sync.Mutex
    l          net.Listener
    me         int
    dead       int32 // for testing
    unreliable int32 // for testing
    px         *paxos.Paxos

    store      map[string]string
    localId    uint64
}

func (kv *KVPaxos) nextId() uint64 {
    return atomic.AddUint64(&kv.localId, 1)
}

// apply log to store
func (kv *KVPaxos) commit(op *Op) {
    switch op.Value {
    case PUT:
        kv.store[op.Key] = op.Value

    case APPEND:
        if v, ok := kv.store[op.Key]; ok {
            kv.store[op.Key] += v
        } else {
            kv.store[op.Key] = v
        }
    }
}

// apply all logs <= seq
func (kv *KVPaxos) catchup(seq int) {
    for i := kv.px.Min(); i <= seq; i++ {
        v, notGarbage := kv.wait(i)
        if notGarbage {
            kv.commit(v)
        }
    }

    kv.px.Done(seq)
    DPrintf("Catchup(me: %d, seq: %d)", kv.me, seq)
}

func (kv *KVPaxos) wait(seq int) (*Op, bool) {
    const MAX_WAIT = time.Second
    to := 5 * time.Millisecond
    for {
        status, v := kv.px.Status(seq)

        switch status {
        case paxos.Decided:
            if o, ok := v.(*Op); ok {
                return o, true
            } else {
                return nil, false
            }

        case paxos.Pending:
            time.Sleep(to)
            if to < MAX_WAIT {
                to *= 2
            }

        case paxos.Forgotten:
            // we haven't heard of the seq(others might know it because of partitioning), so propose a new one
            kv.px.Start(seq, &Op{})

        default:
            log.Panicf("BadSeq(result: unknown, me: %d, seq: %d, status: %d)", kv.me, seq, status)
        }
    }
}

// append the log entry and return its final offset/seq
func (kv *KVPaxos) appendLog(op *Op) int {
    for {
        seq := kv.px.Max() + 1
        kv.px.Start(seq, op)
        o, notGarbage := kv.wait(seq)
        if notGarbage {
            if equals(o, op) {
                return seq
            } else {
                DPrintf("AppendLog(result: conflicts, me: %d, seq: %d)", kv.me, seq)
            }
        } else {
            DPrintf("AppendLog(result: nonOp, me: %d, seq: %d)", kv.me, seq)
        }
    }
}

func (kv *KVPaxos) localGet(args *GetArgs, reply *GetReply) {
    val, exists := kv.store[args.Key]
    if exists {
        reply.Err = OK
        reply.Value = val
    } else {
        reply.Err = ErrNoKey
    }
}

// FIXME:   Get is too slow
func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
    DPrintf("Get(me: %d, key: %s, min: %d, max: %d)", kv.me, args.Key, kv.px.Min(), kv.px.Max())

    op := &Op{GET, args.Key, "", kv.me, kv.nextId()}

    kv.mu.Lock()
    defer kv.mu.Unlock()

    offset := kv.appendLog(op)
    kv.catchup(offset)
    kv.localGet(args, reply)

    return nil
}

func (kv *KVPaxos) buildOp(args *PutAppendArgs) *Op {
    if args.Op == "Put" {
        return &Op{PUT, args.Key, args.Value, kv.me, kv.nextId()}
    } else {
        return &Op{APPEND, args.Key, args.Value, kv.me, kv.nextId()}
    }
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
    DPrintf("PutAppend(me: %d, key: %s, min: %d, max: %d)", kv.me, args.Key, kv.px.Min(), kv.px.Max())

    op := kv.buildOp(args)
    kv.appendLog(op)
    reply.Err = OK

    return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
    DPrintf("Kill(%d): die\n", kv.me)
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
    gob.Register(&Op{})

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
