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

// 错误模型：
// nodes: client不会失效/server不会crash
// network: 会出现partition，同时，发送的请求会出现收不到响应，且在请求执行前就返回了，对应现实情况: 服务器慢，客户端超时，重试，然后服务器又开始执行了
//
// 问题1:ck给m0发送请求，请求还没有执行，又给m1发送了请求，m1执行完成后，又执行了一下一个操作，此时，m0上的操作又开始执行，造成1,2,1的执行
// 问题2:ck waitFailed，可能是它获取一个commitId后，在等待结果时，另一个ck get获取了一个新的commitId，并且执行完成，然后apply了

/**
现在的协议本身有一些问题，出现一个场景
me1: 正在处理150，然后处理了151，此时，150还没有处理完毕

clerk观察到151已经处理完毕，于是发起新的请求，新的请求到了me1，me1尝试150，然后运气很好的抢到了150这个位置的决定权
*/

/*
解决了上面的问题，又发现了新的问题 Put的幂等问题。Put虽然是幂等的，但是，如果重复的Put之间有一个其他的Append，或者说，迟到的Put，即在Clerk看来，
Put已经完成了，所以它执行了后面的操作。但是之前，可能有一个慢的RPC到达了，它又执行了，于是乎系统状态就被归0了。
*/

var Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug > 0 {
        log.Printf(format, a...)
    }
    return
}

func SetLogLevel(level int) {
    Debug = level
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
    Offset  uint64
    Clerk   int64
    Id      int64
}

func equals(a *Op, b *Op) bool {
    return a.Sender == b.Sender && a.Offset == b.Offset
}

// committed but not applied: [applyPoint, commitPoint)
type OpLog struct {
    mu           sync.Mutex

    applyPoint   uint32
    commitPoint  uint32
}

func (log *OpLog) getApplyPoint() int {
    log.mu.Lock()
    defer log.mu.Unlock()

    return int(log.applyPoint)
}

func (log *OpLog) apply(applyPoint uint32) {
    log.mu.Lock()
    defer log.mu.Unlock()

    if applyPoint < log.applyPoint {
        panic("Bad applyPoint: alreay applied")
    } else {
        log.applyPoint = applyPoint + 1
    }
}

func (log *OpLog) nextCommitPoint() int {
    log.mu.Lock()
    defer log.mu.Unlock()

    ret := log.commitPoint
    log.commitPoint += 1

    return int(ret)
}

type KVPaxos struct {
    mu         sync.Mutex
    l          net.Listener
    me         int
    dead       int32 // for testing
    unreliable int32 // for testing
    px         *paxos.Paxos

    store      map[string]string
    dups       map[int64]int64
    log        OpLog
}

// apply log to store
func (kv *KVPaxos) apply(op *Op) {
    if oldId, exists := kv.dups[op.Clerk]; exists {
        if oldId >= op.Id {
            DPrintf("DupRPC(me: %v, key: %v, clerk: %v, id: %v)", kv.me, op.Key, op.Clerk, op.Id)
            return
        }
    }

    switch op.Type {
    case PUT:
        kv.store[op.Key] = op.Value

    case APPEND:
        if v, ok := kv.store[op.Key]; ok {
            kv.store[op.Key] = v + op.Value
        } else {
            kv.store[op.Key] = op.Value
        }
    }

    kv.dups[op.Clerk] = op.Id
}

// apply all logs <= seq
func (kv *KVPaxos) catchup(seq int) {
    for i := kv.log.getApplyPoint(); i <= seq; i++ {
        v, notGarbage := kv.wait(i)
        if notGarbage {
            kv.apply(v)
        }
    }

    kv.px.Done(seq)
    kv.log.apply(uint32(seq))
    DPrintf("Catchup(me: %d, seq: %d)", kv.me, seq)
}

func (kv *KVPaxos) wait(seq int) (*Op, bool) {
    const MAX_WAIT = time.Second
    to := 5 * time.Millisecond
    for {
        status, v := kv.px.Status(seq)

        switch status {
        case paxos.Decided:
            if o, ok := v.(Op); ok {
                return &o, true
            } else {
                return nil, false
            }

        case paxos.Pending:
            time.Sleep(to)
            if to < MAX_WAIT {
                to *= 2
            }

        case paxos.Forgotten:
            if seq < kv.px.Min() {
                DPrintf("WaitFailed(me: %d, seq: %d, min: %d)", kv.me, seq, kv.px.Min())
                return nil, false
            }
            // we haven't heard of the seq(others might know it because of partitioning), so propose a new one
            kv.px.Start(seq, Op{})

        default:
            log.Panicf("BadSeq(result: unknown, me: %d, seq: %d, status: %d)", kv.me, seq, status)
        }
    }
}

// append the log entry and return its final offset/seq
func (kv *KVPaxos) appendLog(op *Op) int {
    op.Sender = kv.me
    for {
        seq := kv.log.nextCommitPoint()
        op.Offset = uint64(seq)
        kv.px.Start(seq, *op)
        o, notGarbage := kv.wait(seq)
        if notGarbage {
            if equals(o, op) {
                DPrintf("AppendLog(me: %d, seq: %d, key: %s, ck: %d, id: %d)", kv.me, seq, op.Key, op.Clerk, op.Id)
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

    start := time.Now()

    op := &Op{GET, args.Key, "", 0, 0, 0, 0}

    kv.mu.Lock()
    offset := kv.appendLog(op)
    kv.catchup(offset)
    kv.localGet(args, reply)
    kv.mu.Unlock()

    elapse := time.Since(start)
    DPrintf("Get(me: %d, key: %s, elapse: %s)", kv.me, args.Key, elapse)

    return nil
}

func (kv *KVPaxos) buildOp(args *PutAppendArgs) *Op {
    if args.Op == "Put" {
        return &Op{PUT, args.Key, args.Value, 0, 0, args.Clerk, args.Id}
    } else {
        return &Op{APPEND, args.Key, args.Value, 0, 0, args.Clerk, args.Id}
    }
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
    DPrintf("PutAppend(me: %d, key: %s, ck: %d, id: %d)", kv.me, args.Key, args.Clerk, args.Id)
    start := time.Now()

    op := kv.buildOp(args)

    kv.mu.Lock()
    kv.appendLog(op)
    kv.mu.Unlock()

    reply.Err = OK

    elapse := time.Since(start)
    DPrintf("PutAppend(me: %d, key: %s, elapse: %s)", kv.me, args.Key, elapse)

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
    kv.store = make(map[string]string)
    kv.dups  = make(map[int64]int64)

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
