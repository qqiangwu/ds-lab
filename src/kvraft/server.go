package raftkv

import (
	"encoding/gob"
	"labrpc"
	"raft"
	"sync"
    "sync/atomic"
    "time"
)

type OpKind int
const (
    GET OpKind = iota
    PUT
    APPEND
    MULTIPUT
)

func opFromString(op string) OpKind {
    switch op {
    case "Put": return PUT
    case "Append": return APPEND
    case "MultiPut": return MULTIPUT
    default:
        panic("bad op")
    }
}

type Op struct {
    Kind      OpKind
    Client    int64
    Seq       int
    Batch     []KV
}

type PendingCall struct {
    index     int
    term      int
    reply     interface{}
    resultCh  chan bool
}

func (call *PendingCall) done() {
    call.resultCh <- true
}

func (call *PendingCall) cancel() {
    call.resultCh <- false
}

type Applier struct {
    kv      *RaftKV
	applyCh chan raft.ApplyMsg
    rf      *raft.Raft
    stopped int32

    // protected by mu
    pendingCalls     map[int]*PendingCall
    termOfPending    int
}

func makeApplier(kv *RaftKV) *Applier {
    ap := &Applier{}
    ap.kv = kv
    ap.applyCh = kv.applyCh
    ap.rf = kv.rf
    ap.pendingCalls = make(map[int]*PendingCall)

    go ap.run()

    return ap
}

func (ap *Applier) isRunning() bool {
    return atomic.LoadInt32(&ap.stopped) == 0
}

// @pre mu.Locked
func (ap *Applier) stop() {
    atomic.StoreInt32(&ap.stopped, 1)

    for index, call := range ap.pendingCalls {
        call.cancel()
        delete(ap.pendingCalls, index)
    }
}

// run in standalone goroutine
func (ap *Applier) run() {
    timeout := 100 * time.Millisecond

    for ap.isRunning() {
        select {
        case msg := <-ap.applyCh:
            if ap.isRunning() {
                DPrintf("Apply(me: %v, index: %v, msg: %v)", ap.kv.me, msg.Index, msg.Command)
                ap.apply(msg)
            }

        case <-time.After(timeout):
            ap.removeLegacy()
        }
    }
}

func (ap *Applier) removeLegacy() {
    ap.kv.mu.Lock()
    defer ap.kv.mu.Unlock()

    ap.removeLegacyWithLockHeld()
}

func (ap *Applier) removeLegacyWithLockHeld() {
    currentTerm, _ := ap.rf.GetState()
    if currentTerm == ap.termOfPending {
        return
    } else if currentTerm < ap.termOfPending {
        panic("currentTerm < ap.termOfPending @ removeLegacy")
    } else {
        for _, call := range ap.pendingCalls {
            call.cancel()
        }

        ap.pendingCalls = make(map[int]*PendingCall)
        ap.termOfPending = currentTerm
    }
}

// @pre ap.kv.mu.Locked
func (ap *Applier) apply(msg raft.ApplyMsg) {
    ap.kv.mu.Lock()
    defer ap.kv.mu.Unlock()

    op, ok := msg.Command.(Op)
    if !ok {
        return
    }

    call, exist := ap.pendingCalls[msg.Index]
    currentTerm, isLeader := ap.rf.GetState()
    if exist {
        // waiter on it
        if call.term < currentTerm || !isLeader {
            // legacy
            call.cancel()
            ap.kv.doDispatch(&op, nil)
        } else if call.term == currentTerm {
            ap.kv.doDispatch(&op, call.reply)
            call.done()
        } else {
            panic("precedent call.term")
        }

        delete(ap.pendingCalls, msg.Index)
    } else {
        // no waiter on index, simply apply
        ap.kv.doDispatch(&op, nil)
    }
}

// @pre mu.Locked
func (ap *Applier) waitOn(index int, term int, reply interface{}) <-chan bool {
    pendingCall := &PendingCall{ index, term, reply, make(chan bool, 1)}

    call, exist := ap.pendingCalls[index]
    if exist {
        // the index already exist a pending call, it must be left in the last leadership
        assert(call.term < term, "legacy pending call have equal or larger term")
        call.cancel()
    }

    ap.pendingCalls[index] = pendingCall
    if ap.termOfPending < term {
        ap.removeLegacyWithLockHeld()
        ap.termOfPending = term
    }

    return pendingCall.resultCh
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
    applier *Applier

	maxraftstate int // snapshot if log grows this big

    // protected by mu
    store       map[string]string
    dups        map[int64]int
}

// @pre kv.mu.Locked
func (kv *RaftKV) doDispatch(op *Op, reply interface{}) {
    switch op.Kind {
    case GET:
        if reply == nil {
            return
        } else if getReply, ok := reply.(*GetReply); ok {
            kv.doGet(op, getReply)
        } else {
            panic("bad get reply @ doDispatch")
        }

    case PUT:
        if reply == nil {
            kv.doPut(op, &PutAppendReply{})
        } else if putReply, ok := reply.(*PutAppendReply); ok {
            kv.doPut(op, putReply)
        } else {
            panic("bad put reply @ doDispatch")
        }

    case APPEND:
        if reply == nil {
            kv.doAppend(op, &PutAppendReply{})
        } else if appendReply, ok := reply.(*PutAppendReply); ok {
            kv.doAppend(op, appendReply)
        } else {
            panic("bad append reply @ doDispatch")
        }

    case MULTIPUT:
        if reply == nil {
            kv.doMultiPut(op, &MultiPutReply{})
        } else if mpReply, ok := reply.(*MultiPutReply); ok {
            kv.doMultiPut(op, mpReply)
        } else {
            panic("bad multiput reply @ doDispatch")
        }

    default:
        panic("invalid op")
    }
}

// @pre kv.mu.Locked
func (kv *RaftKV) doGet(op *Op, reply *GetReply) {
    val, exist := kv.store[op.Batch[0].Key]
    if exist {
        reply.Err = OK
        reply.Value = val
    } else {
        reply.Err = ErrNoKey
    }
}

// @pre kv.mu.Locked
func (kv *RaftKV) doPut(op *Op, reply *PutAppendReply) {
    isDup := getOrDefault(kv.dups, op.Client, -1) >= op.Seq
    if !isDup {
        // FIFO on the same client, so won't see the future
        kv.dups[op.Client] = op.Seq
        kv.store[op.Batch[0].Key] = op.Batch[0].Value
    }

    reply.Err = OK
}

// @pre kv.mu.Locked
func (kv *RaftKV) doMultiPut(op *Op, reply *MultiPutReply) {
    isDup := getOrDefault(kv.dups, op.Client, -1) >= op.Seq
    if !isDup {
        kv.dups[op.Client] = op.Seq

        for _, e := range op.Batch {
            kv.store[e.Key] = e.Value
        }
    }

    reply.Err = OK
}

// @pre kv.mu.Locked
func (kv *RaftKV) doAppend(op *Op, reply *PutAppendReply) {
    isDup := getOrDefault(kv.dups, op.Client, -1) >= op.Seq
    if !isDup {
        kv.dups[op.Client] = op.Seq
        v, ok := kv.store[op.Batch[0].Key]
        if ok {
            kv.store[op.Batch[0].Key] = v + op.Batch[0].Value
        } else {
            kv.store[op.Batch[0].Key] = op.Batch[0].Value
        }
    }

    reply.Err = OK
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
    op := Op{}
    op.Kind = GET
    op.Client = -1
    op.Seq = -1
    op.Batch = []KV{ {args.Key, ""} }

    kv.mu.Lock()

    index, term, isLeader := kv.rf.Start(op)
    if !isLeader {
        reply.WrongLeader = true
        kv.mu.Unlock()
    } else {
        ch := kv.applier.waitOn(index, term, reply)
        kv.mu.Unlock()

        if !<-ch {
            reply.WrongLeader = true
        }
    }
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    op := Op{}
    op.Kind = opFromString(args.Op)
    op.Client = args.Client
    op.Seq = args.Seq
    op.Batch = []KV{ {args.Key, args.Value} }

    kv.mu.Lock()

    index, term, isLeader := kv.rf.Start(op)
    if !isLeader {
        reply.WrongLeader = true
        kv.mu.Unlock()
    } else {
        ch := kv.applier.waitOn(index, term, reply)
        kv.mu.Unlock()

        if !<-ch {
            reply.WrongLeader = true
        }
    }
}

func (kv *RaftKV) MultiPut(args *MultiPutArgs, reply *MultiPutReply) {
    op := Op{}
    op.Kind = MULTIPUT
    op.Client = args.Client
    op.Seq = args.Seq
    op.Batch = args.Batch

    kv.mu.Lock()

    index, term, isLeader := kv.rf.Start(op)
    if !isLeader {
        reply.WrongLeader = true
        kv.mu.Unlock()
    } else {
        ch := kv.applier.waitOn(index, term, reply)
        kv.mu.Unlock()

        if !<-ch {
            reply.WrongLeader = true
        }
    }
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    kv.applier.stop()
	kv.rf.Kill()
}

// no need to lock
func (kv *RaftKV) Raft() *raft.Raft {
    return kv.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
    kv.store = make(map[string]string)
    kv.dups = make(map[int64]int)
    kv.applier = makeApplier(kv)

	return kv
}
