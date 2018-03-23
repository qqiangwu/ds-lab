package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
    "sync/atomic"
)

type RaftKV struct {
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	maxraftstate int

    // protected by mu
	mu      sync.Mutex
    store   map[string]string
    dups    map[int64]int
    pending map[int]*PendingCall    // index -> call
}

func (kv *RaftKV) onLoseLeader() {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    for _, call := range kv.pending {
        call.error <- ErrWrongLeader
    }

    kv.pending = nil
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
}

func (kv *RaftKV) Kill() {
    close(kv.applyCh)
	kv.rf.Kill()
}

func (kv *RaftKV) apply() {
    for msg := range kv.applyCh {
        op, ok := msg.(Op)
        if !ok {
            continue
        }

        kv.doApply(msg.Index, op)
    }
}

func (kv *RaftKV) doApply(index int, op Op) {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    pending, ok := kv.pending[index]
    delete(kv.pending, index)

    if op.needDedup() {
        if kv.dups[op.Client] >= op.Seq {
            if ok {
                pending.error <- ErrDup
            }
            return
        }

        kv.dups[op.Client] = op.Seq
    }

    result := op.Command.Apply(op, kv)
    if ok {
        pending.result <- result
    }
}

type Command interface {
    Apply(op Op, kv *RaftKV) interface{}
}

type Op struct {
    Client  int64
    Seq     int
    Batch   []KV
    Command Command
}

func (op *Op) needDedup() bool {
    return op.Seq > 0
}

type PendingCall struct {
    index   int
    error   chan(string)
    result  chan(interface{})
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
    kv.rf.SetCallback(nil, kv.OnLoseLeader)

    go kv.apply()

	return kv
}
