package shardkv

import "shardmaster"
import "labrpc"
import "raft"
import "kvraft"
import "sync"
import "sync/atomic"
import "time"
import "encoding/gob"
import "encoding/base64"
import "bytes"
import "errors"

////////
// Daemon
type Refresher struct {
    stopped    int32
    fn         func()
}

func (rf *Refresher) isRunning() bool {
    return atomic.LoadInt32(&rf.stopped) == 0
}

func (rf *Refresher) stop() {
    atomic.StoreInt32(&rf.stopped, 1)
}

func (rf *Refresher) run() {
    const interval = 100 * time.Millisecond

    for rf.isRunning() {
        rf.fn()
    }
}

////////
// Daemon
type Cleaner struct {
    stopped   int32
    fn        func()

    // protected by mu
    mu        sync.Mutex
    cond      *sync.Cond
    needClean bool
}

func makeCleaner(fn func()) *Cleaner {
    cl := new(Cleaner)
    cl.fn = fn
    cl.cond = sync.NewCond(&cl.mu)

    return cl
}

func (cl *Cleaner) isRunning() bool {
    return atomic.LoadInt32(&cl.stopped) == 0
}

func (cl *Cleaner) stop() {
    atomic.StoreInt32(&cl.stopped, 1)
}

func (cl *Cleaner) run() {
    cl.mu.Lock()
    defer cl.mu.Unlock()

    for cl.isRunning() {
        for !cl.needClean {
            cl.cond.Wait()

            if !cl.isRunning() {
                return
            }
        }

        cl.needClean = false
        cl.mu.Unlock()
        cl.fn()
        cl.mu.Lock()
    }
}

////////
// KvState
type ShardState int
const (
    NOT_OWNED ShardState = iota
    OWNED,
    SEND,
    RECV
)

type Shard struct {
    Id      int
    State   ShardState
    Owner   int
}

type KvState struct {
    Term        int
    Shard       [shardmaster.NSHARDS]Shard
}

func (state *KvState) encode() string {
    buffer := bytes.Buffer{}
    encoder := gob.NewEncoder(&buffer)
    err := encoder.Encode(state)
    if err != nil {
        panic("encode config failed")
    }
    return base64.StdEncoding.EncodeToString(buffer.Bytes())
}

func decode(repr string) KvState {
    var state KvState

    if len(repr) != 0 {
        b, err := base64.StdEncoding.DecodeString(repr)
        if err != nil {
            panic("decode KvState failed")
        }

        buffer := bytes.Buffer{}
        buffer.Write(b)
        decoder := gob.NewDecoder(&buffer)
        err = decoder.Decode(&state)
        if err != nil {
            panic("decode KvState failed")
        }
    }

    return state
}

////////
var (
    ErrLegacyGroup = errors.New("Wrong group")
)

const META_CONFIG = "meta.config"

type ShardKV struct {
	mu           sync.Mutex
	me           int
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int
    refresher    *Refresher
    cleaner      *Cleaner

    // protected by mu
    shardTransfered *sync.Cond
    store         *raftkv.RaftKV
    rf            *raft.Raft
    mck           *shardmaster.Clerk
}

// run in background
func (kv *ShardKV) doSync() {
    kv.mu.Lock()
    if kv.isLeader() {
        kv.syncImpl()
    }
    kv.mu.Unlock()
}

func (kv *ShardKV) doClean() {
    // TODO
}

func (kv *ShardKV) isLeader() bool {
    return kv.store.IsLeader()
}

// @pre kv.mu.Locked
func (kv *ShardKV) syncImpl() {
    oldConfig := kv.config
    kv.config = kv.mck.Query(-1)
    kv.onConfigChange(oldConfig, kv.config)
}

// @pre kv.mu.Locked
func (kv *ShardKV) onConfigChange(oldCfg shardmaster.Config, newCfg shardmaster.Config) {
    // map: shard -> gid
    movePlan := make(map[int]int)
    for shard, gid := range oldConfig.Shards {
        if gid == kv.gid {
            if newGid := kv.config.Shards[shard]; newGid != gid {
                movePlan[shard] = newGid
            }
        }
    }
}

// @pre kv.mu.Locked
func (kv *ShardKV) syncConfigIfNecessary(num int) KvState, error {
    repr, err := kv.store.KvGet(META_CONFIG)

    if err == nil {
        state := decode(repr)

        if num > state.Term {

        } else if num < state.Term {

        }
    }
}

// @pre kv.mu.Locked
func (kv *ShardKV) serveKey(key string) bool {
    shard := key2shard(key)
    state, err := kv.store.KvGet(META_CONFIG)
}

// @pre kv.mu.Locked
func (kv *ShardKV) syncState(oldState *KvState) KvState, error {
    conf := kv.mck.Query(-1)
    err := kv.store.KvPutV(META_CONFIG, state.encode(), state.Term)
    return state, err
}

// @pre kv.mu.Locked
func (kv *ShardKV) canServe(term int, key string) bool, error {
    shard := key2shard(key)

    ready := false
    repr, err := kv.store.KvGet(META_CONFIG)
    if err == nil {
        state := decode(repr)

        switch {
        case state.Term > term:
            err = ErrLegacyGroup

        case state.Term < term:
            state, err = kv.syncState(&state)
            if err == nil {
                if state.Term > term {
                    err = ErrLegacyGroup
                } else state.Term < term {
                    // bad client term
                    panic("bad client term")
                }
            } else if err == raftkv.ErrLegacyVersion {
                // translate error
                if kv.isLeader() {
                    err = ErrLegacyGroup
                } else {
                    err = raftkv.ErrNotLeader
                }
            }
        }

        if err == nil {
            switch state.Shard[shard].State {
            case OWNED:
                ready = true
            case RECV:
            default:
                err = ErrLegacyGroup
            }
        }
    }

    return ready, err
}

// in the process of request, leadership should not change, otherwise abort tx
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    var err error
    if !kv.isLeader() {
        err = raftkv.ErrNotLeader
    }

    if err == nil {
        ready := false
        ready, err = kv.canServe(args.ConfigNum, args.Key)
        for !ready && err == nil {
            kv.shardTransfered.Wait()
            ready, err = kv.canServe(args.ConfigNum, args.Key)
        }
    }

    if err == nil {
        reply.Value, err = kv.store.KvGet(args.Key)
    }

    if err != nil {
        switch err {
        case raftkv.ErrNotLeader: reply.WrongLeader = true
        case raftkv.ErrNotFound: reply.Err = ErrNoKey
        case ErrLegacyGroup: reply.Err = ErrWrongGroup
        default:
            panic("unknown error:" + err.Error())
        }
    } else {
        reply.Err = OK
    }
}

// TODO
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
}

func (kv *ShardKV) SyncShard(args *SyncShardArgs, reply *SyncShardReply) {
    kv.mu.Lock()
    defer kv.mu.Unlock()

    kv.shardTransfered.Broadcast()
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
    kv.refresher.stop()
    kv.cleaner.stop()
	kv.store.Kill()
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	kv := new(ShardKV)
	kv.me = me
    kv.shardTransfered = sync.NewCond(&kv.mu)
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
    kv.store = raftkv.StartKVServer(servers, me, persister, maxraftstate)
    kv.rf = kv.store.rf.Raft()
    kv.mck = shardmaster.MakeClerk(kv.masters)
    kv.refresher = &Refresher{fn: kv.doSync}
    kv.cleaner = makeCleaner(kv.doClean)

    go kv.refresher.run()
    go kv.cleaner.run()
    go kv.doRecover()

	return kv
}
