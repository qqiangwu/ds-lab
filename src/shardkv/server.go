package shardkv

import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "sync/atomic"
import "encoding/gob"
import "time"
import "bytes"

const NSHARD = shardmaster.NShards

type Op struct {
	Command Command
}

type PendingCall struct {
	index  int
	term   int
	result chan (interface{})
	error  chan (error)
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	persister    *raft.Persister
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	mck          *shardmaster.Clerk
	maxraftstate int

	stopped  int32
	isleader int32

	// protected by mu
	confChanged    *sync.Cond
	confChangeDone *sync.Cond
	syncRequired   *sync.Cond
	shardReceived  *sync.Cond
	pendingCalls   map[int]*PendingCall

	// protected by mu, and mutations must go raft
	confNum int
	conf    shardmaster.Config
	meta    [NSHARD]ShardMeta
	shards  [NSHARD]ShardStore
}

type ShardState int

const (
	NOT_OWNED ShardState = iota
	OWNED
	RECV
	SEND
)

type ShardMeta struct {
	State ShardState
	Owner int
}

type ShardStore struct {
	Version int
	Kv      map[string]string
	Dups    map[int64]int
}

func (store *ShardStore) clear() {
	store.Version = 0
	store.Kv = nil
	store.Dups = nil
}

// @pre kv.mu.Locked
func (kv *ShardKV) isChanging() bool {
	for _, meta := range kv.meta {
		if meta.State == SEND || meta.State == RECV {
			return true
		}
	}

	return false
}

func (kv *ShardKV) checkConfChangeFinished() bool {
	if !kv.isLeader() {
		return false
	}

	if kv.isChanging() {
		return false
	}

	// done
	DPrintf("ConfChangeDone(me: %v, conf: %v)", kv.gid, kv.confNum)
	kv.confChangeDone.Broadcast()

	return true
}

// @pre kv.mu.Locked
func (kv *ShardKV) modifyOwnership(conf shardmaster.Config) {
	oldConf := kv.conf
	newConf := conf

	for shardId := range kv.meta {
		if oldConf.Shards[shardId] != kv.gid && newConf.Shards[shardId] == kv.gid {
			if oldConf.Shards[shardId] == 0 {
				kv.meta[shardId].State = OWNED
			} else {
				kv.meta[shardId].State = RECV
			}
		}
		if oldConf.Shards[shardId] == kv.gid && newConf.Shards[shardId] != kv.gid {
			target := newConf.Shards[shardId]
			if target != 0 {
				kv.meta[shardId].State = SEND
				kv.meta[shardId].Owner = target
			} else {
				kv.meta[shardId].State = NOT_OWNED
			}
		}
	}
}

func (kv *ShardKV) needSync() {
	kv.syncRequired.Signal()
}

// @pre kv.mu.Locked
func (kv *ShardKV) onBecomeLeader(term int) {
	atomic.StoreInt32(&kv.isleader, 1)
	kv.needSync()
}

// @pre kv.mu.Locked
func (kv *ShardKV) onLoseLeader() {
	atomic.StoreInt32(&kv.isleader, 0)

	kv.confChanged.Broadcast()
	kv.confChangeDone.Broadcast()
	kv.shardReceived.Broadcast()

	for index, call := range kv.pendingCalls {
		call.error <- ErrNotLeader
		delete(kv.pendingCalls, index)
	}
}

// daemon

// @pre kv.mu.Locked
func (kv *ShardKV) doSnapshot(index int, term int) {
	DPrintf("DoSnapshot(me: %v-%v, index: %v, term: %v)", kv.gid, kv.me, index, term)

	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)

	encoder.Encode(&index)
	encoder.Encode(&term)
	encoder.Encode(&kv.confNum)
	encoder.Encode(&kv.conf)
	encoder.Encode(&kv.meta)
	encoder.Encode(&kv.shards)

	kv.persister.SaveSnapshot(buffer.Bytes())
	kv.rf.OnSnapshot(index, term)
}

// @pre kv.mu.Locked
func (kv *ShardKV) loadSnapshot(data []byte) (index int, term int) {
	if data == nil || len(data) == 0 {
		return 0, 0
	}

	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	decoder.Decode(&index)
	decoder.Decode(&term)
	decoder.Decode(&kv.confNum)
	decoder.Decode(&kv.conf)
	decoder.Decode(&kv.meta)
	decoder.Decode(&kv.shards)

	DPrintf("LoadSnapshot(me: %v-%v, index: %v, term: %v)", kv.gid, kv.me, index, term)

	return index, term
}

func (kv *ShardKV) applyBG() {
	for msg := range kv.applyCh {
		kv.doApply(msg)
	}
}

func (kv *ShardKV) doApply(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch msg.Type {
	case raft.USE_SNAPSHOT:
		kv.loadSnapshot(msg.Snapshot)
		kv.doSnapshot(msg.Index, msg.Term)

	case raft.DO_SNAPSHOT:
		kv.doSnapshot(msg.Index, msg.Term)

	case raft.GET_LEADER:
		kv.onBecomeLeader(0)

	case raft.LOSE_LEADER:
		kv.onLoseLeader()

	default:
		op, ok := msg.Command.(Op)
		if !ok {
			panic("Bad op when apply")
		}

		DPrintf("ShardKV(me: %v-%v, conf: %v, term:%v, index: %v, msg: %v)", kv.gid, kv.me, kv.confNum, msg.Term, msg.Index, op.Command.Name())
		call, exist := kv.pendingCalls[msg.Index]
		assert(!exist || call.term == msg.Term, "Bad pending call")

		result, err := op.Command.Apply(kv)
		if exist {
			if err != nil {
				call.error <- err
			} else {
				call.result <- result
			}

			delete(kv.pendingCalls, msg.Index)
		}
	}

}

func (kv *ShardKV) snapshotBG(persister *raft.Persister, maxsize int) {
	if maxsize <= 0 {
		return
	}

	for kv.isRunning() {
		if persister.RaftStateSize() > maxsize {
			kv.rf.Snapshot()
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) pollBG() {
	for kv.isRunning() {
		time.Sleep(250 * time.Millisecond)

		if kv.isLeader() {
			kv.syncRequired.Signal()
		}
	}
}

func (kv *ShardKV) syncBG() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for kv.isRunning() {
		if kv.isLeader() {
			needWait := false

			if kv.isChanging() {
				needWait = true
			} else {
				conf := kv.mck.Query(kv.confNum + 1)
				if conf.Num > kv.confNum {
					op := Op{}
					op.Command = ConfChangeCommand{conf}
					_, _, isLeader := kv.rf.Start(op)
					if isLeader {
						DPrintf("SyncStart(me: %v, old: %v, new: %v)", kv.gid, kv.confNum, conf.Num)
						kv.confChanged.Wait()

						for shardId, shardMeta := range kv.meta {
							if shardMeta.State == SEND && kv.isLeader() {
								kv.doSync(kv.confNum, shardId, shardMeta.Owner, copyShard(kv.shards[shardId]))
							}
						}

						if !kv.checkConfChangeFinished() {
							// BUGGY HERE
							needWait = true
						} else {
							DPrintf("SyncEnd(me: %v, new: %v)", kv.gid, kv.confNum)
						}
					}
				}
			}

			if needWait && kv.isLeader() {
				DPrintf("SyncWait(me: %v, new: %v)", kv.gid, kv.confNum)
				kv.confChangeDone.Wait()
			}
		}

		kv.syncRequired.Wait()
	}
}

// @pre kv.mu.Locked
func (kv *ShardKV) doSync(confNum int, shardId int, target int, shard ShardStore) {
	kv.mu.Unlock()
	defer kv.mu.Lock()

	DPrintf("DoSync(from: %v, to: %v, confNum: %v, shardId: %v)", kv.gid, target, confNum, shardId)
	ck := MakeClerk(kv.masters, kv.make_end)
	ck.SendShard(confNum, target, shardId, shard)

	op := Op{}
	op.Command = SendDoneCommand{confNum, shardId}

	// if lost, then another leader will take over the task
	kv.rf.Start(op)
}

func (kv *ShardKV) isRunning() bool {
	return atomic.LoadInt32(&kv.stopped) == 0
}

func (kv *ShardKV) isLeader() bool {
	return atomic.LoadInt32(&kv.isleader) == 1
}

// @pre kv.mu.Locked
func (kv *ShardKV) ensureConfNum(confNum int) error {
	if confNum < kv.confNum {
		return ErrLegacyConf
	}

	for kv.confNum < confNum {
		kv.needSync()

		if kv.confNum+1 == confNum {
			kv.confChanged.Wait()
		} else {
			kv.confChangeDone.Wait()
		}

		if !kv.isLeader() {
			return ErrNotLeader
		}
	}

	return nil
}

// @pre kv.mu.Locked
func (kv *ShardKV) canServeKey(key string) error {
	shard := key2shard(key)
	meta := kv.meta[shard]

	switch meta.State {
	case NOT_OWNED:
		return ErrWrongServer

	case OWNED:
		return nil

	case SEND:
		return ErrWrongServer

	case RECV:
		for kv.meta[shard].State != OWNED && kv.isLeader() {
			kv.shardReceived.Wait()
		}

		if kv.isLeader() {
			return nil
		} else {
			return ErrNotLeader
		}
	}

	panic("wont go here")
}

// @pre kv.mu.Locked
func (kv *ShardKV) waitOn(index int, term int) (<-chan interface{}, <-chan error) {
	call := &PendingCall{index, term, make(chan interface{}), make(chan error)}

	kv.pendingCalls[index] = call

	return call.result, call.error
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()

	var err error

	if !kv.isLeader() {
		err = ErrNotLeader
	}

	if err == nil {
		err = kv.ensureConfNum(args.ConfNum)
	}
	if err == nil {
		err = kv.canServeKey(args.Key)
	}
	if err == nil {
		op := Op{}
		op.Command = GetCommand{args.ConfNum, args.Key}
		index, term, isLeader := kv.rf.Start(op)
		if !isLeader {
			err = ErrNotLeader
		}

		if err == nil {
			resultCh, errCh := kv.waitOn(index, term)
			kv.mu.Unlock()

			select {
			case r := <-resultCh:
				reply.Err = OK
				reply.Value = r.(string)
				return

			case err = <-errCh:
				kv.mu.Lock()
			}
		}
	}

	kv.mu.Unlock()

	if err != nil {
		switch err {
		case ErrNotLeader:
			reply.WrongLeader = true

		case ErrWrongServer:
			reply.Err = ErrWrongGroup

		case ErrLegacyConf:
			reply.Err = ErrWrongGroup

		case ErrNotFound:
			reply.Err = ErrNoKey

		default:
			panic("unknown error: " + err.Error())
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()

	var err error

	if !kv.isLeader() {
		err = ErrNotLeader
	}

	if err == nil {
		err = kv.ensureConfNum(args.ConfNum)
	}
	if err == nil {
		err = kv.canServeKey(args.Key)
	}
	if err == nil {
		op := Op{}
		if args.Op == "Put" {
			op.Command = PutCommand{
				args.ConfNum,
				args.Key,
				args.Value,
				args.Client,
				args.Seq}
		} else {
			op.Command = AppendCommand{
				args.ConfNum,
				args.Key,
				args.Value,
				args.Client,
				args.Seq}
		}
		index, term, isLeader := kv.rf.Start(op)
		if !isLeader {
			err = ErrNotLeader
		}

		if err == nil {
			resultCh, errCh := kv.waitOn(index, term)
			kv.mu.Unlock()

			select {
			case <-resultCh:
				reply.Err = OK
				return

			case err = <-errCh:
				kv.mu.Lock()
			}
		}
	}

	kv.mu.Unlock()

	if err != nil {
		switch err {
		case ErrNotLeader:
			reply.WrongLeader = true

		case ErrWrongServer:
			reply.Err = ErrWrongGroup

		case ErrLegacyConf:
			reply.Err = ErrWrongGroup

		case ErrNotFound:
			reply.Err = ErrNoKey

		case ErrDupCall:
			reply.Err = OK

		default:
			panic("unknown error: " + err.Error())
		}
	}
}

func (kv *ShardKV) SendShard(args *SendArgs, reply *SendReply) {
	kv.mu.Lock()

	var err error

	if !kv.isLeader() {
		err = ErrNotLeader
	}

	DPrintf("RecvShard(me: %v, conf: %v, argConf: %v, shardId: %v)", kv.gid, kv.confNum, args.ConfNum, args.ShardId)

	if err == nil {
		err = kv.ensureConfNum(args.ConfNum)
	}
	if err == nil {
		if kv.confNum > args.ConfNum {
			err = ErrLegacyConf
		} else if kv.meta[args.ShardId].State != RECV {
			err = ErrDupCall
		}
	}
	if err == nil {
		op := Op{}
		op.Command = RecvShardCommand{args.ConfNum, args.ShardId, args.Store}
		index, term, isLeader := kv.rf.Start(op)
		if !isLeader {
			err = ErrNotLeader
		}

		if err == nil {
			resultCh, errCh := kv.waitOn(index, term)
			kv.mu.Unlock()

			select {
			case <-resultCh:
				reply.Err = OK
				return

			case err = <-errCh:
				kv.mu.Lock()
			}
		}
	}

	kv.mu.Unlock()

	if err != nil {
		switch err {
		case ErrNotLeader:
			reply.WrongLeader = true

		case ErrWrongServer:
			reply.Err = ErrWrongGroup

		case ErrLegacyConf:
			reply.Err = ErrWrongGroup

		case ErrDupCall:
			reply.Err = OK

		default:
			panic("unknown error: " + err.Error())
		}
	}

}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.stopped, 1)

	kv.rf.Kill()
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
	gob.Register(Op{})

	gob.Register(shardmaster.Config{})
	gob.Register(ShardMeta{})
	gob.Register(ShardStore{})

	gob.Register(ConfChangeCommand{})
	gob.Register(SendDoneCommand{})
	gob.Register(RecvShardCommand{})
	gob.Register(GetCommand{})
	gob.Register(PutCommand{})
	gob.Register(AppendCommand{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	kv.confChanged = sync.NewCond(&kv.mu)
	kv.confChangeDone = sync.NewCond(&kv.mu)
	kv.syncRequired = sync.NewCond(&kv.mu)
	kv.shardReceived = sync.NewCond(&kv.mu)

	kv.pendingCalls = make(map[int]*PendingCall)

	for k := range kv.shards {
		kv.shards[k].Kv = make(map[string]string)
		kv.shards[k].Dups = make(map[int64]int)
	}

	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
    kv.rf.EnableCB()

	index, term := kv.loadSnapshot(persister.ReadSnapshot())
	kv.rf.OnSnapshot(index, term)

	go kv.syncBG()
	go kv.pollBG()
	go kv.applyBG()
	go kv.snapshotBG(persister, maxraftstate)

	return kv
}
