package shardmaster

import "raft"
import "kvraft"
import "labrpc"
import "sync"
import "encoding/gob"
import "encoding/base64"
import "strconv"
import "bytes"

type ShardMaster struct {
	mu sync.Mutex
	me int
	kv *raftkv.RaftKV
	rf *raft.Raft

	configs []Config // indexed by config num
}

// @pre sm.mu.Locked
func (sm *ShardMaster) isLeader() bool {
	_, isLeader := sm.rf.GetState()
	return isLeader
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.isLeader() {
		reply.WrongLeader = true
		return
	}

	config, expired := sm.getCurrentConfig()
	if expired {
		reply.WrongLeader = true
		return
	}

	config.join(args)
	expired = sm.updateOnce(args.Client, args.Seq, config)
	if expired {
		reply.WrongLeader = true
	} else {
		reply.Err = OK
		DPrintf("Join(me: %v, config: %v, args: %v)", sm.me, config.Num, args)
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.isLeader() {
		reply.WrongLeader = true
		return
	}

	config, expired := sm.getCurrentConfig()
	if expired {
		reply.WrongLeader = true
		return
	}

	config.leave(args)
	expired = sm.updateOnce(args.Client, args.Seq, config)
	if expired {
		reply.WrongLeader = true
	} else {
		reply.Err = OK
		DPrintf("Leave(me: %v, config: %v, args: %v)", sm.me, config.Num, args)
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.isLeader() {
		reply.WrongLeader = true
		return
	}

	config, expired := sm.getCurrentConfig()
	if expired {
		reply.WrongLeader = true
		return
	}

	config.move(args)
	expired = sm.updateOnce(args.Client, args.Seq, config)
	if expired {
		reply.WrongLeader = true
	} else {
		reply.Err = OK
		DPrintf("Move(me: %v, config: %v, args: %v)", sm.me, config.Num, args)
	}
}

// need lock to prevent any pending configuration modifications
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var config Config
	var wrongLeader = !sm.isLeader()

	if !wrongLeader {
		config, wrongLeader = sm.getCurrentConfig()
	}

	if !wrongLeader {
		if args.Num >= 0 && args.Num < config.Num {
			config, wrongLeader = sm.getConfig(args.Num)
		}
	}

	if wrongLeader {
		reply.WrongLeader = true
	} else {
		reply.Err = OK
		reply.Config = config
		DPrintf("Query(me: %v, config: %v)", sm.me, config)
	}
}

const CURRENT_CONFIG = "CONFIG"

// @pre sm.mu.Locked
func (sm *ShardMaster) getCurrentConfig() (config Config, expired bool) {
	args := raftkv.GetArgs{CURRENT_CONFIG}
	reply := raftkv.GetReply{}

	sm.kv.Get(&args, &reply)

	return parseConfig(reply), reply.WrongLeader
}

// @pre sm.mu.Locked
func (sm *ShardMaster) getConfig(num int) (config Config, expired bool) {
	args := raftkv.GetArgs{strconv.Itoa(num)}
	reply := raftkv.GetReply{}

	sm.kv.Get(&args, &reply)

	return parseConfig(reply), reply.WrongLeader
}

// @pre sm.mu.Locked
func (sm *ShardMaster) updateOnce(client int64, seq int, config Config) (expired bool) {
	repr := toString(config)
	batch := []raftkv.KV{
		{CURRENT_CONFIG, repr},
		{strconv.Itoa(config.Num), repr}}

	args := raftkv.MultiPutArgs{batch, client, seq}
	reply := raftkv.MultiPutReply{}

	sm.kv.MultiPut(&args, &reply)

	return reply.WrongLeader
}

func toString(config Config) string {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(config)
	if err != nil {
		panic("encode config failed")
	}
	return base64.StdEncoding.EncodeToString(b.Bytes())
}

func parseConfig(reply raftkv.GetReply) (config Config) {
	if !reply.WrongLeader && reply.Err == OK {
		repr, err := base64.StdEncoding.DecodeString(reply.Value)
		if err != nil {
			panic("decode repr failed")
		}

		buffer := bytes.Buffer{}
		buffer.Write(repr)

		decoder := gob.NewDecoder(&buffer)
		err = decoder.Decode(&config)
		if err != nil {
			panic("decode to config failed")
		}
	}
	if reply.Err == raftkv.ErrNoKey {
		config.Groups = make(map[int][]string)
	}

	return config
}

func init() {
	gob.Register(Config{})
}

// put configuration mutation operations in Config.
// actually we should make it a strategy object to enable
// customization.
func (cfg *Config) join(args *JoinArgs) {
	cfg.Num++

	for gid, group := range args.Servers {
		cfg.Groups[gid] = group
	}

	cfg.rebalance()
}

func (cfg *Config) leave(args *LeaveArgs) {
	cfg.Num++

	removed := make(map[int]bool)
	for _, gid := range args.GIDs {
		delete(cfg.Groups, gid)
		removed[gid] = true
	}

	gk := 0
	for shard, gid := range cfg.Shards {
		_, exist := removed[gid]
		if exist {
			cfg.Shards[shard] = 0
			gk++
		}
	}

	cfg.rebalance()
}

func (cfg *Config) move(args *MoveArgs) {
	cfg.Num++

	src := cfg.Shards[args.Shard]
	if src != args.GID {
		cfg.Shards[args.Shard] = args.GID
	}
}

func (cfg *Config) rebalance() {
	if len(cfg.Groups) == 0 {
		for i := range cfg.Shards {
			cfg.Shards[i] = 0
		}

		return
	}
	for shard, gid := range cfg.Shards {
		if gid == 0 {
			for gid2, _ := range cfg.Groups {
				cfg.Shards[shard] = gid2
				break
			}
		}
	}

	avgLoad := NShards / len(cfg.Groups)
	loads := make(map[int]int)
	for _, gid := range cfg.Shards {
		loads[gid]++
	}

	for i := 0; i < len(cfg.Shards); i++ {
		moved := false

		for gid, load1 := range loads {
			if load1 > avgLoad {
				for gid2 := range cfg.Groups {
					if gid2 == gid {
						continue
					}

					load2 := loads[gid2]
					if load2 >= avgLoad {
						continue
					}

					loads[gid]--
					loads[gid2]++
					for shard, srcGid := range cfg.Shards {
						if srcGid == gid {
							cfg.Shards[shard] = gid2
							moved = true
							break
						}
					}
					break
				}
			}
		}

		if !moved {
			break
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.kv.Kill()
}

// needed by shardkv tester
// no need to lock
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.kv.Raft()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me
	sm.kv = raftkv.StartKVServer(servers, me, persister, -1)
	sm.rf = sm.kv.Raft()

	return sm
}
