package shardkv

import "fmt"
import "shardmaster"

type Command interface {
	Apply(kv *ShardKV) (interface{}, error)
	Name() string
}

type SendDoneCommand struct {
	ConfNum int
	ShardId int
}

func (cmd SendDoneCommand) Name() string {
	return "SendDoneCmd"
}

// @pre kv.mu.Locked
// @pre RPC层保证，cmd.ConfNum一定比kv小 <=
func (cmd SendDoneCommand) Apply(kv *ShardKV) (interface{}, error) {
	assert(cmd.ConfNum <= kv.confNum, "cmd conf is too large")

	if cmd.ConfNum < kv.confNum {
		// 延迟的响应
		DPrintf("SendDoneCmd(me: %v, cmdConf: %v, kvConf: %v): legacy cmd", kv.gid, cmd.ConfNum, kv.confNum)
	} else {
		shardId := cmd.ShardId

		DPrintf("SendDoneCmd(me: %v, conf: %v, shard: %v): done", kv.gid, cmd.ConfNum, cmd.ShardId)

		if kv.meta[shardId].State == SEND {
			kv.meta[shardId].State = NOT_OWNED
			kv.shards[shardId].clear()
			kv.checkConfChangeFinished()
		}
	}

	return nil, nil
}

type RecvShardCommand struct {
	ConfNum int
	ShardId int
	Store   ShardStore
}

func (cmd RecvShardCommand) Name() string {
	return "RecvShardCmd"
}

// @pre kv.mu.Locked
// @pre RPC层保证，cmd.ConfNum一定比kv小 <=
func (cmd RecvShardCommand) Apply(kv *ShardKV) (interface{}, error) {
	assert(cmd.ConfNum <= kv.confNum, "cmd conf is too large")

	if cmd.ConfNum < kv.confNum {
		DPrintf("RecvShardCommand(me: %v, kv: %v, cmd: %v): legacy recv", kv.me, kv.confNum, cmd.ConfNum)
		return nil, nil
	} else {
		meta := kv.meta[cmd.ShardId]

		if meta.State == RECV {
			DPrintf("RecvShardCmd(me: %v, conf: %v, shard: %v)", kv.gid, cmd.ConfNum, cmd.ShardId)

			kv.meta[cmd.ShardId].State = OWNED
			kv.shards[cmd.ShardId] = cmd.Store
			kv.shardReceived.Broadcast()

			kv.checkConfChangeFinished()

			return nil, nil
		} else {
			return nil, ErrDupCall
		}
	}
}

type ConfChangeCommand struct {
	NewConf shardmaster.Config
}

func (cmd ConfChangeCommand) Name() string {
	return "ConfChangeCmd"
}

// @pre kv.mu.Locked
func (cmd ConfChangeCommand) Apply(kv *ShardKV) (interface{}, error) {
	DPrintf("ConfChangeCmd(me: %v-%v, conf: %v, newConf: %v)", kv.gid, kv.me, kv.confNum, cmd.NewConf.Num)

	if cmd.NewConf.Num <= kv.confNum {
		return nil, ErrLegacyConf
	} else if cmd.NewConf.Num == kv.confNum+1 {
		for _, meta := range kv.meta {
			if meta.State == RECV || meta.State == SEND {
				panic("Change conf concurrently")
			}
		}

		kv.confNum = cmd.NewConf.Num
		kv.modifyOwnership(cmd.NewConf)
		kv.conf = cmd.NewConf

		kv.confChanged.Broadcast()
		kv.checkConfChangeFinished()

		return nil, nil
	} else {
		panic("Bad conf change")
	}
}

type PutCommand struct {
	ConfNum int
	Key     string
	Value   string
	Client  int64
	Seq     int
}

func (cmd PutCommand) Name() string {
	return "PutCmd"
}

// @pre kv.mu.Locked
// @pre cmd.ConfNum <= kv.confNum
func (cmd PutCommand) Apply(kv *ShardKV) (interface{}, error) {
	assert(cmd.ConfNum <= kv.confNum, "cmd conf is too large")

	if cmd.ConfNum < kv.confNum {
		return nil, ErrLegacyConf
	} else {
		requestShard := key2shard(cmd.Key)

		meta := kv.meta[requestShard]
		assert(meta.State == OWNED, "shard not owned")

		store := &kv.shards[requestShard]
		if store.Dups[cmd.Client] >= cmd.Seq {
			return nil, ErrDupCall
		} else {
			DPrintf("Put(me: %v-%v, key: %v, value: %v)", kv.gid, kv.me, cmd.Key, cmd.Value)

			store.Dups[cmd.Client] = cmd.Seq
			store.Kv[cmd.Key] = cmd.Value

			return nil, nil
		}
	}
}

type AppendCommand struct {
	ConfNum int
	Key     string
	Value   string
	Client  int64
	Seq     int
}

func (cmd AppendCommand) Name() string {
	return "AppendCmd"
}

// @pre kv.mu.Locked
// @pre cmd.ConfNum <= kv.confNum
func (cmd AppendCommand) Apply(kv *ShardKV) (interface{}, error) {
	assert(cmd.ConfNum <= kv.confNum, "cmd conf is too large")

	if cmd.ConfNum < kv.confNum {
		return nil, ErrLegacyConf
	} else {
		requestShard := key2shard(cmd.Key)

		meta := kv.meta[requestShard]
		assert(meta.State == OWNED, "shard not owned")

		store := &kv.shards[requestShard]
		if store.Dups[cmd.Client] >= cmd.Seq {
			return nil, ErrDupCall
		} else {
			store.Dups[cmd.Client] = cmd.Seq
			store.Kv[cmd.Key] = store.Kv[cmd.Key] + cmd.Value

			return nil, nil
		}
	}
}

type GetCommand struct {
	ConfNum int
	Key     string
}

func (cmd GetCommand) Name() string {
	return "GetCmd"
}

// @pre kv.mu.Locked
// @pre cmd.ConfNum <= kv.confNum
func (cmd GetCommand) Apply(kv *ShardKV) (interface{}, error) {
	assert(cmd.ConfNum <= kv.confNum, "cmd conf is too large")

	if cmd.ConfNum < kv.confNum {
		return nil, ErrLegacyConf
	} else {
		requestShard := key2shard(cmd.Key)

		assert(kv.meta[requestShard].State == OWNED, fmt.Sprintf("key shard not owned by this group(conf: %v, shard: %v, state: %v)", cmd.ConfNum, requestShard, kv.meta[requestShard].State))

		value, ok := kv.shards[requestShard].Kv[cmd.Key]
		if !ok {
			return nil, ErrNotFound
		} else {
			return value, nil
		}
	}
}
