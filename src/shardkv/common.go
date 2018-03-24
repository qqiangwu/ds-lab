package shardkv

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ConfNum int
	Key     string
	Value   string
	Op      string // "Put" or "Append"
	Client  int64
	Seq     int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	ConfNum int
	Key     string
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type SendArgs struct {
	ConfNum int
	ShardId int
	Store   ShardStore
}

type SendReply struct {
	WrongLeader bool
	Err         Err
}
