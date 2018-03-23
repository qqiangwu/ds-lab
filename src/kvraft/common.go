package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
    ErrWrongLeader = "ErrWrongLeader"
    ErrDup   = "ErrDup"
)

type Err string

// change server's state, need de-duplicate
// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string
    Client int64
    Seq    int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type KV struct {
    Key string
    Value string
}

type MultiPutArgs struct {
    Batch    []KV
    Client   int64
    Seq      int
}

type MultiPutReply struct {
    WrongLeader bool
    Err         Err
}

// won't change server's state, it's safe to execute multi-times.
type GetArgs struct {
	Key string
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
