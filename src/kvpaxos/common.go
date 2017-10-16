package kvpaxos

const (
    OK       = "OK"
    ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
    Key   string
    Value string
    Op    string // "Put" or "Append"
    Clerk int64
    Id    int64
}

type PutAppendReply struct {
    Err Err
}

type GetArgs struct {
    Key string
}

type GetReply struct {
    Err   Err
    Value string
}
