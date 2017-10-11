package pbservice

import "viewservice"

const (
    OK             = "OK"
    ErrNoKey       = "ErrNoKey"
    ErrWrongServer = "ErrWrongServer"
    ErrNotSync     = "ErrNotSync"
    ErrLegacyView  = "ErrLegacyView"
)

const (
    OP_PUT         = "Put"
    OP_APPEND      = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
    Key   string
    Value string
    Op    string
    ViewNum uint
    FromClient bool
    Source string
    Id     int64
}

type PutAppendReply struct {
    Err Err
}

type GetArgs struct {
    Key string
    FromClient bool
    ViewNum uint
}

type GetReply struct {
    Err   Err
    Value string
}

type SyncArgs struct {
    ViewNum uint
    View    viewservice.View
    Store   map[string]string
    Filter  map[string]int64
}

type SyncReply struct {
    Err Err
}
