package lockservice

//
// RPC definitions for a simple lock service.
//
// You will need to modify this file.
//
//
// Lock(lockname) returns OK=true if the lock is not held.
// If it is held, it returns OK=false immediately.
//
type LockArgs struct {
    Lockname string // lock name
    Id       uint64
}

type LockReply struct {
    OK bool
}

type UnlockArgs struct {
    Lockname string
    Id       uint64
}

type UnlockReply struct {
    OK bool
}
