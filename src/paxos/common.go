package paxos

import (
    "net/rpc"
    "net"
    "syscall"
    "fmt"
)

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
    c, err := rpc.Dial("unix", srv)
    if err != nil {
        err1 := err.(*net.OpError)
        if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
            fmt.Printf("paxos Dial() failed: %v\n", err1)
        }
        return false
    }
    defer c.Close()

    err = c.Call(name, args, reply)
    if err == nil {
        return true
    }

    fmt.Println(err)
    return false
}

func minElement(values []int32) int32 {
    if len(values) == 0 {
        panic("minElement of empty array")
    }

    minV := values[0]
    for _, v := range values {
        if v < minV {
            minV = v
        }
    }

    return minV
}

func maxOf(a int, b int) int {
    if a < b {
        return b
    } else {
        return a
    }
}

func maxOfInt32(a int32, b int32) int32 {
    if a < b {
        return b
    } else {
        return a
    }
}

type MessageType int

const (
    PREPARE MessageType = iota
    PREPARE_OK
    PREPARE_REJECT
    ACCEPT
    ACCEPT_OK
    ACCEPT_REJECT
    DECIDE
)

var types = []string { "PREPARE", "PREPARE_OK", "PREPARE_REJECT", "ACCEPT", "ACCEPT_OK", "ACCEPT_REJECT", "DECIDE" }

type PaxosMessage struct {
    Type        MessageType
    Proposal    int
    MaxProposal int
    MaxValue    interface{}
    Seq         int
    Sender      int
    SenderMin   int32
    MinValues   []int32
}

func (p *PaxosMessage) typeStr() string {
    return types[p.Type]
}
