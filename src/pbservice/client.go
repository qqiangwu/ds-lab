package pbservice

import "viewservice"
import "net/rpc"
import "fmt"
import "strconv"

import "crypto/rand"
import (
    "math/big"
    "time"
    "log"
)

type Clerk struct {
    vs *viewservice.Clerk
    me string
    primary string
}

// this may come in handy.
func nrand() int64 {
    max := big.NewInt(int64(1) << 62)
    bigx, _ := rand.Int(rand.Reader, max)
    x := bigx.Int64()
    return x
}

func MakeClerk(vshost string, me string) *Clerk {
    ck := new(Clerk)
    ck.vs = viewservice.MakeClerk(me, vshost)
    ck.me = strconv.Itoa(int(nrand()))
    ck.primary = ck.vs.Primary()

    return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
    args interface{}, reply interface{}) bool {
    c, errx := rpc.Dial("unix", srv)
    if errx != nil {
        return false
    }
    defer c.Close()

    err := c.Call(rpcname, args, reply)
    if err == nil {
        return true
    }

    fmt.Println(err)
    return false
}

func (ck *Clerk) refreshView() {
    for {
        ck.primary = ck.vs.Primary()
        time.Sleep(viewservice.PingInterval)

        if ck.primary != "" {
            break
        }
    }
}

func (ck *Clerk) refreshViewIfNecessary() {
    for ck.primary == "" {
        ck.refreshView()
    }
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
    ck.refreshViewIfNecessary()

    args := GetArgs{key, true, 0 }
    reply := GetReply{}

    for {
        if call(ck.primary, "PBServer.Get", &args, &reply) {
            switch reply.Err {
            case OK: return reply.Value

            case ErrNoKey: return ""

            case ErrNotSync:
                log.Printf("ClerkGet(result: notSync, me: %s, primary: %s)", ck.me, ck.primary)
                time.Sleep(viewservice.PingInterval)

            case ErrWrongServer:
                log.Printf("ClerkGet(result: wrongServer, me: %s, primary: %s)", ck.me, ck.primary)
                ck.refreshView()

            case ErrLegacyView:
                log.Printf("ClerkGet(result: legacyView, me: %s, primary: %s)", ck.me, ck.primary)
                time.Sleep(viewservice.PingInterval)

            default:
                log.Panicf("Unexpected(err: %s, key: %s)", reply.Err, key)
            }
        } else {
            log.Printf("Get(result: serverDied, clerk: %s)", ck.me)
            ck.refreshView()
        }
    }
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
    ck.refreshViewIfNecessary()

    args := PutAppendArgs{key, value, op, 0, true, ck.me, nrand() }
    reply := PutAppendReply{}

    for {
        if call(ck.primary, "PBServer.PutAppend", &args, &reply) {
            switch reply.Err {
            case OK: fallthrough
            case ErrNoKey: return

            case ErrNotSync:
                time.Sleep(viewservice.PingInterval)

            case ErrWrongServer:
                log.Printf("ClertPutAppend(result: wrongServer, me: %s, primary: %s)", ck.me, ck.primary)
                ck.refreshView()

            case ErrLegacyView:
                log.Printf("ClertPutAppend(result: legacyView, me: %s, primary: %s)", ck.me, ck.primary)
                time.Sleep(viewservice.PingInterval)

            default:
                log.Panicf("Unexpected(err: %s, key: %s, value: %s, op: %s)",
                    reply.Err, key, value, op)
            }
        } else {
            log.Printf("PutAppend(result: serverDied, clerk: %s)", ck.me)
            ck.refreshView()
        }
    }
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
    ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
    ck.PutAppend(key, value, "Append")
}
