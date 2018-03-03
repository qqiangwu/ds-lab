package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "time"

type Clerk struct {
	servers []*labrpc.ClientEnd

    id      int64
    seq     int
    leader  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
    ck.id = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
    nservers := len(ck.servers)

    args := &GetArgs{}
    args.Key = key

    for {
        // either block or retry
        for i := 0; i < nservers; i++ {
            server := (ck.leader + i) % nservers
            reply := &GetReply{}

            ok := ck.servers[server].Call("RaftKV.Get", args, reply)
            if ok {
                if !reply.WrongLeader {
                    ck.leader = server
                    if reply.Err == OK {
                        return reply.Value
                    } else {
                        return ""
                    }
                }
            }
        }

        // avoid busy loop
        time.Sleep(50 * time.Millisecond)
    }

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
    nservers := len(ck.servers)

    ck.seq++

    args := &PutAppendArgs{}
    args.Key = key
    args.Value = value
    args.Op = op
    args.Client = ck.id
    args.Seq = ck.seq

    for {
        // either block or retry
        for i := 0; i < nservers; i++ {
            server := (ck.leader + i) % nservers
            reply := &PutAppendReply{}

            ok := ck.servers[server].Call("RaftKV.PutAppend", args, reply)
            if ok {
                if !reply.WrongLeader {
                    ck.leader = server
                    return
                }
            }
        }

        // avoid busy loop
        time.Sleep(50 * time.Millisecond)
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
