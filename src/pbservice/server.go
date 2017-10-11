package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

const (
    IDLE = 0
    UNINITIALIZED_BACKUP = 1
    BACKUP = 2
    PRIMARY = 3
)

var repr = []string { "IDLE", "UNINITIALIZED_BACKUP", "BACKUP", "PRIMARY" }

type PBServer struct {
    mu         sync.Mutex
    l          net.Listener
    dead       int32 // for testing
    unreliable int32 // for testing
    me         string
    vs         *viewservice.Clerk

    state      int
    view       viewservice.View
    store      map[string]string
    filter     RPCFilter
}

func (pb *PBServer) getState() string {
    return repr[pb.state]
}

func (pb *PBServer) checkAccess(fromClient bool, viewNum uint) Err {
    if fromClient && pb.state != PRIMARY {
        return ErrWrongServer
    }

    switch pb.state {
    case IDLE:
        if viewNum > pb.view.Viewnum {
            return OK
        } else {
            return ErrWrongServer
        }

    case UNINITIALIZED_BACKUP:
        if viewNum >= pb.view.Viewnum {
            return ErrNotSync
        } else {
            return ErrLegacyView
        }

    case BACKUP:
        return OK

    case PRIMARY:
        if fromClient {
            return OK
        } else if viewNum > pb.view.Viewnum {
            pb.state = BACKUP
            return OK
        } else {
            return ErrLegacyView
        }

    default:
        log.Panicf("BadState(me: %s)", pb.me)
        return ErrWrongServer
    }
}

func (pb *PBServer) serveGet(key string, reply *GetReply) {
    if val, exists := pb.store[key]; !exists {
        reply.Err = ErrNoKey
    } else {
        reply.Value = val
        reply.Err = OK
    }
}

func (pb *PBServer) replicatedGet(args *GetArgs, reply *GetReply) {
    val, exist := pb.store[args.Key]
    req := GetArgs{args.Key, false, pb.view.Viewnum }
    resp := GetReply{}

    if call(pb.view.Backup, "PBServer.Get", &req, &resp) {
        switch resp.Err {
        case OK:
            if !exist || resp.Value != val {
                log.Panicf("Inconsistency(view: %d, primary: %s, backup: %s, key: %s)",
                    pb.view.Viewnum, pb.view.Primary, pb.view.Backup, args.Key)
                reply.Err = OK
            } else {
                reply.Value = val
                reply.Err = OK
            }

        case ErrNoKey:
            if exist {
                log.Panicf("Inconsistency(view: %d, primary: %s, backup: %s, key: %s",
                    pb.view.Viewnum, pb.view.Primary, pb.view.Backup, args.Key)
            } else {
                reply.Err = ErrNoKey
            }

        case ErrNotSync:
            // should sync after primary observe view change
            log.Panicf("NotSync(view: %d, primary: %s, backup: %s, key: %s",
                pb.view.Viewnum, pb.view.Primary, pb.view.Backup, args.Key)
            reply.Err = ErrNotSync

        case ErrWrongServer:
            log.Panicf("ErrWrongServer in replicatedGet")
            reply.Err = ErrWrongServer

        case ErrLegacyView:
            reply.Err = ErrWrongServer
        }
    } else {
        // view invalid: backup failed, we need a view change
        // FIXME: what if the network partitioned
        reply.Err = ErrLegacyView
    }
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
    pb.mu.Lock()
    defer pb.mu.Unlock()

    if err := pb.checkAccess(args.FromClient, args.ViewNum); err != OK {
        log.Printf("Get(result: %s, key: %s, view: %d, me: %s, client: %t)",
            err, args.Key, args.ViewNum, pb.me, args.FromClient)
        reply.Err = err
        return nil
    }

    log.Printf("Get(key: %s, view: %d, me: %s, client: %t)", args.Key, pb.view.Viewnum, pb.me, args.FromClient)
    switch pb.state {
    case BACKUP:
        pb.serveGet(args.Key, reply)

    case PRIMARY:
        if pb.view.Backup != "" {
            pb.replicatedGet(args, reply)
        } else {
            pb.serveGet(args.Key, reply)
        }

    default:
        log.Panicf("BadState(me: %s)", pb.me)
    }

    return nil
}

func (pb *PBServer) servePut(key string, value string) {
    pb.store[key] = value
}

func (pb *PBServer) replicatedPut(args *PutAppendArgs, reply *PutAppendReply) {
    pb.servePut(args.Key, args.Value)

    req := PutAppendArgs{args.Key, args.Value, "Put", pb.view.Viewnum, false, args.Source, args.Id }
    resp := PutAppendReply{}

    if call(pb.view.Backup, "PBServer.PutAppend", &req, &resp) {
        switch resp.Err {
        case OK:
            reply.Err = OK

        case ErrNotSync:
            // should sync after primary observe view change
            log.Panicf("NotSync(view: %d, primary: %s, backup: %s, key: %s",
                pb.view.Viewnum, pb.view.Primary, pb.view.Backup, args.Key)
            reply.Err = ErrNotSync

        case ErrWrongServer:
            log.Panicf("ErrWrongServer in replicatedPut")

        case ErrLegacyView:
            reply.Err = ErrWrongServer

        default:
            log.Panicf("ReplicatedPut(unexpected: %s, me: %s, state: %s, view: %d)",
                resp.Err, pb.me, pb.getState(), pb.view.Viewnum)
            reply.Err = ErrWrongServer
        }
    } else {
        // view invalid: backup failed, we need a view change
        // FIXME: what if the network partitioned
        reply.Err = ErrLegacyView
    }
}

func (pb *PBServer) put(args *PutAppendArgs, reply *PutAppendReply) {
    if err := pb.checkAccess(args.FromClient, args.ViewNum); err != OK {
        log.Printf("Put(result: %s, key: %s, view: %d, me: %s, client: %t)",
            err, args.Key, args.ViewNum, pb.me, args.FromClient)
        reply.Err = err
        return
    }

    log.Printf("Put(key: %s, value: %s, me: %s, state: %s, view: %d, client: %t)",
        args.Key, args.Value, pb.me, pb.getState(), pb.view.Viewnum, args.FromClient)

    switch pb.state {
    case PRIMARY:
        if pb.view.Backup != "" {
            pb.replicatedPut(args, reply)
        } else {
            pb.servePut(args.Key, args.Value)
            reply.Err = OK
        }

    case BACKUP:
        pb.servePut(args.Key, args.Value)
        reply.Err = OK

    default:
        log.Panicf("Put(result: panic, me: %s, state: %s, view: %d, argview: %d",
            pb.me, pb.getState(), pb.view.Viewnum, args.ViewNum)
    }
}

// idempotent
func (pb *PBServer) serveAppend(args *PutAppendArgs) {
    if !pb.filter.Start(args.Source, args.Id) {
        log.Printf("ServerAppend(result: duplicated, source: %s, id: %d, value: %s)",
            args.Source, args.Id, args.Value)
        return
    }

    val, exist := pb.store[args.Key]

    if !exist {
        val = args.Value
    } else {
        val += args.Value
    }

    pb.store[args.Key] = val

    pb.filter.Commit(args.Source, args.Id)
}

func (pb *PBServer) replicatedAppend(args *PutAppendArgs, reply *PutAppendReply) {
    pb.serveAppend(args)

    req := PutAppendArgs{
        args.Key,
        args.Value,
        "Append",
        pb.view.Viewnum,
        false,
        args.Source,
        args.Id }
    resp := PutAppendReply{}

    if call(pb.view.Backup, "PBServer.PutAppend", &req, &resp) {
        switch resp.Err {
        case OK:
            reply.Err = OK

        case ErrNotSync:
            // should sync after primary observe view change
            log.Panicf("NotSync(view: %d, primary: %s, backup: %s, key: %s",
                pb.view.Viewnum, pb.view.Primary, pb.view.Backup, args.Key)
            reply.Err = ErrNotSync

        case ErrWrongServer:
            log.Panicf("ErrWrongServer in replicatedAppend")

        case ErrLegacyView:
            log.Printf("ReplicatedAppend(result: ErrLegacyView, me: %s, view: %d)",
                pb.me, pb.view.Viewnum)
            reply.Err = ErrWrongServer

        default:
            log.Panicf("ReplicatedAppend(unexpected: %s, me: %s, state: %s, view: %d)",
                resp.Err, pb.me, pb.getState(), pb.view.Viewnum)
            reply.Err = ErrWrongServer
        }
    } else {
        // view invalid: backup failed, we need a view change
        // FIXME: what if the network partitioned
        reply.Err = ErrLegacyView
    }
}

func (pb *PBServer) append(args *PutAppendArgs, reply *PutAppendReply) {
    if err := pb.checkAccess(args.FromClient, args.ViewNum); err != OK {
        log.Printf("Append(result: %s, key: %s, view: %d, me: %s, client: %t)",
            err, args.Key, args.ViewNum, pb.me, args.FromClient)
        reply.Err = err
        return
    }

    log.Printf("Append(key: %s, value: %s, me: %s, state: %s, view: %d, client: %t)",
        args.Key, args.Value, pb.me, pb.getState(), pb.view.Viewnum, args.FromClient)

    switch pb.state {
    case PRIMARY:
        if pb.view.Backup != "" {
            pb.replicatedAppend(args, reply)
        } else {
            pb.serveAppend(args)
            reply.Err = OK
        }

    case BACKUP:
        pb.serveAppend(args)
        reply.Err = OK

    default:
        log.Panicf("Append(result: panic, me: %s, state: %s, view: %d, argview: %d",
            pb.me, pb.getState(), pb.view.Viewnum, args.ViewNum)
    }
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
    pb.mu.Lock()
    defer pb.mu.Unlock()

    switch args.Op {
    case OP_PUT: pb.put(args, reply)
    case OP_APPEND: pb.append(args, reply)
    default:
        log.Panicf("BadOp(op: %s)", args.Op)
    }

    return nil
}

func (pb *PBServer) Sync(args *SyncArgs, reply *SyncReply) error {
    pb.mu.Lock()
    defer pb.mu.Unlock()

    log.Printf("Sync(backup: %s, view: %d, state: %s)", pb.me, pb.view.Viewnum, pb.getState())
    if err := pb.checkAccess(false, args.ViewNum); err != ErrNotSync && err != OK {
        log.Printf("Sync(result: unexpected, backup: %s, view: %d, newview: %d",
            pb.me, pb.view.Viewnum, args.ViewNum)
        return nil
    }

    pb.store = args.Store
    pb.state = BACKUP
    pb.view = args.View
    pb.filter.Load(args.Filter)
    reply.Err = OK

    log.Printf("Sync(result: done, view: %d, me: %s, state: %s, meView: %d)",
        args.ViewNum, pb.me, pb.getState(), pb.view.Viewnum)

    return nil
}

func (pb *PBServer) syncToBackup() {
    if pb.view.Backup == "" {
        return
    }

    log.Printf("syncToBackup(view: %d, primary: %s, backup: %s)",
        pb.view.Viewnum, pb.view.Primary, pb.view.Backup)

    req := SyncArgs{pb.view.Viewnum, pb.view, pb.store, pb.filter.Dump() }
    resp := SyncReply{}
    if call(pb.view.Backup, "PBServer.Sync", &req, &resp) {
        switch resp.Err {
        case ErrLegacyView:
            log.Printf("Sync(result: legacyView, view: %d, prmiary: %s, backup: %s)",
                pb.view.Viewnum, pb.view.Primary, pb.view.Backup)

        case ErrWrongServer:
            log.Printf("Sync(result: wrongServer, view: %d, primary: %s, backup: %s)",
                pb.view.Viewnum, pb.view.Primary, pb.view.Backup)

        case OK:
            log.Printf("Sync(result: done, view: %d, prmiary: %s, backup: %s)",
                pb.view.Viewnum, pb.view.Primary, pb.view.Backup)

        default:
            log.Panicf("Sync(result: %s, view: %d, prmiary: %s, backup: %s)",
                resp.Err, pb.view.Viewnum, pb.view.Primary, pb.view.Backup)
        }
    } else {
        log.Printf("Sync(result: backupDied, view: %d, primary: %s, backup: %s)",
            pb.view.Viewnum, pb.view.Primary, pb.view.Backup)
    }
}

func (pb *PBServer) onNewView(view *viewservice.View) {
    log.Printf("> OnNewView(me: %s, state: %s, old: %d, new: %d)", pb.me, pb.getState(), pb.view.Viewnum, view.Viewnum)

    switch pb.state {
    case IDLE:
        switch pb.me {
        case view.Primary:
            log.Printf("> OnNewView(result: idleToPrimary, me: %s, view: %d)", pb.me, view.Viewnum)
            pb.view = *view
            pb.store = make(map[string]string)
            pb.syncToBackup()
            pb.state = PRIMARY
        case view.Backup:
            log.Printf("> OnNewView(result: idleToBackup, me: %s, view: %d)", pb.me, view.Viewnum)
            pb.view = *view
            pb.state = UNINITIALIZED_BACKUP
        default:
            log.Printf("> OnNewView(result: keepIdle, me: %s)", pb.me)
            pb.view = *view
        }

    case UNINITIALIZED_BACKUP:
        log.Panicf("View changed when not synced")

    case BACKUP:
        switch pb.me {
        case view.Primary:
            log.Printf("> OnNewView(result: backupToPrimary, me: %s, view: %d)", pb.me, view.Viewnum)
            pb.view = *view
            pb.syncToBackup()
            pb.state = PRIMARY

        case view.Backup:
            log.Printf("> OnNewView(result: backupToBackup, me: %s, view: %d)", pb.me, view.Viewnum)
            pb.view = *view
            pb.state = UNINITIALIZED_BACKUP

        default:
            pb.view = *view
            pb.state = IDLE
        }

    case PRIMARY:
        switch pb.me {
        case view.Primary:
            log.Printf("> OnNewView(result: primaryToPrimary, me: %s, view: %d)", pb.me, view.Viewnum)
            pb.view = *view
            pb.syncToBackup()

        case view.Backup:
            log.Printf("> OnNewView(result: primaryToBackup, me: %s, view: %d)", pb.me, view.Viewnum)
            pb.view = *view
            pb.state = UNINITIALIZED_BACKUP

        default:
            log.Printf("> OnNewView(result: primaryToIdle, me: %s, view: %d)", pb.me, view.Viewnum)
            pb.view = *view
            pb.state = IDLE
        }
    }

    log.Printf("< OnNewView(me: %s, state: %s, view: %d)", pb.me, pb.getState(), pb.view.Viewnum)
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
    pb.mu.Lock()
    defer pb.mu.Unlock()

    // fetch new view if any
    view, ok := pb.vs.Ping(pb.view.Viewnum)
    if ok != nil {
        log.Printf("Ping(result: failed, me: %s)", pb.me)
        return
    }

    if view.Viewnum > pb.view.Viewnum {
        pb.onNewView(&view)
    }
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
    atomic.StoreInt32(&pb.dead, 1)
    pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
    return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
    if what {
        atomic.StoreInt32(&pb.unreliable, 1)
    } else {
        atomic.StoreInt32(&pb.unreliable, 0)
    }
}

func (pb *PBServer) isunreliable() bool {
    return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
    pb := new(PBServer)
    pb.me = me
    pb.vs = viewservice.MakeClerk(me, vshost)
    pb.store = make(map[string]string)
    pb.filter.Init()

    rpcs := rpc.NewServer()
    rpcs.Register(pb)

    os.Remove(pb.me)
    l, e := net.Listen("unix", pb.me)
    if e != nil {
        log.Fatal("listen error: ", e)
    }
    pb.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    go func() {
        for pb.isdead() == false {
            conn, err := pb.l.Accept()
            if err == nil && pb.isdead() == false {
                if pb.isunreliable() && (rand.Int63()%1000) < 100 {
                    // discard the request.
                    conn.Close()
                } else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
                    // process the request but force discard of reply.
                    c1 := conn.(*net.UnixConn)
                    f, _ := c1.File()
                    err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
                    if err != nil {
                        fmt.Printf("shutdown: %v\n", err)
                    }
                    go rpcs.ServeConn(conn)
                } else {
                    go rpcs.ServeConn(conn)
                }
            } else if err == nil {
                conn.Close()
            }
            if err != nil && pb.isdead() == false {
                fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
                pb.kill()
            }
        }
    }()

    go func() {
        for pb.isdead() == false {
            pb.tick()
            time.Sleep(viewservice.PingInterval)
        }

        log.Printf("PBServer(died: %s)", pb.me)
    }()

    return pb
}
