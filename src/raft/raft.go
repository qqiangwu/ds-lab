package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "bytes"
import "encoding/gob"
import "fmt"

type FsmState int
const (
    FOLLOWER FsmState = iota
    CANDIDATE
    LEADER
    KILLED
)

type FsmHandler interface {
    enter(rf *Raft)
    leave()

    getState() FsmState

    handleVote(args *RequestVoteArgs, reply *RequestVoteReply)
    appendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)
    submit(command interface{}) (int, int, bool)

    toDebugString() string
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
    Term        int
    Index       int
    Command     interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int               // this peer's index into peers[]
    applier   chan ApplyMsg

    // guarded by mu
    currentTerm int
    votedFor     int
    log         []LogEntry
    handler     FsmHandler
    commitIndex int
    lastApplied int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    return rf.currentTerm, rf.handler.getState() == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
// @pre rf.mu.Locked
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
// @note we don't validate the persisted state
func (rf *Raft) readPersist(data []byte) {
    if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.log)
	d.Decode(&rf.currentTerm)
    d.Decode(&rf.votedFor)
}

// @pre rf.mu.Locked
func (rf *Raft) applyNew() {
    for rf.commitIndex > rf.lastApplied {
        rf.lastApplied++
        rf.apply(rf.log[rf.lastApplied])
    }
}

// @pre rf.mu.Locked
func (rf *Raft) apply(entry LogEntry) {
    rf.mu.Unlock()
    defer rf.mu.Lock()

    msg := ApplyMsg{}
    msg.Index = entry.Index
    msg.Command = entry.Command

    rf.applier <- msg
}

// @pre rf.mu.Locked
func (rf *Raft) validateTerm(term int) bool {
    if rf.handler.getState() == KILLED {
        return false
    }

    if rf.currentTerm < term {
        rf.currentTerm = term
        rf.becomeFollower()

        // term and votedFor changed
        rf.persist()
    }

    return rf.currentTerm == term
}

// become follower don't change term
// @pre rf.mu.Locked
func (rf *Raft) becomeFollower() {
    if rf.currentTerm != 0 {
        // assert term = 0
        rf.handler.leave()
    }
    rf.handler = &FollowerHandler{}
    rf.handler.enter(rf)

    DPrintf("BecomeFollower(me: %v, term: %v)", rf.me, rf.currentTerm)
}

// @pre rf.mu.Locked
func (rf *Raft) becomeCandidate() {
    rf.currentTerm++
    rf.handler.leave()
    rf.handler = &CandidateHandler{}
    rf.handler.enter(rf)

    // term changed
    rf.persist()

    DPrintf("BecomeCandidate(me: %v, term: %v)", rf.me, rf.currentTerm)
}

// become leader don't change term
// @pre rf.mu.Locked
func (rf *Raft) becomeLeader() {
    rf.handler.leave()
    rf.handler = &LeaderHandler{}
    rf.handler.enter(rf)

    DPrintf("BecomeLeader(me: %v, term: %v)", rf.me, rf.currentTerm)
}

type RequestVoteArgs struct {
    Term                int         // candidate term
    CandidateId         int
    LastLogIndex        int
    LastLogTerm         int
}

type RequestVoteReply struct {
	Term                int // term of the current peer
    VoteGranted         bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.validateTerm(args.Term) {
        rf.handler.handleVote(args, reply)
    } else {
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
    }
}

type AppendEntriesArgs struct {
    Term                int     // leader term
    LeaderId            int
    PrevLogIndex        int
    PrevLogTerm         int
    Entries             []LogEntry
    LeaderCommit        int
}

type AppendEntriesReply struct {
    Term                int     // the receiver's term
    Success             bool
    TermHint            int
    IndexHint           int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.validateTerm(args.Term) {
        rf.handler.appendEntries(args, reply)
    } else {
        reply.Term = rf.currentTerm
        reply.Success = false
    }
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
// @returns index,term,isLeader
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    return rf.handler.submit(command)
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
// @pre rf.mu.Locked
func (rf *Raft) Kill() {
	// Your code here, if desired.
    rf.mu.Lock()
    defer rf.mu.Unlock()

    rf.currentTerm++
    rf.handler.leave()
    rf.handler = &KilledHandler{}
    rf.handler.enter(rf)

    DPrintf("Kill(me: %v, term: %v, commitIndex: %v, applied: %v)",
        rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied)
}

func (rf *Raft) ToDebugString() string {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    return fmt.Sprintf("me: %v, term: %v, commitIndex: %v, applied: %v, handler: %v, log: %v",
        rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.handler.toDebugString(), rf.log)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
    rf.applier = applyCh
    rf.becomeFollower()
    rf.log = []LogEntry{ LogEntry{0, 0, nil} }

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
