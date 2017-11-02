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
import "time"
import "math/rand"

// import "bytes"
// import "encoding/gob"

const TIMEOUT_MIN = 300
const TIMEOUT_MAX = 600



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

type FsmState int

const (
    FOLLOWER FsmState = iota
    CANDIDATE
    LEADER
)

type RoleHandler interface {
    enter(rf *Raft)
    leave()

    handleVote(args *RequestVoteArgs, reply *RequestVoteReply)
    appendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
    role      FsmState
    handler   RoleHandler

    currentTerm int
    votedFor    int
}

type FollowerHandler struct {
    rf               *Raft
    heartbeatTimer   *time.Timer
}

func (hd *FollowerHandler) enter(rf *Raft) {
    hd.rf = rf
    hd.resetTimer()
}

func (hd *FollowerHandler) leave() {
    hd.heartbeatTimer.Stop()
    hd.heartbeatTimer = nil
}

func (hd *FollowerHandler) handleVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    if hd.rf.currentTerm > args.Term {
        reply.Term = hd.rf.currentTerm
        reply.VoteGranted = false
        return
    }

    reply.Term = hd.rf.currentTerm
    reply.VoteGranted = (hd.rf.votedFor == -1) || (hd.rf.votedFor == args.CandidateId)

    if reply.VoteGranted && hd.rf.votedFor == -1 {
        hd.rf.votedFor = args.CandidateId
    }
    if reply.VoteGranted {
        hd.resetTimer()
    }
}

func (hd *FollowerHandler) appendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    if hd.rf.currentTerm > args.Term {
        reply.Term = hd.rf.currentTerm
        reply.Success = false
    }

    // TODO
    reply.Term = hd.rf.currentTerm
    reply.Success = true
    hd.resetTimer()
}

func (hd *FollowerHandler) onHeartbeatTimeout() {
    hd.rf.mu.Lock()
    defer hd.rf.mu.Unlock()

    // eliminate legacy timers: actually there are potential bugs but we can safely ignore it
    if hd.rf.handler != hd {
        return
    }

    DPrintf("HeartbeatTimeout(me: %v, term: %v)", hd.rf.me, hd.rf.currentTerm)
    hd.rf.becomeCandidate()
}

func (hd *FollowerHandler) resetTimer() {
    if hd.heartbeatTimer != nil {
        hd.heartbeatTimer.Stop()
        hd.heartbeatTimer = nil
    }
    timeout := time.Duration(rand.Intn(TIMEOUT_MAX - TIMEOUT_MIN) + TIMEOUT_MIN) * time.Millisecond
    hd.heartbeatTimer = time.AfterFunc(timeout, hd.onHeartbeatTimeout)
}

type CandidateHandler struct {
    rf              *Raft
    electionTimer   *time.Timer
}

func (hd *CandidateHandler) resetTimer() {
    timeout := time.Duration(rand.Intn(TIMEOUT_MAX - TIMEOUT_MIN) + TIMEOUT_MIN) * time.Millisecond
    hd.electionTimer = time.AfterFunc(timeout, func(term int) func(){
        return func() { hd.onElectionTimeout(term) }
    }(hd.rf.currentTerm))
}

func (hd *CandidateHandler) onElectionTimeout(term int) {
    hd.rf.mu.Lock()
    defer hd.rf.mu.Unlock()

    // eliminate legacy timers
    if hd.rf.handler != hd || hd.rf.currentTerm != term {
        return
    }

    DPrintf("CandidateTimeout(me: %v, term: %v)", hd.rf.me, hd.rf.currentTerm)
    hd.rf.becomeCandidate()
}

func (hd *CandidateHandler) startElection(rf *Raft, me int, term int, peers []*labrpc.ClientEnd) {
    args := &RequestVoteArgs{}
    args.Term = term
    args.CandidateId = me

    votes := 1
    for i, peer := range peers {
        if i != me {
            reply := &RequestVoteReply{}
            if peer.Call("Raft.RequestVote", args, reply) && reply.VoteGranted {
                votes++

                if votes * 2 > len(peers) {
                    rf.mu.Lock()
                    if rf.currentTerm == term {
                        rf.becomeLeader()
                    }
                    rf.mu.Unlock()
                    return
                }
            }
        }
    }
}

func (hd *CandidateHandler) enter(rf *Raft) {
    hd.rf = rf
    hd.rf.currentTerm++
    hd.rf.votedFor = hd.rf.me
    hd.resetTimer()
    go hd.startElection(rf, rf.me, rf.currentTerm, rf.peers)
}

func (hd *CandidateHandler) leave() {
    hd.electionTimer.Stop()
    hd.electionTimer = nil
}

func (hd *CandidateHandler) handleVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    if hd.rf.currentTerm > args.Term {
        reply.Term = hd.rf.currentTerm
        reply.VoteGranted = false
        return
    }

    reply.Term = hd.rf.currentTerm
    reply.VoteGranted = hd.rf.votedFor == args.CandidateId
}

func (hd *CandidateHandler) appendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    if hd.rf.currentTerm > args.Term {
        reply.Term = hd.rf.currentTerm
        reply.Success = false
    }

    hd.rf.becomeFollower()

    reply.Term = hd.rf.currentTerm
    reply.Success = true
}

type Transmitter struct {
    me          int
    term        int
    peers       []*labrpc.ClientEnd
    stopCmd     chan bool
}

func (tm *Transmitter) start(me int, term int, peers []*labrpc.ClientEnd) {
    tm.me = me
    tm.term = term
    tm.peers = peers
    tm.stopCmd = make(chan bool)

    go tm.run()
}

func (tm *Transmitter) run() {
    timeout := time.Duration(100) * time.Millisecond

    DPrintf("StartHeartbeat(me: %v)", tm.me)

    tm.doSend()

    for {
        select {
        case <-tm.stopCmd:
            return

        case <-time.After(timeout):
            tm.doSend()
        }
    }
}

func (tm *Transmitter) doSend() {
    // FIXME: parallel
    args := &AppendEntriesArgs{}
    args.Term = tm.term
    args.LeaderId = tm.me

    for i, peer := range tm.peers {
        if i != tm.me {
            reply := &AppendEntriesReply{}
            peer.Call("Raft.AppendEntries", args, reply)
        }
    }
}

func (tm *Transmitter) stop() {
    if tm.stopCmd != nil {
        tm.stopCmd <- true
        tm.stopCmd = nil
    }
}

type LeaderHandler struct {
    rf              *Raft
    transmitter     Transmitter
}

func (hd *LeaderHandler) enter(rf *Raft) {
    hd.rf = rf
    hd.transmitter.start(rf.me, rf.currentTerm, rf.peers)
}

func (hd *LeaderHandler) leave() {
    hd.transmitter.stop()
}

func (hd *LeaderHandler) handleVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    if hd.rf.currentTerm > args.Term {
        reply.Term = hd.rf.currentTerm
        reply.VoteGranted = false
        return
    }

    reply.Term = hd.rf.currentTerm
    reply.VoteGranted = false
}

func (hd *LeaderHandler) appendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

}

func (rf *Raft) becomeFollower() {
    if rf.handler != nil {
        rf.handler.leave()
    }

    rf.role = FOLLOWER
    rf.votedFor = -1
    rf.handler = &FollowerHandler{}
    rf.handler.enter(rf)

    DPrintf("BecomeFollower(me: %v, term: %v)", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeCandidate() {
    if rf.handler != nil {
        rf.handler.leave()
    }

    rf.role = CANDIDATE
    rf.handler = &CandidateHandler{}
    rf.handler.enter(rf)

    DPrintf("BecomeCandidate(me: %v, term: %v)", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeLeader() {
    if rf.handler != nil {
        rf.handler.leave()
    }

    rf.role = LEADER
    rf.handler = &LeaderHandler{}
    rf.handler.enter(rf)

    DPrintf("BecomeLeader(me: %v, term: %v)", rf.me, rf.currentTerm)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    return rf.currentTerm, rf.role == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

func (rf *Raft) validateTerm(requestTerm int) {
    if rf.currentTerm < requestTerm {
        rf.currentTerm = requestTerm
        rf.becomeFollower()
    }
}

type RequestVoteArgs struct {
    Term            int
    CandidateId     int
    LastLogIndex    int
    LastLogTerm     int
}

type RequestVoteReply struct {
    Term            int
    VoteGranted     bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    rf.validateTerm(args.Term)
    rf.handler.handleVote(args, reply)
}

type LogEntry struct {

}

type AppendEntriesArgs struct {
    Term            int
    LeaderId        int
    PrevLogIndex    int
    PrevLogTerm     int
    Entries         []LogEntry
    LeaderCommit    int
}

type AppendEntriesReply struct {
    Term            int
    Success         bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    rf.validateTerm(args.Term)
    rf.handler.appendEntries(args, reply)
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    DPrintf("Kill(me: %v)", rf.me)

    rf.handler.leave()
    rf.handler = nil
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
    rf.currentTerm = 0
    rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
    // FIXME:   crashed follower recovers
    rf.becomeFollower()

	return rf
}
