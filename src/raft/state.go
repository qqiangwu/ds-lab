package raft

import "time"
import "math/rand"
import "sync"
import "sync/atomic"
import "labrpc"

const ELECTION_TIMEOUT_MIN = 300
const ELECTION_TIMEOUT_MAX = 800
const HEARTBEAT_INTERVAL = 200

func randomTimeoutMillis() time.Duration {
    randVal := rand.Intn(ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN

    return time.Duration(randVal) * time.Millisecond
}

type SafeTimer struct {
    mu         *sync.Mutex

    // guarded by mu
    underlying *time.Timer
    callback   func()
    canceled   bool
}

func safeAfterFunc(mu *sync.Mutex, d time.Duration, callback func()) *SafeTimer {
    timer := &SafeTimer{}

    timer.mu = mu
    timer.callback = callback
    timer.underlying = time.AfterFunc(d, callback)

    return timer
}

func (timer *SafeTimer) onTimeout() {
    timer.mu.Lock()
    defer timer.mu.Unlock()

    if !timer.canceled {
        timer.callback()
    }
}

// @pre mu.Locked
func (timer *SafeTimer) cancel() {
    // assert lock held
    timer.underlying.Stop()
    timer.canceled = true
    timer.underlying = nil
}

type KilledHandler struct {
}

func (hd *KilledHandler) enter(rf *Raft) {
}

func (hd *KilledHandler) leave() {
    panic("KilledHandler.leave")
}

func (hd *KilledHandler) getState() FsmState {
    return KILLED
}

func (hd *KilledHandler) handleVote(args *RequestVoteArgs, reply *RequestVoteReply) {
}

func (hd *KilledHandler) appendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
}

// @pre rf.mu.Locked
func (hd *KilledHandler) submit(command interface{}) (int, int, bool) {
    return -1, -1, false
}


type FollowerHandler struct {
    rf              *Raft
    votedFor        int
    heartbeatTimer  *SafeTimer
}

const NOT_VOTE = -1

// @pre rf.mu.Locked
func (hd *FollowerHandler) resetTimer() {
    hd.stopTimer()

    timeout := randomTimeoutMillis()
    hd.heartbeatTimer = safeAfterFunc(&hd.rf.mu, timeout, hd.onHeartbeatTimeout)
}

// @pre rf.mu.Locked
func (hd *FollowerHandler) stopTimer() {
    if hd.heartbeatTimer != nil {
        hd.heartbeatTimer.cancel()
        hd.heartbeatTimer = nil
    }
}

// @pre rf.mu.Locked
func (hd *FollowerHandler) onHeartbeatTimeout() {
    DPrintf("FollowerTimeout(me:%v, term: %v)", hd.rf.me, hd.rf.currentTerm)
    hd.rf.becomeCandidate()
}

// @pre rf.mu.Locked
func (hd *FollowerHandler) enter(rf *Raft) {
    hd.rf = rf
    hd.votedFor = NOT_VOTE
    hd.resetTimer()
}

// @pre rf.mu.Locked
func (hd *FollowerHandler) leave() {
    hd.stopTimer()
}

// @pre rf.mu.Locked
func (hd *FollowerHandler) getState() FsmState {
    return FOLLOWER
}

// @pre rf.mu.Locked
func (hd *FollowerHandler) canGrant(args *RequestVoteArgs) bool {
    if hd.votedFor == NOT_VOTE {
        lastLog := tail(hd.rf.log)

        if args.LastLogTerm > lastLog.Term {
            return true
        } else if args.LastLogTerm == lastLog.Term {
            return args.LastLogIndex >= lastLog.Index
        } else {
            return false
        }
    } else if hd.votedFor == args.CandidateId {
        return true
    } else {
        return false
    }
}

// @pre args.Term == rf.currentTerm
// @pre rf.mu.Locked
func (hd *FollowerHandler) handleVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    reply.Term = hd.rf.currentTerm
    reply.VoteGranted = hd.canGrant(args)

    if reply.VoteGranted {
        hd.votedFor = args.CandidateId
        hd.resetTimer()
    }
}

func findFirstNonMatch(log []LogEntry, trailer []LogEntry) (int, int) {
    for i, v := range trailer {
        if existInLog := v.Index < len(log); !existInLog {
            return v.Index, i
        }
        if match := v.Term == log[v.Index].Term; !match {
            return v.Index, i
        }
    }

    return len(log), len(trailer)
}

// @pre args.Term == rf.currentTerm
// @pre rf.mu.Locked
func (hd *FollowerHandler) appendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    hd.resetTimer()

    prefixMatch := args.PrevLogIndex < len(hd.rf.log) && hd.rf.log[args.PrevLogIndex].Term == args.PrevLogTerm

    reply.Term = hd.rf.currentTerm
    reply.Success = prefixMatch

    if prefixMatch {
        if len(args.Entries) != 0 {
            firstNonMatchIndex, entryIndex := findFirstNonMatch(hd.rf.log, args.Entries)
            hd.rf.log = append(hd.rf.log[:firstNonMatchIndex], args.Entries[entryIndex:]...)
        }
        if args.LeaderCommit > hd.rf.commitIndex {
            hd.rf.commitIndex = min(args.LeaderCommit, tail(hd.rf.log).Index)

            hd.rf.applyNew()
        }
    }
}

// @pre rf.mu.Locked
func (hd *FollowerHandler) submit(command interface{}) (int, int, bool) {
    return -1, -1, false
}

type CandidateHandler struct {
    rf              *Raft
    electionTimer   *SafeTimer
}

// @pre rf.mu.Locked
func (hd *CandidateHandler) resetTimer() {
    hd.stopTimer()

    timeout := randomTimeoutMillis()
    hd.electionTimer = safeAfterFunc(&hd.rf.mu, timeout, hd.onElectionTimeout)
}

// @pre rf.mu.Locked
func (hd *CandidateHandler) stopTimer() {
    if hd.electionTimer != nil {
        hd.electionTimer.cancel()
        hd.electionTimer = nil
    }
}

// @pre rf.mu.Locked
func (hd* CandidateHandler) onElectionTimeout() {
    DPrintf("ElectionTimeout(me: %v, term: %v)", hd.rf.me, hd.rf.currentTerm)

    hd.rf.becomeCandidate()
}

// @pre rf.mu.Locked
func (hd *CandidateHandler) enter(rf *Raft) {
    hd.rf = rf
    hd.resetTimer()

    lastLog := tail(rf.log)
    go hd.startElection(rf.currentTerm, lastLog.Index, lastLog.Term)
}

// @pre rf.mu.Locked
func (hd *CandidateHandler) leave() {
    hd.stopTimer()
}

// @pre rf.mu.Locked
func (hd *CandidateHandler) getState() FsmState {
    return CANDIDATE
}

// @pre rf.mu.Locked
func (hd *CandidateHandler) canGrant(args *RequestVoteArgs) bool {
    return hd.rf.me == args.CandidateId
}

func (hd *CandidateHandler) startElection(term int, lastLogIndex int, lastLogTerm int) {
    args := &RequestVoteArgs{}
    args.Term = term
    args.CandidateId = hd.rf.me
    args.LastLogIndex = lastLogIndex
    args.LastLogTerm = lastLogTerm

    result := make(chan bool, len(hd.rf.peers))

    for i, peer := range hd.rf.peers {
        if i != hd.rf.me {
            go func(peer *labrpc.ClientEnd) {
                reply := &RequestVoteReply{}
                success := peer.Call("Raft.RequestVote", args, reply)

                result <- success && reply.VoteGranted
            }(peer)
        } else {
            result <- true
        }
    }

    voted := 0
    for i := 0; i < len(hd.rf.peers); i++ {
        voteGranted := <-result
        if voteGranted {
            voted++
            if voted * 2 > len(hd.rf.peers) {
                hd.tryPromote(term)
                break
            }
        }
    }
}

// called by election goroutine
func (hd *CandidateHandler) tryPromote(term int) bool {
    hd.rf.mu.Lock()
    defer hd.rf.mu.Unlock()

    if term == hd.rf.currentTerm {
        hd.rf.becomeLeader()
        return true
    } else {
        DPrintf("LegacyCandidate(me: %v, term: %v)", hd.rf.me, term)
        return false
    }
}

// @pre args.Term == rf.currentTerm
// @pre rf.mu.Locked
func (hd *CandidateHandler) handleVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    reply.Term = hd.rf.currentTerm
    reply.VoteGranted = hd.canGrant(args)
}

// @pre args.Term == rf.currentTerm
// @pre rf.mu.Locked
func (hd *CandidateHandler) appendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    hd.rf.becomeFollower()
    hd.rf.handler.appendEntries(args, reply)
    // FIXME: a redundant resetTimer
}

// @pre rf.mu.Locked
func (hd *CandidateHandler) submit(command interface{}) (int, int, bool) {
    return -1, -1, false
}

// background task
type Appender struct {
    rf              *Raft
    leader          *LeaderHandler
    term            int
    stopped         int32
}

func makeAppender(leader *LeaderHandler) *Appender {
    appender := &Appender{}
    appender.rf = leader.rf
    appender.leader = leader
    appender.term = leader.rf.currentTerm
    appender.stopped = 0

    for i := range appender.rf.peers {
        if i != appender.rf.me {
            go appender.startSenderFor(i)
        }
    }

    return appender
}

func (appender *Appender) stop() {
    atomic.StoreInt32(&appender.stopped, 1)
}

func (appender *Appender) isRunning() bool {
    return atomic.LoadInt32(&appender.stopped) == 0
}

// @pre in a standalone goroutine
func (appender *Appender) startSenderFor(peer int) {
    const timeout = time.Duration(HEARTBEAT_INTERVAL) * time.Millisecond

    for appender.isRunning() {
        needSleep := appender.doSend(peer)

        if needSleep && appender.isRunning() {
            time.Sleep(timeout)
        }
    }
}

func (appender *Appender) doSend(peer int) (needSleep bool) {
    args := appender.leader.retrieveEntries(peer)
    if args == nil {
        DPrintf("LeaderLegacy(me: %v, term: %v)", appender.rf.me, appender.term)
        return
    }

    reply := &AppendEntriesReply{}
    success := appender.rf.peers[peer].Call("Raft.AppendEntries", args, reply)
    if success {
        if reply.Success {
            appender.leader.onFollowerOk(peer, args)
        } else {
            appender.leader.onFollowerFailed(peer, args, reply.Term)
        }

        isHeartbeat := len(args.Entries) == 0

        return isHeartbeat
    } else {
        // do sleep if cannot contact the peer
        return true
    }
}

type LeaderHandler struct {
    rf              *Raft
    term            int
    appender        *Appender
    nextIndex       []int
    matchIndex      []int
}

func (hd *LeaderHandler) isLegacy() bool {
    return hd.term < hd.rf.currentTerm
}

// called by Appender
func (hd *LeaderHandler) onFollowerOk(peer int, args *AppendEntriesArgs) {
    hd.rf.mu.Lock()
    defer hd.rf.mu.Unlock()

    if hd.isLegacy() {
        return
    }

    if len(args.Entries) != 0 {
        lastLogIndex := tail(args.Entries).Index

        if lastLogIndex > hd.matchIndex[peer] {
            DPrintf("FollowerOK(me: %v, term: %v, peer: %v, peerIndex: %v)", hd.rf.me, hd.term, peer, tail(args.Entries).Index)
            hd.nextIndex[peer] = lastLogIndex + 1
            hd.matchIndex[peer] = lastLogIndex
            hd.onFollowerAdvanced()
        }
    }
}

func (hd *LeaderHandler) onFollowerFailed(peer int, args *AppendEntriesArgs, peerTerm int) {
    hd.rf.mu.Lock()
    defer hd.rf.mu.Unlock()

    if hd.isLegacy() {
        return
    }
    if hd.term < peerTerm {
        //hd.rf.currentTerm = peerTerm
        //hd.rf.becomeFollower()
        // legacy leader, wait for new leader
    } else {
        hd.nextIndex[peer]--
    }
}

// called by Appender
func (hd *LeaderHandler) retrieveEntries(peer int) *AppendEntriesArgs {
    hd.rf.mu.Lock()
    defer hd.rf.mu.Unlock()

    if hd.isLegacy() {
        return nil
    }

    args := &AppendEntriesArgs{}
    args.Term = hd.term
    args.LeaderId = hd.rf.me
    args.LeaderCommit = hd.rf.commitIndex

    if tail(hd.rf.log).Index >= hd.nextIndex[peer] {
        args.Entries = hd.rf.log[hd.nextIndex[peer]:]
    }

    if len(args.Entries) == 0 {
        args.PrevLogIndex = tail(hd.rf.log).Index
    } else {
        headIndex := args.Entries[0].Index
        args.PrevLogIndex = headIndex - 1
    }

    args.PrevLogTerm = hd.rf.log[args.PrevLogIndex].Term

    return args
}

// @pre rf.mu.Locked
func (hd *LeaderHandler) onFollowerAdvanced() {
    nextCommitIndex := -1

    for i := hd.rf.commitIndex + 1; i <= hd.matchIndex[hd.rf.me]; i++ {
        vote := 0

        for _, idx := range hd.matchIndex {
            if idx >= i {
                vote++
            }
            if vote * 2 > len(hd.rf.peers) {
                nextCommitIndex = i
                break
            }
        }
    }

    if nextCommitIndex > 0 {
        DPrintf("Advance(me: %v, term: %v, index: %v)", hd.rf.me, hd.term, nextCommitIndex)
        hd.rf.commitIndex = nextCommitIndex
        hd.rf.applyNew()
    }
}

// @pre rf.mu.Locked
func (hd *LeaderHandler) enter(rf *Raft) {
    hd.rf = rf
    hd.term = rf.currentTerm
    hd.nextIndex = make([]int, len(rf.peers))
    hd.matchIndex = make([]int, len(rf.peers))

    lastLogIndex := tail(rf.log).Index
    for i := range hd.nextIndex {
        hd.nextIndex[i] = lastLogIndex + 1
    }

    hd.appender = makeAppender(hd)
}

// @pre hd.appender != nil
// @pre rf.mu.Locked
func (hd *LeaderHandler) leave() {
    hd.appender.stop()
    hd.appender = nil
}

// @pre rf.mu.Locked
func (hd *LeaderHandler) getState() FsmState {
    return LEADER
}

// @pre args.Term == rf.currentTerm
// @pre rf.mu.Locked
//
// never vote
func (hd *LeaderHandler) handleVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    reply.Term = hd.rf.currentTerm
    reply.VoteGranted = false
}

// @pre args.Term == rf.currentTerm
// @pre rf.mu.Locked
func (hd *LeaderHandler) appendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    reply.Term = hd.rf.currentTerm
    reply.Success = false
}

// @pre rf.mu.Locked
func (hd *LeaderHandler) submit(command interface{}) (int, int, bool) {
    nextIndex := len(hd.rf.log)
    term := hd.rf.currentTerm

    hd.rf.log = append(hd.rf.log, LogEntry{term, nextIndex, command})
    hd.matchIndex[hd.rf.me] = nextIndex

    return nextIndex, term, true
}
