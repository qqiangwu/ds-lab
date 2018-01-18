package raft

import "time"
import "math/rand"
import "sync"

func randomTimeoutMillis() time.Duration {
    const TIMEOUT_MIN = 300
    const TIMEOUT_MAX = 600

    randVal := rand.Intn(TIMEOUT_MAX - TIMEOUT_MIN) + TIMEOUT_MIN

    return time.Duration(randVal) * time.Millisecond
}

type SafeTimer struct {
    mu         *sync.Mutex
    callback   func()
    underlying *time.Timer
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

type FollowerHandler struct {
    rf              *Raft
    votedFor        int
    heartbeatTimer  *SafeTimer
}

const NOT_VOTE = -1

func (hd *FollowerHandler) resetTimer() {
    hd.stopTimer()

    timeout := randomTimeoutMillis()
    hd.heartbeatTimer = safeAfterFunc(&hd.rf.mu, timeout, hd.onHeartbeatTimeout)
}

func (hd *FollowerHandler) stopTimer() {
    if hd.heartbeatTimer != nil {
        hd.heartbeatTimer.cancel()
        hd.heartbeatTimer = nil
    }
}

// must be serialized
func (hd *FollowerHandler) onHeartbeatTimeout() {
    hd.rf.becomeCandidate()
}

func (hd *FollowerHandler) enter(rf *Raft) {
    hd.rf = rf
    hd.votedFor = NOT_VOTE
    hd.resetTimer()
}

func (hd *FollowerHandler) leave() {
    hd.stopTimer()
}

func (hd *FollowerHandler) getState() FsmState {
    return FOLLOWER
}

func (hd *FollowerHandler) canGrant(args *RequestVoteArgs) bool {
    return hd.votedFor == NOT_VOTE || hd.votedFor == args.CandidateId
}

// @pre args.Term == rf.currentTerm
func (hd *FollowerHandler) handleVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    reply.Term = hd.rf.currentTerm
    reply.VoteGranted = hd.canGrant(args)

    if reply.VoteGranted {
        hd.votedFor = args.CandidateId
        hd.resetTimer()
    }
}

// @pre args.Term == rf.currentTerm
func (hd *FollowerHandler) appendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    hd.resetTimer()

    reply.Term = hd.rf.currentTerm
    reply.Success = true
}

type CandidateHandler struct {
    rf              *Raft
    electionTimer  *SafeTimer
}

func (hd *CandidateHandler) resetTimer() {
    hd.stopTimer()

    timeout := randomTimeoutMillis()
    hd.electionTimer = safeAfterFunc(&hd.rf.mu, timeout, hd.onElectionTimeout)
}

func (hd *CandidateHandler) stopTimer() {
    if hd.electionTimer != nil {
        hd.electionTimer.cancel()
        hd.electionTimer = nil
    }
}

// must be serialized
func (hd* CandidateHandler) onElectionTimeout() {
    DPrintf("ElectionTimeout(me: %v, term: %v)", hd.rf.me, hd.rf.currentTerm)

    hd.rf.becomeCandidate()
}

func (hd *CandidateHandler) enter(rf *Raft) {
    hd.rf = rf
    hd.resetTimer()
    go hd.startElection(rf.currentTerm)
}

func (hd *CandidateHandler) leave() {
    hd.stopTimer()
}

func (hd *CandidateHandler) getState() FsmState {
    return CANDIDATE
}

func (hd *CandidateHandler) canGrant(args *RequestVoteArgs) bool {
    return hd.rf.me == args.CandidateId
}

func (hd *CandidateHandler) startElection(term int) {
    args := &RequestVoteArgs{}
    args.Term = term
    args.CandidateId = hd.rf.me

    // FIXME: parallel
    votes := 1
    promoted := false
    for i, peer := range hd.rf.peers {
        if i != hd.rf.me {
            reply := &RequestVoteReply{}
            success := peer.Call("Raft.RequestVote", args, reply)
            if success && reply.VoteGranted {
                votes++
                if votes * 2 > len(hd.rf.peers) && !promoted {
                    promoted = hd.tryPromote(term)
                    if !promoted {
                        // stop further invoking
                        return
                    }
                }
            }
        }
    }
}

func (hd *CandidateHandler) tryPromote(term int) bool {
    hd.rf.mu.Lock()
    defer hd.rf.mu.Unlock()

    if term == hd.rf.currentTerm {
        hd.rf.becomeLeader()
        return true
    }

    return false
}

// @pre args.Term == rf.currentTerm
func (hd *CandidateHandler) handleVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    reply.Term = hd.rf.currentTerm
    reply.VoteGranted = hd.canGrant(args)
}

// @pre args.Term == rf.currentTerm
func (hd *CandidateHandler) appendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    hd.rf.becomeFollower()

    reply.Success = true
    reply.Term = hd.rf.currentTerm
}

type Appender struct {
    rf          *Raft
    term        int
    stopReceiver     chan(bool)
    stopNotifier     chan(bool)
}

func makeAppender(leader *LeaderHandler) *Appender {
    appender := &Appender{}
    appender.rf = leader.rf
    appender.term = leader.rf.currentTerm
    appender.stopReceiver = make(chan bool)
    appender.stopNotifier = make(chan bool)

    go appender.run()

    return appender
}

func (appender *Appender) stopAndWait() {
    appender.stopReceiver <- true
    <-appender.stopNotifier
}

func (appender *Appender) run() {
    DPrintf("Leader.heartbeat(me: %v, term: %v)", appender.rf.me, appender.term)

    timeout := time.Duration(100) * time.Millisecond

    defer func(){
        appender.stopNotifier <- true
    }()
    appender.doSend()

    for {
        select {
            // FIXME take too long to quit: make it orphan is enough
        case <-appender.stopReceiver:
            return

        case <-time.After(timeout):
            appender.doSend()
        }
    }
}

func (appender *Appender) doSend() {
    args := &AppendEntriesArgs{}
    args.Term = appender.term
    args.LeaderId = appender.rf.me

    for i, peer := range appender.rf.peers {
        if i != appender.rf.me {
            reply := &AppendEntriesReply{}
            peer.Call("Raft.AppendEntries", args, reply)
            // TODO handle the results
        }
    }
}

type LeaderHandler struct {
    rf              *Raft
    appender        *Appender
}

func (hd *LeaderHandler) enter(rf *Raft) {
    hd.rf = rf
    hd.appender = makeAppender(hd)
}

// @pre hd.appender != nil
func (hd *LeaderHandler) leave() {
    hd.appender.stopAndWait()
    hd.appender = nil
}

func (hd *LeaderHandler) getState() FsmState {
    return LEADER
}

// @pre args.Term == rf.currentTerm
func (hd *LeaderHandler) handleVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    reply.Term = hd.rf.currentTerm
    reply.VoteGranted = false
}

// @pre args.Term == rf.currentTerm
func (hd *LeaderHandler) appendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    reply.Term = hd.rf.currentTerm
    reply.Success = false
}
