package raft

import "time"
import "math/rand"
import "sync"
import "sync/atomic"
import "fmt"
import "bytes"
import "encoding/gob"

const ELECTION_TIMEOUT_MIN = 400
const ELECTION_TIMEOUT_MAX = 800
const HEARTBEAT_INTERVAL = 100

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

func (hd *KilledHandler) toDebugString() string {
    return "KILLED"
}

type FollowerHandler struct {
    rf              *Raft
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
    hd.rf.votedFor = NOT_VOTE
    hd.resetTimer()
}

// @pre rf.mu.Locked
func (hd *FollowerHandler) leave() {
    hd.stopTimer()
    hd.rf.votedFor = NOT_VOTE
}

// @pre rf.mu.Locked
func (hd *FollowerHandler) getState() FsmState {
    return FOLLOWER
}

// @pre rf.mu.Locked
func (hd *FollowerHandler) canGrant(args *RequestVoteArgs) bool {
    if hd.rf.votedFor == NOT_VOTE {
        lastIndex, lastTerm := hd.rf.log.TailMeta()

        if args.LastLogTerm > lastTerm {
            return true
        } else if args.LastLogTerm == lastTerm {
            return args.LastLogIndex >= lastIndex
        } else {
            return false
        }
    } else if hd.rf.votedFor == args.CandidateId {
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
        DPrintf("Vote(me: %v, term: %v, for: %v)\n", hd.rf.me, hd.rf.currentTerm, args.CandidateId)

        hd.rf.votedFor = args.CandidateId

        // voted
        hd.rf.persist()
        hd.resetTimer()
    }
}

// @prev prevIndex is not in snapshot
func (hd *FollowerHandler) isPrefixMatch(prevIndex int, prevTerm int) bool {
    lastIndex, _ := hd.rf.log.TailMeta()

    return prevIndex <= lastIndex && hd.rf.log.GetTerm(prevIndex) == prevTerm
}

// @pre args.Term == rf.currentTerm
// @pre rf.mu.Locked
func (hd *FollowerHandler) appendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    hd.resetTimer()

    reply.Term = hd.rf.currentTerm

    if hd.rf.log.InSnapshot(args.PrevLogIndex + 1) {
        //reply.Success = true
        //reply.IndexHint = hd.rf.log.LastIndex + 1
        //reply.TermHint = hd.rf.log.LastTerm
        reply.Success = false
        reply.IndexHint = -1
        reply.TermHint = -1
    } else {
        prefixMatch := hd.isPrefixMatch(args.PrevLogIndex, args.PrevLogTerm)
        reply.Success = prefixMatch

        if prefixMatch {
            if len(args.Entries) != 0 {
                firstNonMatchIndex, entryIndex := hd.rf.log.FindFirstNonMatch(args.Entries)
                hd.rf.log.RemoveFrom(firstNonMatchIndex)
                hd.rf.log.AppendRange(args.Entries[entryIndex:])

                // rf.log changes
                hd.rf.persist()
            }
            if args.LeaderCommit > hd.rf.commitIndex {
                lastIndex, _ := hd.rf.log.TailMeta()
                hd.rf.commitIndex = min(args.LeaderCommit, lastIndex)
                hd.rf.applyNew()
            }
        } else {
            lastIndex, lastTerm := hd.rf.log.TailMeta()

            if lastIndex < args.PrevLogIndex {
                // no conflicts, self log is too short
                reply.TermHint = lastTerm
                reply.IndexHint = lastIndex
            } else {
                // conflicts
                reply.TermHint = lastTerm
                reply.IndexHint = hd.rf.log.FindTermFirstIndex(args.PrevLogIndex)
            }
        }
    }
}

// @pre rf.mu.Locked
func (hd *FollowerHandler) submit(command interface{}) (int, int, bool) {
    return -1, -1, false
}

func (hd *FollowerHandler) toDebugString() string {
    return "FOLLOWER"
}

type CandidateHandler struct {
    rf              *Raft
    electionTimer   *SafeTimer
    votingFinished  int32
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

    lastIndex, lastTerm := rf.log.TailMeta()
    go hd.startElection(rf.currentTerm, lastIndex, lastTerm)
}

// @pre rf.mu.Locked
func (hd *CandidateHandler) leave() {
    hd.stopTimer()
    hd.stopVoting()
}

func (hd *CandidateHandler) stopVoting() {
    atomic.StoreInt32(&hd.votingFinished, 1)
}

func (hd *CandidateHandler) isVoting() bool {
    return atomic.LoadInt32(&hd.votingFinished) == 0
}

// @pre rf.mu.Locked
func (hd *CandidateHandler) getState() FsmState {
    return CANDIDATE
}

// @pre rf.mu.Locked
func (hd *CandidateHandler) canGrant(args *RequestVoteArgs) bool {
    return hd.rf.me == args.CandidateId
}

// run in standalone goroutine
func (hd *CandidateHandler) startElection(term int, lastLogIndex int, lastLogTerm int) {
    args := &RequestVoteArgs{}
    args.Term = term
    args.CandidateId = hd.rf.me
    args.LastLogIndex = lastLogIndex
    args.LastLogTerm = lastLogTerm

    result := make(chan bool, len(hd.rf.peers))

    for peer := range hd.rf.peers {
        if peer != hd.rf.me {
            go hd.askVoteFrom(args, peer, result)
        } else {
            result <- true
        }
    }

    voted := 0
    unvote := 0
    for i := 0; i < len(hd.rf.peers); i++ {
        voteGranted := <-result
        if voteGranted {
            voted++
            if voted * 2 > len(hd.rf.peers) {
                hd.tryPromote(term)
                break
            }
        } else {
            unvote++
            if unvote * 2 > len(hd.rf.peers) {
                DPrintf("LoseVote(me: %v, term: %v)\n", hd.rf.me, term)
                hd.stopVoting()
                break
            }
        }
    }
}

// run in standalone goroutine
func (hd *CandidateHandler) askVoteFrom(args *RequestVoteArgs, peer int, result chan bool) {
    reply := &RequestVoteReply{}

    for hd.isVoting() {
        success := hd.rf.peers[peer].Call("Raft.RequestVote", args, reply)

        if success {
            break
        }

        // retry when unreliable
    }

    result <- reply.VoteGranted
}

// called by election goroutine
func (hd *CandidateHandler) tryPromote(term int) bool {
    hd.rf.mu.Lock()
    defer hd.rf.mu.Unlock()

    hd.stopVoting()

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

func (hd *CandidateHandler) toDebugString() string {
    return "CANDIDATE"
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
    var ts int32

    for appender.isRunning() {
        ch := make(chan bool)
        go func(){
            ch <- appender.doSend(peer, &ts)
        }()

        select {
        case retry := <-ch:
            if !retry && appender.isRunning() {
                time.Sleep(timeout)
            }

        case <-time.After(timeout):
        }
    }
}

func (appender *Appender) doSend(peer int, ts *int32) (retry bool) {
    seq := atomic.AddInt32(ts, 1)

    args, snapshot := appender.leader.retrieveEntries(peer)
    if args == nil && snapshot == nil {
        DPrintf("LeaderLegacy(me: %v, term: %v)", appender.rf.me, appender.term)
        return false
    } else if snapshot != nil {
        DPrintf("SendSnapshot(me: %v, term: %v, peer: %v, sterm: %v, sindex: %v)",
            appender.rf.me, appender.term, peer, snapshot.LastIncludedTerm, snapshot.LastIncludedIndex)
        appender.rf.peers[peer].Call("Raft.InstallSnapshot", snapshot, &InstallSnapshotReply{})
        return false
    } else {
        reply := &AppendEntriesReply{}
        success := appender.rf.peers[peer].Call("Raft.AppendEntries", args, reply)
        if seq < atomic.LoadInt32(ts) {
            // what about wrap?
            return false
        } else if success {
            if reply.Success {
                appender.leader.onFollowerOk(peer, args)
            } else {
                snapshot := appender.leader.onFollowerFailed(peer, args, reply)
                if snapshot != nil {
                    DPrintf("SendSnapshot(me: %v, term: %v, peer: %v, sterm: %v, sindex: %v)",
                        appender.rf.me, appender.term, peer, snapshot.LastIncludedTerm, snapshot.LastIncludedIndex)
                    // send snapshot and don't retry
                    sreply := InstallSnapshotReply{}
                    appender.rf.peers[peer].Call("Raft.InstallSnapshot", snapshot, &sreply)
                    if sreply.Term > appender.leader.term {
                        // ignored: actually we can stop being leader here
                    }
                    return false
                }
            }

            isSyncing := len(args.Entries) != 0

            // retry if syncing or not match
            return isSyncing || !reply.Success
        } else {
            // retry if connecting failed
            return true
        }
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
    return hd != hd.rf.handler
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

func (hd *LeaderHandler) isPeerAlreadySnapshot(reply *AppendEntriesReply) bool {
    return reply.IndexHint < 0
}

func (hd *LeaderHandler) onFollowerFailed(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) (snapshot *InstallSnapshotArgs){
    hd.rf.mu.Lock()
    defer hd.rf.mu.Unlock()

    if hd.isLegacy() {
        DPrintf("LegacyLeaderAck(me: %v, term: %v, newTerm: %v)\n", hd.rf.me, hd.term, reply.Term)
    } else if hd.term < reply.Term {
        DPrintf("LegacyLeader(me: %v, term: %v, newTerm: %v)\n", hd.rf.me, hd.term, reply.Term)

        hd.appender.stop()
        hd.rf.becomeFollower()
    } else if hd.isPeerAlreadySnapshot(reply) {
        // 可能来源于一旧新消息的返回值
        // 保证自己不死就行了，不用管别人
        // 但是Raft是一个完整的组成部分，任意一部分出问题都会响应其他部分啊
        DPrintf("PeerSnapshoted(me: %v, term: %v: peer: %v, prevIndex: %v): peer snapshot uncommitted entries", 
            hd.rf.me, hd.term, peer, args.PrevLogIndex)
    } else if reply.IndexHint >= hd.nextIndex[peer] {
        // duplicated response
    } else {
        if hd.rf.log.InSnapshot(reply.IndexHint) {
            lastIndex, lastTerm, data := hd.loadSnapshot(peer)

            // the previous is sent
            snapshot = &InstallSnapshotArgs{
                Term: hd.term,
                LeaderId: hd.rf.me,
                LastIncludedIndex: lastIndex,
                LastIncludedTerm: lastTerm,
                Offset: 0,
                Data: data,
                Done: true}

            hd.nextIndex[peer] = lastIndex + 1
        } else if term := hd.rf.log.GetTerm(reply.IndexHint); term == reply.TermHint {
            hd.nextIndex[peer] = reply.IndexHint + 1
        } else {
            hd.nextIndex[peer] = reply.IndexHint
        }
    }

    return snapshot
}

// @pre rf.mu.Locked
func (hd *LeaderHandler) loadSnapshot(peer int) (lastIndex int, lastTerm int, data []byte) {
    data = hd.rf.persister.ReadSnapshot()

    if data == nil || len(data) == 0 {
        return 0, 0, data
    }

    // read meta
    buffer := bytes.NewBuffer(data)
    decoder := gob.NewDecoder(buffer)
    decoder.Decode(&lastIndex)
    decoder.Decode(&lastTerm)

    return lastIndex, lastTerm, data
}

// called by Appender
func (hd *LeaderHandler) retrieveEntries(peer int) (*AppendEntriesArgs, *InstallSnapshotArgs) {
    hd.rf.mu.Lock()
    defer hd.rf.mu.Unlock()

    if hd.isLegacy() {
        return nil, nil
    }

    args := &AppendEntriesArgs{}
    args.Term = hd.term
    args.LeaderId = hd.rf.me
    args.LeaderCommit = hd.rf.commitIndex

    if hd.rf.log.InSnapshot(hd.nextIndex[peer]) {
        lastIndex, lastTerm, data := hd.loadSnapshot(peer)

        // the previous is sent
        snapshot := &InstallSnapshotArgs{
            Term: hd.term,
            LeaderId: hd.rf.me,
            LastIncludedIndex: lastIndex,
            LastIncludedTerm: lastTerm,
            Offset: 0,
            Data: data,
            Done: true}

        hd.nextIndex[peer] = lastIndex + 1
        return nil, snapshot
    }

    lastIndex, _ := hd.rf.log.TailMeta()
    if lastIndex >= hd.nextIndex[peer] {
        args.Entries = hd.rf.log.GetFrom(hd.nextIndex[peer])
    }

    if len(args.Entries) == 0 {
        args.PrevLogIndex = lastIndex
    } else {
        headIndex := args.Entries[0].Index
        args.PrevLogIndex = headIndex - 1
    }

    args.PrevLogTerm = hd.rf.log.GetTerm(args.PrevLogIndex)

    return args, nil
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

    nextIndex := rf.log.NextIndex()
    for i := range hd.nextIndex {
        hd.nextIndex[i] = nextIndex
    }

    hd.appender = makeAppender(hd)
}

// @pre hd.appender != nil
// @pre rf.mu.Locked
func (hd *LeaderHandler) leave() {
    hd.appender.stop()
    // hd.appender = nil
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
    nextIndex := hd.rf.log.NextIndex()
    term := hd.rf.currentTerm

    hd.rf.log.AppendEntry(LogEntry{term, nextIndex, command})
    hd.rf.persist()
    hd.matchIndex[hd.rf.me] = nextIndex

    return nextIndex, term, true
}

// @pre rf.mu.Locked
func (hd* LeaderHandler) toDebugString() string {
    return fmt.Sprintf("LEADER(next: %v, match: %v)", hd.nextIndex, hd.matchIndex)
}
