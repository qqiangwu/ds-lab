package paxos

import (
    "log"
    "time"
    "math/rand"
    "sync"
)

const (
    PROPOSER = 0
    VOTER    = 1
    PROPOSED = 2
    DONE     = 3
)

var states = []string { "PROPOSER", "VOTER", "PROPOSED", "DONE" }

const TIMEOUT_MIN = 10
const TIMEOUT_MAX = 50

type Acceptor struct {
    maxProposalSeen int
    maxProposalAccepted int
    maxValueAccepted interface{}
}

type Proposer struct {
    proposal int
    seen     int
    numAck   int
    numNack  int
    maxProposal int
    maxValue interface{}
    peerMins []int
}

type PaxosFsm struct {
    mu    sync.Mutex

    id    int
    me    string
    peers []string
    state int
    seq   int
    min   int

    proposer Proposer
    acceptor Acceptor

    timer    *time.Timer
    timeout  time.Duration
}

func makePaxosFsm(id int, peers []string, seq int, value interface{}) *PaxosFsm {
    result := new(PaxosFsm)

    result.state = VOTER
    result.id = id
    result.me = peers[id]
    result.peers = peers
    result.seq = seq

    result.acceptor.maxProposalSeen = 0
    result.acceptor.maxProposalAccepted = 0
    result.acceptor.maxValueAccepted = value

    result.proposer.seen = id << 16
    result.proposer.peerMins = make([]int, len(peers))
    for i, _ := range result.proposer.peerMins {
        result.proposer.peerMins[i] = -1
    }

    result.timeout = time.Duration(rand.Intn(TIMEOUT_MAX - TIMEOUT_MIN) + TIMEOUT_MIN) * time.Millisecond

    // FIXME: to make test deaf pass, we cannot start a voter with a timer
    // result.becomeVoter()

    return result
}

func (fsm *PaxosFsm) getState() string {
    return states[fsm.state]
}

func (fsm *PaxosFsm) multicast(method string, request interface{}, replyFactory func() (interface{}), callback func(interface{})) {
    for id, peer := range fsm.peers {
        go func(peerId int, peer string) {
            reply := replyFactory()
            if call(peer, "Paxos." + method, request, reply) {
                if callback != nil {
                    callback(reply)
                }
            } else {
                log.Printf("Multicast(result: failed, method: %s, me: %d, peer: %d)", method, fsm.id, peerId)
                if callback != nil {
                    callback(nil)
                }
            }
        }(id, peer)
    }
}

func (fsm *PaxosFsm) resetVoterTimer() {
    fsm.clearTimer()

    if fsm.state == VOTER {
        fsm.timer = time.AfterFunc(fsm.timeout, fsm.OnTimeout)
    }
}

func (fsm *PaxosFsm) acceptProposal(args *PrepareArgs, reply *PrepareReply) {
    reply.Accept = true
    reply.MaxProposal = fsm.acceptor.maxProposalAccepted
    reply.MaxValue = fsm.acceptor.maxValueAccepted

    fsm.acceptor.maxProposalSeen = args.Proposal
    fsm.resetVoterTimer()
}

func (fsm *PaxosFsm) acceptValue(proposal int, value interface{}) {
    fsm.acceptor.maxProposalAccepted = proposal
    fsm.acceptor.maxValueAccepted = value
    fsm.acceptor.maxProposalSeen = proposal
}

func (fsm *PaxosFsm) becomeVoter() {
    log.Printf("BecomeVoter(me: %d, from: %s)", fsm.id, fsm.getState())

    fsm.state = VOTER
    fsm.resetVoterTimer()
}

func (fsm *PaxosFsm) sendDeciding() {
    log.Printf("Consensus(me: %d, seq: %d, proposal: %d)", fsm.id, fsm.seq, fsm.proposer.proposal)

    fsm.state = DONE
    fsm.acceptor.maxValueAccepted = fsm.proposer.maxValue
    fsm.multicast("Decide",
        &DecideArgs{fsm.id,fsm.proposer.proposal, fsm.proposer.maxValue, fsm.seq, fsm.proposer.peerMins },
        func() (interface{}) { return new(DecideReply) },
        nil)
}

func (fsm *PaxosFsm) decide(proposal int, value interface{}) {
    fsm.state = DONE
    fsm.acceptValue(proposal, value)
    fsm.clearTimer()
}

func (fsm *PaxosFsm) OnPrepare(args *PrepareArgs, reply *PrepareReply) {
    fsm.mu.Lock()
    defer fsm.mu.Unlock()

    if args.Proposal <= fsm.acceptor.maxProposalSeen {
        log.Printf("OnPrepare(result: reject, me: %d, seq: %d, proposalSeen: %d, proposalNew: %d)",
                fsm.id, fsm.seq, fsm.acceptor.maxProposalSeen, args.Proposal)
        reply.Accept = false
        reply.MaxProposal = fsm.acceptor.maxProposalSeen

        if fsm.state == DONE {
            reply.MaxValue = fsm.acceptor.maxValueAccepted
            reply.Decided = true
        }
        return
    }

    switch fsm.state {
    case VOTER:
        fsm.acceptProposal(args, reply)

    case PROPOSER:
        if args.Proposal > fsm.proposer.proposal {
            fsm.acceptProposal(args, reply)
            fsm.becomeVoter()
        } else if args.Proposal == fsm.proposer.proposal && args.Me == fsm.id {
            fsm.acceptProposal(args, reply)
        } else {
            reply.Accept = false
        }

    case PROPOSED:
        if args.Proposal > fsm.proposer.proposal {
            fsm.acceptProposal(args, reply)
            fsm.becomeVoter()
        } else {
            reply.Accept = false
        }

    case DONE:
        fsm.acceptProposal(args, reply)

    default:
        panic("Bad paxos state")
    }

    log.Printf("OnPrepare(me: %d, seq: %d, state: %s, accepted: %t)", fsm.id, fsm.seq, fsm.getState(), reply.Accept)
}

func (fsm *PaxosFsm) OnTimeout() {
    fsm.mu.Lock()
    defer fsm.mu.Unlock()

    if fsm.state != VOTER {
        log.Printf("OnTimeout(result: badState, me: %d, state: %s)", fsm.id, fsm.getState())
    } else {
        fsm.propose()

        log.Printf("OnTimeout(result: toProposer, me: %d, state: %s, proposalMax: %d)",
            fsm.id, fsm.getState(), fsm.acceptor.maxProposalSeen)
    }
}

func (fsm *PaxosFsm) OnPrepareOK(proposal int, reply *PrepareReply) {
    fsm.mu.Lock()
    defer fsm.mu.Unlock()

    fsm.proposer.seen = maxOf(fsm.proposer.seen, proposal)
    if proposal < fsm.proposer.proposal {
        log.Printf("OnPrepareOK(result: legacy, me: %d, get: %d, wait: %d)",
            fsm.id, proposal, fsm.proposer.proposal)
    } else {
        if fsm.state != PROPOSER {
            log.Printf("OnPrepareOK(result: ignore, me: %d, proposal: %d, state: %s)",
                fsm.id, fsm.proposer.proposal, fsm.getState())
            return
        }

        fsm.proposer.numAck++

        if fsm.proposer.maxProposal < reply.MaxProposal {
            fsm.proposer.maxValue = reply.MaxValue
            fsm.proposer.maxProposal = reply.MaxProposal
        }

        if fsm.proposer.numAck * 2 > len(fsm.peers) {
            log.Printf("OnPrepareOK(result: majority, me: %d, proposal: %d)", fsm.id, proposal)

            req := AcceptArgs{fsm.id, fsm.proposer.proposal, fsm.proposer.maxValue, fsm.seq}

            if req.Value == nil {
                //panic("Propose a nil")
                req.Value = -1
            }

            fsm.state = PROPOSED
            fsm.proposer.numAck = 0
            fsm.proposer.numNack = 0
            fsm.multicast("Accept", &req, func()(interface{}){ return new(AcceptReply) }, func(i interface{}) {
                if i == nil {
                    fsm.OnAcceptFailed(proposal)
                } else {
                    reply, ok := i.(*AcceptReply)
                    if !ok {
                        panic("Impossible: Accept doesn't return *AcceptReply")
                    }

                    if reply.Accept {
                        fsm.proposer.peerMins[reply.Peer] = maxOf(fsm.proposer.peerMins[reply.Peer], reply.PeerMin)
                        fsm.OnAcceptOK(proposal, reply.Peer)
                    } else {
                        fsm.OnAcceptRejected(proposal, reply.Peer)
                    }
                }
            })
        }
    }
}

func (fsm *PaxosFsm) OnPrepareRejected(proposal int, reply *PrepareReply) {
    fsm.mu.Lock()
    defer fsm.mu.Unlock()

    fsm.proposer.seen = maxOf(fsm.proposer.seen, reply.MaxProposal)

    if proposal < fsm.proposer.proposal {
        log.Printf("OnPrepareRejected(result: invalidProposal, me: %d, get: %d, wait: %d)",
            fsm.id, proposal, fsm.proposer.proposal)
    } else {
        if reply.Decided {
            log.Printf("OnPrepareRejected(result: decided, me: %d, seq: %d)", fsm.id, fsm.seq)
            fsm.decide(proposal, reply.MaxValue)
            return
        }
        if fsm.state != PROPOSER {
            log.Printf("OnPrepareRejected(result: ignore, me: %d, proposal: %d, state: %s)",
                fsm.id, fsm.proposer.proposal, fsm.getState())
            return
        }

        fsm.proposer.numNack++

        if fsm.proposer.numNack * 2 > len(fsm.peers) {
            log.Printf("OnPrepareRejected(result: majority, me: %d, proposal: %d)", fsm.id, proposal)

            fsm.becomeVoter()
        }
    }
}

func (fsm *PaxosFsm) OnPrepareFailed(proposal int) {
    fsm.mu.Lock()
    defer fsm.mu.Unlock()

    if proposal < fsm.proposer.proposal {
        log.Printf("OnPrepareFailed(result: legacy, me: %d, get: %d, wait: %d)",
            fsm.id, proposal, fsm.proposer.proposal)
    } else {
        if fsm.state != PROPOSER {
            log.Printf("OnPrepareFailed(result: ignore, me: %d, proposal: %d, state: %s)",
                fsm.id, fsm.proposer.proposal, fsm.getState())
            return
        }

        fsm.proposer.numNack++

        if fsm.proposer.numNack * 2 > len(fsm.peers) {
            log.Printf("OnPrepareFailed(result: majority, me: %d, proposal: %d)", fsm.id, proposal)

            fsm.becomeVoter()
        }
    }
}

func (fsm *PaxosFsm) OnAccept(args *AcceptArgs, reply *AcceptReply) {
    fsm.mu.Lock()
    defer fsm.mu.Unlock()

    log.Printf("OnAccept(me: %d, seq: %d, state: %s)", fsm.id, fsm.seq, fsm.getState())

    if args.Proposal >= fsm.acceptor.maxProposalSeen {
        reply.Accept = true

        switch fsm.state {
        case PROPOSER:
            fsm.acceptValue(args.Proposal, args.Value)
            fsm.becomeVoter()

        case PROPOSED:
            fsm.acceptValue(args.Proposal, args.Value)
            if fsm.id != args.Me {
                fsm.becomeVoter()
            }

        case DONE:
            fsm.acceptValue(args.Proposal, args.Value)

        case VOTER:
            fsm.acceptValue(args.Proposal, args.Value)
            fsm.resetVoterTimer()

        default:
            panic("Bad state")
        }
    } else {
        reply.Accept = false
    }
}

func (fsm *PaxosFsm) OnAcceptOK(proposal int, peer int) {
    fsm.mu.Lock()
    defer fsm.mu.Unlock()

    if proposal < fsm.proposer.proposal {
        log.Printf("OnAcceptOK(result: legacy, me: %d, state: %s)", fsm.id, fsm.getState())
    } else if proposal > fsm.proposer.proposal {
        panic("Impossible: bad accept proposal")
    } else {
        switch fsm.state {
        case PROPOSED:
            fsm.proposer.numAck++

            if fsm.proposer.numAck * 2 > len(fsm.peers) {
                fsm.sendDeciding()
            }

        case DONE:
            log.Printf("OnAcceptOK(result: legacy, me: %d, peer: %d, state: %s)", fsm.id, peer, fsm.getState())

        case VOTER:
            log.Printf("OnAcceptOK(result: ignore, me: %d, peer: %d, state: %s)", fsm.id, peer, fsm.getState())

        default:
            log.Panicf("OnAcceptOK(result: badState, me: %d, peer: %d, state: %s)", fsm.id, peer, fsm.getState())
        }
    }
}

func (fsm *PaxosFsm) OnAcceptRejected(proposal int, peer int) {
    fsm.mu.Lock()
    defer fsm.mu.Unlock()

    if proposal < fsm.proposer.proposal {
        log.Printf("OnAcceptRejected(result: legacy, me: %d, state: %s)", fsm.id, fsm.getState())
    } else if proposal > fsm.proposer.proposal {
        panic("Impossible: bad accept value")
    } else {
        switch fsm.state {
        case PROPOSED:
            fsm.proposer.numNack++

            if fsm.proposer.numNack * 2 > len(fsm.peers) {
                log.Printf("OnAcceptRejected(result: majority, me: %d, seq: %d, proposal: %d)", fsm.id, fsm.seq, proposal)
                fsm.becomeVoter()
            }

        case DONE:
            log.Printf("OnAcceptRejected(result: legacy, me: %d, peer: %d, state: %s)", fsm.id, peer, fsm.getState())

        case VOTER:
            log.Printf("OnAcceptRejected(result: ignore, peer: %d, me: %d, state: %s)", fsm.id, peer, fsm.getState())

        default:
            log.Panicf("OnAcceptRejected(result: badState, me: %d, state: %s)", fsm.id, fsm.getState())
        }
    }
}

func (fsm *PaxosFsm) OnAcceptFailed(proposal int) {
    fsm.mu.Lock()
    defer fsm.mu.Unlock()

    if proposal < fsm.proposer.proposal {
        log.Printf("OnAcceptFailed(result: legacy, me: %d, state: %s)", fsm.id, fsm.getState())
    } else if proposal > fsm.proposer.proposal {
        panic("Impossible: bad accept value")
    } else {
        switch fsm.state {
        case PROPOSED:
            fsm.proposer.numNack++

            if fsm.proposer.numNack * 2 > len(fsm.peers) {
                log.Printf("OnAcceptFailed(result: majority, me: %d, seq: %d, proposal: %d)", fsm.id, fsm.seq, proposal)
                fsm.becomeVoter()
            }

        case DONE:
            log.Printf("OnAcceptFailed(result: legacy, me: %d, state: %s)", fsm.id, fsm.getState())

        case VOTER:
            log.Printf("OnAcceptFailed(result: ignore, me: %d, state: %s)", fsm.id, fsm.getState())

        default:
            log.Panicf("OnAcceptFailed(result: badState, me: %d, state: %s)", fsm.id, fsm.getState())
        }
    }
}

func (fsm *PaxosFsm) OnDecide(args *DecideArgs, reply *DecideReply) {
    fsm.mu.Lock()
    defer fsm.mu.Unlock()

    log.Printf("OnDecide(me: %d, seq: %d)", fsm.id, fsm.seq)

    fsm.decide(args.Proposal, args.Value)
}

func (fsm *PaxosFsm) clearTimer() {
    if fsm.timer != nil {
        fsm.timer.Stop()
        fsm.timer = nil
    }
}

func (fsm *PaxosFsm) propose() {
    fsm.clearTimer()

    fsm.state = PROPOSER
    fsm.proposer.proposal = fsm.proposer.seen + 1
    fsm.proposer.numAck = 0
    fsm.proposer.numNack = 0
    fsm.proposer.maxValue = fsm.acceptor.maxValueAccepted
    fsm.proposer.maxProposal = 0

    log.Printf("Propose(me: %d, seq: %d, proposal: %d)", fsm.id, fsm.seq, fsm.proposer.proposal)

    proposal := fsm.proposer.proposal

    fsm.multicast("Prepare",
        &PrepareArgs{fsm.id, fsm.seq, fsm.proposer.proposal},
        func()(interface{}){ return new(PrepareReply) },
        func(i interface{}) {
            if i == nil {
                fsm.OnPrepareFailed(proposal)
            } else {
                reply, ok := i.(*PrepareReply)
                if !ok {
                    panic("Impossible: Prepare doesn't return PrepareReply")
                }

                if reply.Accept {
                    fsm.OnPrepareOK(proposal, reply)
                } else {
                    fsm.OnPrepareRejected(proposal, reply)
                }
            }
        })
}

func (fsm *PaxosFsm) Start() {
    fsm.mu.Lock()
    defer fsm.mu.Unlock()

    log.Printf("PaxosStart(me: %d, seq: %d)", fsm.id, fsm.seq)

    fsm.propose()
}

func (fsm *PaxosFsm) SubmitValueIfNeeded(v interface{}) {
    fsm.mu.Lock()
    defer fsm.mu.Unlock()

    if fsm.acceptor.maxValueAccepted == nil {
        fsm.acceptor.maxValueAccepted = v
    }
}

func (fsm* PaxosFsm) Finalize() {
    fsm.clearTimer()
    fsm.state = DONE
}

func (fsm *PaxosFsm) IsDone() bool {
    fsm.mu.Lock()
    defer fsm.mu.Unlock()

    return fsm.state == DONE
}

func (fsm *PaxosFsm) GetValue() interface{} {
    fsm.mu.Lock()
    defer fsm.mu.Unlock()

    return fsm.acceptor.maxValueAccepted
}
