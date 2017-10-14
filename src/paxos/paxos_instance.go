package paxos

import (
    "sync"
    "log"
    "math/rand"
    "time"
)

const (
    STOP = iota
    START
)

type Message struct {
    kind    int
    value   interface{}
}

type Proposer struct {
    me              int
    seq             int
    proposal        int
    maxProposalSeen int
    maxValueSeen    interface{}
    peers           []string
    mins            []int32
    pipe            chan Message
    done            bool
    acceptor        *Acceptor   // to pass the deaf peer test
}

type Acceptor struct {
    me              int
    proposal        int
    maxProposalSeen int
    maxValueSeen    interface{}
    minV            int32
    done            bool
    pipe            chan Message
    mu              sync.Mutex
}

func (p *Proposer) init(me int, peers []string, seq int) {
    p.me = me
    p.seq = seq
    p.maxProposalSeen = me * 10000
    p.maxValueSeen = nil
    p.peers = peers
    p.mins  = make([]int32, len(peers))
    p.pipe  = make(chan Message, 2)

    for i := range p.mins {
        p.mins[i] = -1
    }

    go p.run()
}

func (p *Proposer) multicast(method string, msg *PaxosMessage, majorityWanted MessageType) []*PaxosMessage {
    results := make([]*PaxosMessage, len(p.peers))
    pipe    := make(chan(*PaxosMessage), len(p.peers))

    for i, peer := range p.peers {
        go func(peerId int, peerName string) {
            reply := &PaxosMessage{}

            if call(peerName, "Paxos." + method, msg, reply) {
                pipe <- reply
            } else {
                log.Printf("MulticastFailed(me: %d, seq: %d, to: %d)", p.me, p.seq, peerId)
                pipe <- nil
            }
       }(i, peer)
    }

    ack := 0
    nack := 0
    for i := 0; i < len(p.peers); i++ {
        reply := <- pipe
        if reply != nil {
            results[reply.Sender] = reply

            p.maxProposalSeen = maxOf(p.maxProposalSeen, reply.Proposal)
            p.mins[reply.Sender] = maxOfInt32(p.mins[reply.Sender], reply.SenderMin)

            switch reply.Type {
            case DECIDE:
                return results

            case majorityWanted:
                ack++

            default:
                nack++
            }
        } else {
            nack++
        }

        if ack * 2 > len(p.peers) {
            break
        }
        if nack * 2 > len(p.peers) {
            break
        }
    }

    // FIXME better majority support
    return results
}

func (p *Proposer) prepare() (interface{}, bool) {
    prepareMsg := PaxosMessage{
        PREPARE,
        p.proposal,
        0,
        nil,
        p.seq,
        p.me,
        0,
        nil,
    }

    results := p.multicast("Prepare", &prepareMsg, PREPARE_OK)
    ack := 0
    maxProposal := 0
    var maxValue interface{} = nil
    for _, reply := range results {
        if reply == nil {
            continue
        }

        switch reply.Type {
        case DECIDE:
            p.decide(reply.MaxValue)

        case PREPARE_OK:
            ack++
            if reply.MaxProposal > maxProposal {
                maxValue = reply.MaxValue
            }
            if reply.MaxProposal > p.proposal {
                p.maxValueSeen = reply.MaxValue
            }

        case PREPARE_REJECT:
            // ignore

        default:
            log.Panicf("InPropose(result: unexpected, me: %d, seq: %d, msg: %s)",
                p.me, p.seq, reply.typeStr())
        }
    }

    if maxValue == nil {
        maxValue = p.maxValueSeen
    }
    if maxValue == nil {
        maxValue = 0
    }

    return maxValue, ack * 2 > len(p.peers)
}

func (p *Proposer) accept(v interface{}) bool {
    log.Printf("Accept(me: %d, seq: %d, proposal: %d)", p.me, p.seq, p.proposal)

    if v == nil {
        log.Panicf("SendNilToAccept(me: %d, seq: %d, proposal: %d)", p.me, p.seq, p.proposal)
        return false
    }

    msg := PaxosMessage{
        ACCEPT,
        p.proposal,
        0,
        v,
        p.seq,
        p.me,
        0,
        nil,
    }
    results := p.multicast("Accept", &msg, ACCEPT_OK)
    ack := 0
    for _, reply := range results {
        if reply == nil {
            continue
        }

        switch reply.Type {
        case DECIDE:
            p.decide(reply.MaxValue)

        case ACCEPT_OK:
            ack++

        case ACCEPT_REJECT:
            // ignore

        default:
            log.Panicf("InPropose(result: unexpected, me: %d, seq: %d, msg: %s)",
                p.me, p.seq, reply.typeStr())
        }
    }

    return ack * 2 > len(p.peers)
}

func (p *Proposer) decide(v interface{}) {
    log.Printf("Decide(me: %d, seq: %d, proposal: %d)", p.me, p.seq, p.proposal)

    msg := PaxosMessage{
        DECIDE,
        p.proposal,
        0,
        v,
        p.seq,
        p.me,
        0,
        p.mins,
    }
    p.done = true
    p.maxValueSeen = v
    // FIXME(workaround): to pass the deaf peer test
    p.acceptor.OnMessage(&msg, &PaxosMessage{})
    p.multicast("Decide", &msg, DECIDE)
}

func (p *Proposer) propose() {
    p.maxProposalSeen += 1
    p.proposal = p.maxProposalSeen

    log.Printf("Propose(me: %d, seq: %d, proposal: %d)", p.me, p.seq, p.proposal)
    v, ok := p.prepare()
    if p.done {
        return
    }
    if !ok {
        log.Printf("PrepareFailed(me: %d, seq: %d, proposal: %d)", p.me, p.seq, p.proposal)
        return
    }

    accepted := p.accept(v)
    if p.done {
        return
    }
    if !accepted {
        log.Printf("AcceptFailed(me: %d, seq: %d, proposal: %d)", p.me, p.seq, p.proposal)
        return
    }

    p.decide(v)
}

func (p *Proposer) run() {
    log.Printf("ProposerStart(me: %d, seq: %d)", p.me, p.seq)

    timeout := time.Duration(10 + rand.Intn(100)) * time.Millisecond

    for {
        select {
        case ev := <- p.pipe:
            if ev.kind == STOP {
                log.Printf("ProposerStop(me: %d, seq: %d)", p.me, p.seq)
                return
            } else {
                if p.maxValueSeen == nil {
                    p.maxValueSeen = ev.value
                }
                p.propose()
            }

        case <-time.After(timeout):
            if p.done {
                p.decide(p.maxValueSeen)
            } else {
                p.propose()
            }
        }
    }
}

func (a *Acceptor) init(me int, minV int32, pipe chan Message) {
    a.me = me
    a.proposal = 0
    a.maxProposalSeen = 0
    a.maxValueSeen = nil
    a.minV = minV
    a.pipe = pipe
}

type PaxosInstance struct {
    proposer        Proposer
    acceptor        Acceptor
}

func makeInstance(me int, peers []string, seq int, minV int32) *PaxosInstance {
    inst := &PaxosInstance{}

    inst.proposer.init(me, peers, seq)
    inst.acceptor.init(me, minV, inst.proposer.pipe)
    inst.proposer.acceptor = &inst.acceptor

    return inst
}

func (p *PaxosInstance) Start(value interface{}) {
    p.proposer.pipe <- Message{START, value}
}

func (p *PaxosInstance) Stop() {
    p.proposer.pipe <- Message{STOP, nil}
}

func (p *PaxosInstance) OnMessage(req *PaxosMessage, resp *PaxosMessage) {
    p.acceptor.OnMessage(req, resp)
}

func (p *PaxosInstance) GetValue() (interface{}, bool) {
    return p.acceptor.GetValue()
}

func (a *Acceptor) setReply(req *PaxosMessage, resp *PaxosMessage, result MessageType) {
    resp.Seq = req.Seq
    resp.Proposal = a.proposal
    resp.MaxValue = a.maxValueSeen
    resp.MaxProposal = a.maxProposalSeen
    resp.Sender = a.me
    resp.SenderMin = a.minV
    resp.Type = result
}

func (a *Acceptor) OnMessage(req *PaxosMessage, resp *PaxosMessage) {
    a.mu.Lock()
    defer a.mu.Unlock()

    log.Printf("OnMessage(me: %d, from: %d, seq: %d, msg: %s)", a.me, req.Sender, req.Seq, req.typeStr())

    if a.done {
        if req.Type == DECIDE {
            log.Printf("< OnMessage(me: %d, from: %d, seq: %d, msg: %s, result: ignore)",
                a.me, req.Sender, req.Seq, req.typeStr())
        } else {
            log.Printf("< OnMessage(me: %d, from: %d, seq: %d, msg: %s, result: alreadyDone)",
                a.me, req.Sender, req.Seq, req.typeStr())

            a.setReply(req, resp, DECIDE)
        }
    } else {
        switch req.Type {
        case PREPARE:
            if req.Proposal > a.proposal {
                log.Printf("PrepareOK(me: %d, from: %d, seq: %d, acceptP: %d, reqP: %d)",
                    a.me, req.Sender, req.Seq, a.proposal, req.Proposal)
                a.proposal = req.Proposal
                a.setReply(req, resp, PREPARE_OK)
            } else {
                log.Printf("PrepareReject(me: %d, from: %d, seq: %d, acceptP: %d, reqP: %d)",
                    a.me, req.Sender, req.Seq, a.proposal, req.Proposal)
                a.setReply(req, resp, PREPARE_REJECT)
            }

        case ACCEPT:
            if req.Proposal >= a.proposal {
                log.Printf("AcceptOK(me: %d, from: %d, seq: %d, acceptP: %d, reqP: %d)",
                    a.me, req.Sender, req.Seq, a.proposal, req.Proposal)

                if req.MaxValue == nil {
                     log.Panicf("ProposeNil(me: %d, from: %d, seq: %d, acceptP: %d, reqP: %d)",
                         a.me, req.Sender, req.Seq, a.proposal, req.Proposal)
                     return
                }

                a.proposal = req.Proposal
                a.maxProposalSeen = req.Proposal
                a.maxValueSeen = req.MaxValue

                a.setReply(req, resp, ACCEPT_OK)
            } else {
                log.Printf("AcceptReject(me: %d, from: %d, seq: %d, acceptP: %d, reqP: %d)",
                    a.me, req.Sender, req.Seq, a.proposal, req.Proposal)

                a.setReply(req, resp, ACCEPT_REJECT)
            }

        case DECIDE:
            log.Printf("Decide(me: %d, from: %d, seq: %d, acceptP: %d, reqP: %d)",
                    a.me, req.Sender, req.Seq, a.proposal, req.Proposal)
            a.done = true
            a.maxValueSeen = req.MaxValue
            a.maxProposalSeen = req.MaxProposal
            a.pipe <- Message{STOP, nil}

        default:
            log.Panicf("OnMessage(me: %d, from: %d, seq: %d, msg: %s, result: badMsg)",
                a.me, req.Sender, req.Seq, req.typeStr())
        }
    }
}

func (a *Acceptor) GetValue() (interface{}, bool) {
    a.mu.Lock()
    defer a.mu.Unlock()

    if a.done {
        return a.maxValueSeen, true
    } else {
        return nil, false
    }
}
