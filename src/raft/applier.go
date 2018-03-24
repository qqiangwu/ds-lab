package raft

import "container/list"
import "sync"
import "sync/atomic"

type Applier struct {
    mu         sync.Mutex
    notEmpty   *sync.Cond

    ch      chan ApplyMsg
    stopped int32

    list    *list.List
}

func makeApplier(ch chan ApplyMsg) *Applier {
    ap := &Applier{ch: ch}

    ap.list = list.New()
    ap.notEmpty = sync.NewCond(&ap.mu)

    go ap.run()

    return ap
}

func (ap *Applier) Stop() {
    atomic.StoreInt32(&ap.stopped, 1)
    ap.notEmpty.Signal()
}

func (ap *Applier) IsRunning() bool {
    return atomic.LoadInt32(&ap.stopped) == 0
}

func (ap *Applier) run() {
    for ap.IsRunning() {
        msg, stopped := ap.popFront()
        if stopped {
            break
        }

        ap.ch <- msg
    }

    close(ap.ch)
}

func (ap *Applier) popFront() (ApplyMsg, bool) {
    ap.mu.Lock()
    defer ap.mu.Unlock()

    for ap.IsRunning() {
        e := ap.list.Front()

        if e != nil {
            ap.list.Remove(e)
            return e.Value.(ApplyMsg), false
        }

        ap.notEmpty.Wait()
    }

    return ApplyMsg{}, true
}

func (ap *Applier) Push(msg ApplyMsg) {
    ap.mu.Lock()
    defer ap.mu.Unlock()

    ap.list.PushBack(msg)
    ap.notEmpty.Signal()
}
