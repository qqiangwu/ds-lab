package raft

import "encoding/gob"
import "fmt"

type LogEntry struct {
    Term        int
    Index       int
    Command     interface{}
}

// need external sync
type Log struct {
    Log       []LogEntry
    LastIndex int           // last index of snapshot
    LastTerm  int           // last term of snapshot
}

func (log *Log) hasNewLog() bool {
    return len(log.Log) != 0
}

func (log *Log) NextIndex() int {
    if log.hasNewLog() {
        return log.Tail().Index + 1
    } else {
        return log.LastIndex + 1
    }
}

func (log *Log) AppendEntry(e LogEntry) {
    assert(e.Index == log.NextIndex(), "Invalid index of log")
    log.Log = append(log.Log, e)
}

func (log *Log) AppendRange(r []LogEntry) {
    if len(r) != 0 {
        assert(r[0].Index == log.NextIndex(), "Invalid index of log")
    }
    log.Log = append(log.Log, r...)
}

// @pre index is in pending log
func (log *Log) Get(index int) LogEntry {
    for _, entry := range log.Log {
        if entry.Index == index {
            return entry
        }
    }

    panic("Out of range @ Get")
}

func (log *Log) GetTerm(index int) int {
    if index == log.LastIndex {
        return log.LastTerm
    }

    return log.Get(index).Term
}

// @pre indexFrom is in pending log
// [indexFrom:]
func (log *Log) GetFrom(indexFrom int) []LogEntry {
    if log.InSnapshot(indexFrom) {
        panic("Get log of snapshot")
    } else if indexFrom > log.NextIndex() {
        panic("Index too large @ GetFrom")
    }

    for i, e := range log.Log {
        if e.Index == indexFrom {
            return log.Log[i:]
        }
    }

    return nil
}


// @pre indexFrom is in pending log
// remove [index: ]
func (log *Log) RemoveFrom(index int) {
    if log.InSnapshot(index) {
        panic("InSnapshot @ RemoveFrom")
    }

    for i, e := range log.Log {
        if e.Index == index {
            log.Log = log.Log[:i]
            return
        }
    }
}

// remove [:index]
func (log *Log) Snapshot(index int, term int) {
    if index < log.LastIndex {
        panic(fmt.Sprintf("Index already snapshot(index: %v, lastIndex: %v)\n", index, log.LastIndex))
    }
    if index == log.LastIndex {
        return
    }

    ll := log.Log
    log.Log = nil

    for i, e := range ll {
        if e.Index == index {
            log.Log = ll[i+1:]
        }
    }

    log.LastIndex = index
    log.LastTerm = term
}

func (log *Log) Head() LogEntry {
    assert(len(log.Log) != 0, "empty log")
    return log.Log[0]
}

func (log *Log) Tail() LogEntry {
    logLen := len(log.Log)

    assert(logLen != 0, "empty log")

    return log.Log[logLen - 1]
}

func (log *Log) TailMeta() (index int, term int) {
    if !log.hasNewLog() {
        return log.LastIndex, log.LastTerm
    } else {
        last := log.Tail()
        return last.Index, last.Term
    }
}

// @pre len(trailer) != 0
// @pre trailer > snapshot
func (log *Log) FindFirstNonMatch(trailer []LogEntry) (logIndex int, trailerArrayIndex int) {
    assert(len(trailer) != 0, "trailer is empty")
    assert(trailer[0].Index > log.LastIndex, "Trailer in snapshot")

    if !log.hasNewLog() {
        return log.NextIndex(), 0
    } else {
        tail := log.Tail()

        for i, v := range trailer {
            if existInLog := v.Index <= tail.Index; !existInLog {
                return v.Index, i
            }
            if match := v.Term == log.Get(v.Index).Term; !match {
                return v.Index, i
            }
        }

        return trailer[len(trailer) - 1].Index + 1, len(trailer)
    }
}

// @pre index is not in snapshot
func (log *Log) FindTermFirstIndex(index int) int {
    assert(!log.InSnapshot(index), "InSnapshot")

    cterm := log.LastTerm
    cindex := log.LastIndex + 1

    for _, e := range log.Log {
        if e.Index > index {
            break
        }

        if e.Term != cterm {
            cterm = e.Term
            cindex = e.Index
        }
    }

    return cindex
}

func (log *Log) InSnapshot(index int) bool {
    return index <= log.LastIndex
}

func init() {
    gob.Register(Log{})
}
