package raft

import "log"

// Debugging
var Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func tail(log []LogEntry) LogEntry {
    return log[len(log) - 1]
}

func min(a int, b int) int {
    if a < b {
        return a
    } else {
        return b
    }
}

func majority(rf *Raft) int {
    return len(rf.peers) / 2 + 1
}

func assert(expr bool, msg string) {
    if !expr {
        panic(msg)
    }
}
