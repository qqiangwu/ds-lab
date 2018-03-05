package shardkv

import "log"

var Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func assert(expr bool, msg string) {
    if !expr {
        panic(msg)
    }
}

func getOrDefault(m map[int64]int, k int64, defaultV int) int {
    v, ok := m[k]
    if ok {
        return v
    } else {
        return defaultV
    }
}

func max(a int, b int) int {
    if a > b {
        return a
    } else {
        return b
    }
}
