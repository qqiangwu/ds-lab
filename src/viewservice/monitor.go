package viewservice

import (
    "time"
    "log"
)

type ServerInfo struct {
    viewNum        uint
    lastPing     time.Time
}

type Monitor struct {
    servers     map[string]ServerInfo
    expired     []string
}

func (monitor *Monitor) Init() {
    log.Printf("MonitorInit")
    monitor.servers = make(map[string]ServerInfo)
}

func (monitor *Monitor) Tick() {
    monitor.expired = nil
    now := time.Now()

    for k, v := range monitor.servers {
        if now.Sub(v.lastPing) >= DeadPings * PingInterval {
            monitor.expired = append(monitor.expired, k)
        }
    }

    for _, v := range monitor.expired {
        delete(monitor.servers, v)
    }
}

func (monitor *Monitor) OnPing(server string, viewNum uint) {
    monitor.servers[server] = ServerInfo { viewNum, time.Now() }
}

func (monitor *Monitor) GetExpired() []string {
    return monitor.expired
}

func (monitor *Monitor) GetAll() map[string]uint {
    ret := make(map[string]uint)

    for k, v := range monitor.servers {
        ret[k] = v.viewNum
    }

    return ret
}

func (monitor *Monitor) Get(server string) (uint, bool) {
    v, ok := monitor.servers[server]

    if ok {
        return v.viewNum, true
    } else {
        return 0, false
    }
}
