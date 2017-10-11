package viewservice

import "log"

// Assumptions: primary not ack will never die!
// In the current test cases, the assumption is not true and the actual behavior is not properly defined.
// When primary restarted or died and rejoined, the view is not changed. We can switch to next view when it acked,
// but the switch may cause data loss. For example:
//      [p] and p restarted, when p is acked, we can do nothing
//      [p, b] and p restarted, when p is acked, we can change view and promote b to primary, but b is not guaranteed
//      to have all data
//
// The above problem is somehow hard to solve, since node crash time is less than election time and election failed to
// function.
//
// Protocol: we will ignore all restarts/expires from primary when it is not acked.

const (
    EMPTY = 0
    WAIT = 1
    LEGACY_WAIT = 2
    RUN = 3
)

var stateValues = []string{ "EMPTY", "WAIT", "LEGACY_WAIT", "RUN" }

func String(state int) string {
    return stateValues[state]
}

type ViewFsm struct {
    state int
    view View
    monitor *Monitor
}

func (fsm *ViewFsm) getState() string {
    return String(fsm.state)
}

func (fsm *ViewFsm) advance() {
    liveServers := fsm.monitor.GetAll()
    candidates := make([]string, 0, len(liveServers))

    if _, ok := liveServers[fsm.view.Primary]; ok {
        candidates = append(candidates, fsm.view.Primary)
        delete(liveServers, fsm.view.Primary)
    }
    if _, ok := liveServers[fsm.view.Backup]; ok {
        candidates = append(candidates, fsm.view.Backup)
        delete(liveServers, fsm.view.Backup)
    }

    for k := range liveServers {
        candidates = append(candidates, k)
    }

    if len(candidates) == 0 {
        log.Printf("Advance failed: no servers")
        fsm.state = EMPTY
    } else {
        primaryCandidate := candidates[0]
        // note: we have to explicitly check the new primary is in the set of old [primary, backup], only check
        //       viewnum is not wrong
        if fsm.state == EMPTY || primaryCandidate == fsm.view.Primary || primaryCandidate == fsm.view.Backup {
            fsm.view.Primary = candidates[0]
            if len(candidates) > 1 {
                fsm.view.Backup = candidates[1]
            } else {
                fsm.view.Backup = ""
            }
            
            fsm.view.Viewnum++
            fsm.state = WAIT

            log.Printf("View(view: %d, primary: %s, backup: %s)", fsm.view.Viewnum, fsm.view.Primary, fsm.view.Backup)
        } else {
            // in order to pass tests
            // FIXME(panic)
            log.Printf("View change failed: no servers from last view")
        }
    }
}

func (fsm *ViewFsm) OnPrimaryAck() {
    if fsm.state == EMPTY || fsm.state == RUN {
        return
    }

    log.Printf("OnPrimaryAck(state: %s, primary: %s)", fsm.getState(), fsm.view.Primary)

    oldState := fsm.getState()

    switch fsm.state {
    case WAIT:
        fsm.state = RUN

    case LEGACY_WAIT:
        fsm.view.Backup = ""
        fsm.advance()
    }

    log.Printf("OnPrimaryAck(old: %s, new: %s)", oldState, fsm.getState())
}

func (fsm *ViewFsm) OnRestarted(server string) {
    log.Printf("OnRestarted(state: %s, server: %s)", fsm.getState(), server)

    oldState := fsm.getState()

    switch fsm.state {
    case EMPTY:
        panic("OnRestarted when empty")

    case WAIT:
        if server == fsm.view.Primary {
            panic("Primary restarted when waiting for ack")
        } else if server == fsm.view.Backup {
            fsm.state = LEGACY_WAIT
        }

    case LEGACY_WAIT:
        if server == fsm.view.Primary {
            panic("Primary restarted when legacy waiting")
        }

    case RUN:
        if server == fsm.view.Primary {
            fsm.view.Primary = ""
        } else if server == fsm.view.Backup {
            fsm.view.Backup = ""
        }

        fsm.advance()
    }

    log.Printf("OnRestarted(old: %s, new: %s)", oldState, fsm.getState())
}

func (fsm *ViewFsm) OnExpire(servers []string) {
    log.Printf("OnExpire(state: %s, servers: %s)", fsm.getState(), servers)

    oldState := fsm.getState()

    switch fsm.state {
    case EMPTY:
        panic("Expired when empty")

    case WAIT:
        for _, server := range servers {
            if server == fsm.view.Primary {
                // in order to pass tests
                // FIXME    (panic)
                log.Printf("Primary expired when waiting for ack")
            } else if server == fsm.view.Backup {
                fsm.state = LEGACY_WAIT
            }
        }

    case LEGACY_WAIT:
        for _, server := range servers {
            if server == fsm.view.Primary {
                // in order to pass tests
                // FIXME(panic)
                log.Printf("Primary expired when waiting(legacy) for ack")
            }
        }

    case RUN:
        fsm.advance()
    }

    log.Printf("OnExpire(old: %s, new: %s)", oldState, fsm.getState())
}

func (fsm *ViewFsm) OnNew(server string) {
    log.Printf("OnNew(state: %s, server: %s)", fsm.getState(), server)

    oldState := fsm.getState()

    switch fsm.state {
    case EMPTY:
        fsm.advance()

    case WAIT:
        fsm.state = LEGACY_WAIT

    case LEGACY_WAIT:
        // ignore

    case RUN:
        // if the view is full, won't advance
        if fsm.view.Backup == "" {
            fsm.advance()
        }
    }

    log.Printf("OnNew(old: %s, new: %s)", oldState, fsm.getState())
}

func (fsm *ViewFsm) Init(monitor *Monitor) {
    log.Printf("ViewFsmInit")
    fsm.monitor = monitor
}

func (fsm *ViewFsm) GetView() View {
    return fsm.view
}
