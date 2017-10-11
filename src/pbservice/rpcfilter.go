package pbservice

type RPCFilter struct {
    filters  map[string]int64
}

func (filter *RPCFilter) Init() {
    filter.filters = make(map[string]int64)
}

func (filter *RPCFilter) Start(source string, rpcId int64) bool {
    id, exist := filter.filters[source]

    if !exist {
        return true
    } else {
        return id != rpcId
    }
}

func (filter *RPCFilter) Commit(source string, rpcId int64) {
    filter.filters[source] = rpcId
}

func (filter *RPCFilter) Dump() map[string]int64 {
    return filter.filters
}

func (filter *RPCFilter) Load(filters map[string]int64) {
    filter.filters = filters
}
