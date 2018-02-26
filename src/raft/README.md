candidate timeout后应该增加term，否则，如果分散的followers永远会投分散的票

一个term里面，follower/candidate只能给一个人投票

收到AppendEntries时，如果其term至少为currentTerm，则可以接收


避免split votes：随机化election timeout

保证选举出来的leader包含已经ack过的entry(通过vote来实现)。其他一致性协议，比如vs，不要求这一点，但是它需要额外的机制来实现这一点(即由follower将最新的日志传递给leader)。raft实现这一特性后，日志只会单向流动，简化了整个流程和状态空间。
update-to-date定义
+ term大的赢
+ term相同，index大的赢，就是比谁的长。注意，raft中的log，index从1开始，0用于表示一个初始值

replication时的优化
+ batch简单
+ pipeline呢？简单的方法是，第一个append还没有返回时，不会发送第二个，但是这样延迟会很大，所以pipeline是最好的方法

使用锁定序与使用队列定序的区别以及性能

# timeout问题
+ election timeout: follower and candidate wait for
+ heartbeat interval << election timeout
+ 现在的问题是，candidate总是timeout，然后两个candidate不停地相互timeout，造成活锁
+ 由于通信会延迟，而每个通信占据一个goroutine，那么，如何回收呢？每次timeout都会发起新的election，造成新的通信
+ 把RequestVote与AppendEntries的发送全部并行化就OK了
+ Follower.appendEntries时，即使entries为空，也要检查leaderCommit
+ Disconnected Leader rejoin一个leader死去的集群，a,b两者log冲突，从而无法
    + `[{0 0 <nil>} {1 1 101} {1 2 102} {1 3 103} {1 4 104} {1 5 104}]`
    + `[{0 0 <nil>} {1 1 101} {2 2 103}]`
+ Prefix match的情况下，才能使用leaderCommit更新自己
+ 测试中有一个不恰当的例子，1，2离线很久，3，4，5进度领先，但是3，4忽然无法联系，1，2，5在线，此时，谁能当leader？
    + 从协议上来看，我的实现是对的，5会成为Leader，但是由于1,2,5从index=2处就相差，所有收敛需要很长时间
    + 手工添加了一堆sleep，能够正常收敛了
    + 最开始的实现是，每100ms，doSend，现在看来，不同follower速率不一样，这样做没有意思，每个follower一个线程好了
