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
