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
        + 这样太浪费线程资源了吧


# 2C Persistence
## 需要persiste的状态
log/voteFor/currentTerm，数据每次更改，都需要持久化。其中，currentTerm可以包含在logEntry中，一次append。voteFor需要单独的append。不对，currentTerm与log并不一致，也需要单独的append

candidate reboot不影响系统正确性，leader失效恢复后成为follower，同样不影响。follower则不行，它reboot前后，如果选举正在进行中，则reboot后，可能给另外一个人投票，造成一个人投了两次不一样的票

需要注意的是，voteFor一定是最后一个entry，当有新的log后的，或者term改变，则voteFor过时了

## 何时读取持久化信息
reboot时恢复

## 问题
对三个变量的更新遍布在各处，如何最化小开销？

# 不可靠网络
之前的实现出了问题，因为，投票时，没有考虑网络的不可靠性，所以，candidate只发出一次投票，但在大部分情况下，由于网络不可靠，回复都丢了。

因此，为每一个follower起一个goroutine。

改了之后还是错。

进一步思考：添加更多的日志，以从日志中看出网络丢包事件。

又改了一些东西，当doSend时，如果validation失败，则立即重试

想了一下，还是要启动不匹配时的优化，从而减少RPC数量，否则，恶劣网络条件下，花费的时间太长。原有实现中，不匹配时，回退一个index，更改的实现是，回退整个term。Follower返回冲突点的term及此term的第一个index，从而给candidate更多的信息。从而，一个RPC就可以解决一个Term的冲突。

论文中没有采取此优化，因为，它假设网络不会那么差；然而在此Lab中，网络的确很差。

同时，group commit时，没有限制entries的大小，实际工作中肯定要限制。

考虑几种情况：

+ conflict: return follower.term + first index in the term
+ f.index < c.index: return f.term + f.max-index

然后又发现其他的错误，old leader reconnect后，checkOneLeader成功的，但disconntect其他之后，old leader还是leader。说明，new leader的心跳还没有发给old。于是，我在old leader append检测到新term时，自动退化

然而并没有用，还是出问题，于是乎只好把心跳间隔设置成更小。

测试2B时，又出现新的问题，一个assert不满足！一定要测试Assert!

添加了不匹配时的回退优化，但还是有问题，因此，提高follower超时timeout，给leader更多时间去同步。
