# ViewStamped Replication
基于ViewService的Primary/Backup Replication是一种ViewStamped Replication，这里，将讨论一些模型上的细节。

# 全序多播
Replication协议的正确性依赖于全序多播。考虑ViewStamped Replication是如何支持全序多播的。

将Primary/Backup看作RSM。支持的命令有Get/Put/Sync/ViewChange，考虑这些命令（源于于并发的Client与ViewService）是如何全序应用在所有状态机上的。

## 单View内
如果所有命令都发生在一个View以内，则命令由Primary定序，并复制到所有Backup。有依赖以下两点：

+ View建立时，Primary/Backup的状态应该相同
+ Primary，需要与所有其他节点通信，从而能够定义一个所有节点都接收的顺序

## 跨View
考虑Client1给Primary发送了一条Put命令，ViewService发送了一个ViewChange事件。如果保证两者在所有副本上都严格有序？

+ 如果Primary在进行Put Replication时，没有节点收到ViewChange，则此Put正常进行，正确同步到Backup。ViewChange发生在其后，其全序为Put/ViewChange/Sync
+ 如果Client发送Put时，有节点收到了ViewChange事件
    + 如果Primary收到了ViewChange，并且它依然是Primary，则Primary会发送Sync事件到所有Backup，从而新的View建立，然后Put被执行，其全序为ViewChange/Sync/Put
    + 如果Primary收到了ViewChange，但是它不再是Primary，则Put会被拒绝，Client重试，直到发现了新的Primary，而新的Primary此时必然已经执行了Sync，故能够执行Put，其全序为ViewChange/Sync/Put
    + 如果Backup收到了ViewChange，则Primary无法执行Put。Client重试，Put会被发向新的Primary，且其必然已经收到ViewChange/Sync。于是全序为ViewChange/Sync/Put
    
上面有两条假设，即，在Primary上，
+ ViewChange与Sync是原子执行的。故节点收到ViewChange并且变成Primary后，立即执行Sync，将自身状态同步到Backup，同步成功才Ack，表示View建立完成。
+ ViewChange与Sync执行过程中，不会再有新的ViewChange，即ViewService所提供的Primary Ack Rule。这里，潜在的问题是Sync可能会占用大量时间。

# 状态机同步
一般情况下，PB状态机是处于同一View的，此时，P可以正常发起同步。但是，在一个分布式系统中，所有状态机并不总是同步的，即考虑lagging状态机问题

+ lagging状态机的判断：当Backup收到一条Put命令，它可能来自旧的Primary，我们使用ViewNum充当逻辑时间，从而判断先后
+ 当Primary向Backup同步，但Backup此时还是Idle，即它还没有收到ViewChange。我们可以让Primary等待后重试。但为了让状态机尽快达到一致，避免不必要的通信开销，可以让Primary直接把最新的View同步给Backup

# CAP/CAL
## Primary/Backup同步时的Network Partition或者Latency
考虑Primary执行Put指令时，需要向所有Backup同步，如果此同步过程很慢(Latency)或者失败了(Network Partition)，则，操作永远无法成功。故，只有两种选择:

+ Blocking，直到操作返回
+ ViewChange，然后在下一个View中(不包括之前慢的节点或者失效的节点)中响应Put请求

## Split Brain：双主问题
+ 预防：ViewService的Primary Ack Rule，保证不会同时选举出两个Primary
+ 解决：如果一个Primary因为Network partition而变成旧的Primary，新的Primary选举出来后，某个client仍然向旧的Primary发送命令。根据上面的全序多播规则，旧的Primary不会再响应任务请求。

# At-most-once RPC
实际上是Exactly-once：at-lease-once(客户端重试) + 服务器端去重(idempotent) = exactly-once。为了实现此语义，我们需要在应用层去重，而不是RPC层去重。由于Get/Put/Sync都是幂等的，所以不需要处理。而Append需要去重。

考虑以下几点：

+ 应用层去重：由于是在应用层去重，所以Sync时，要同步相关状态
+ 原子性：Append的提交与去重必须是原子的，否则，语义要么是at-lease-once，要么是at-most-once，取决于两者的相对顺序。在进程fail-stop模型下，考虑到只在执行前与返回响应前会失效，因此，只要保证RPC开始执行到开始返回中间是原子的即可。显然这是可以办到的。

# 问题
+ Primary Ack Rule带来的问题
+ Primary必须执行完成Sync后，才能Ack，如果Sync时间很长，会有问题