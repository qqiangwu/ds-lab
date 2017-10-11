# ViewService
本Lab中的Primary/Backup KV使用ViewService来提供选举机制，这与ViewStamped Replication协议类似。这里，仅仅考虑ViewService本身的设计与实现。

# 设计
核心代码在`viewfsm.go`与`monitor.go`中，前者提供View管理的状态机，后者提供Failure detector与成员管理功能。

# 问题
ViewService实现比较简单，因为没有涉及太多的分布式的内容。然后，同步协议本身有一个很大的问题，即：当切换到一个新的View时，如果Primary无法ack，则ViewService无法前进。从ViewService本身来讲，本不需要如此，然而，在实现Primary/Backup KV时，其正确性依赖于此语意。其原因说明如下：

+ View重叠：进入一个新的View时，Primary保证是上一个View的成员，因此，上一个View中ack过的数据，Primary中都有，故数据不会丢。如果允许从一个未ack的View进入一个新的View，则新的View中的Primary可能没有所有数据。
+ View同步：进入一个新的View时，Primary需要执行恢复逻辑，保证当前View中的所有成员状态达到一致。

## 合理性
考虑这种行为的合理性：

一般的，对于一个复制组，我们应该有以下假设：`选举时间 < 节点失效时间`。如果此条件不满足，每当一个新的View还没有完全建立起来时，即需要过渡到下一个View，则系统必然不连续，也即，必然会有数据丢失。

## 协议定义
由于Primary Ack Rule的存在，ViewService的协议定义有些不那么明确。现在，在Primary Ack Rule的基础上，不考虑ViewService的使用者，定义协议如下：

+ 新的View建立且Primary未ack时
    + 如果Primary停止，则永远等待下去，直到其启动后重新加入
    + 如果Primary重启，忽略之，View从WAIT状态进行RUN状态
+ 如果View中的唯一节点失效，则没有新的节点，则View进行EMPTY状态