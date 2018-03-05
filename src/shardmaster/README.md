# 设计
+ key被切分成shard，shard被分布到replica group上
+ replica group是一个raft组，提供高可用
+ master管理所有replica groups

# 挑战
+ shard在replica group之间进行迁移
    + 两个组达成一致，shard是谁负责的
    + 单个组内，所有成员达成一致
    + 迁移过程中：新的操作怎么办？
    + 如何进行数据的迁移，replica group之间是否需要互相通信？
+ 如何切分shard
+ 如何负责均衡？
+ RPC还是要去重：为啥每个地方都要解决这个问题？

# Master
+ configuration有版本号，每次变更版本中与加一
+ 利用kvraft的实现: 但是这样，序列化所有操作的成本太高了，因为临界区的时间太长了
    + kvraft是基本抽象，因为将它弄得尽量高效，而不是所有东西都从raft重新构建
    + 重用实现的话，那么master所有副本都可以响应请求?
    + 实现一个master调用，需要调用好几个kv调用，如果调用中，leadership改变了，怎么办？
        + 并不能，因为master一次调用需要多次kv调用，因为，这几次kv调用必须是原子的。
    + 直接调用kvraft，去重怎么做？
        + 要么复用master收到的seq来去重，要么，为kvraft添加不需要去重的接口，然后master调用它来实现去重版本
    + 写完了，一测试出错误了，因为getConfig的初始情况下，kv里面没有东西，因此，需要初始化
