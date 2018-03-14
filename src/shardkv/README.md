# 问题
+ master的config改变时，涉及的replica如何完成迁移工作
    + 先假设迁移过程中副本不可用
    + 如果迁移过程中副本可以支持并发访问呢？
+ 错误模型
    + replica在raft正常时可以正常工作
    + replica在master正常时可以正常工作
+ client如何进行路由
    + 缓存
    + 当访问失败时，从master更新路由
+ server如何检测config改变
    + watch
    + 由于server是高可用的，所以可以维护当的迁移状态(raft)
    + 迁移过程中，config发生变化怎么处理？
    + client检测到了，但是server还没有检查到怎么办？=> 虚拟同步
+ 如何处理并发的config变更？
+ 在处理Get时，需要进行一系列操作，而操作中的每一步都有可能造成leadership的改变，此时，没有异常会让情况变得有些麻烦

# 实现
+ 先不考虑config的变化，透明实现kvraft
+ lazy更新config
    + 定时器更新
    + 与client请求进行比较
+ 理论上，每个shard一个raft比较好，但是测试中只提供了一个persister，所以只能共享raft
+ 迁移实现
    + 迁移过程中，应该禁止写
    + 对于迁出者，禁写，记录状态，进行迁移（万了出错怎么办）
    + 对于迁入者，禁写，直接复制完成（状态也需要记录，万一出错怎么办）
    + 为了简单，暂时禁止并发变量，旧的变更未完成前，新的config永远不会被应用
+ 实现中发现，config也需要持久化，因为kv的处理依赖于config，config发生变化时，会触发kv的状态变化(再思考一下)
    + 把kvraft当作一个持久化的东西来处理，不要在意其leadershp改变的细节
+ req处理时：leadership不能改变，否则，abort；使用multiput来实现tx
+ req处理时：configNum发生变化怎么办？即，处理时刚刚更新了一个版本，处理到一半，又出现一个新的版本
    + 可以忽略，因为，此时，cient与kv处于同一个同步段内，因此，故不需要考虑
+ 更新config时，要保证read-and-write是原子的，否则，可能会覆盖（ABA问题）？即，需要CAS支持
    + 为kvraft添加CAS支持
    + 如果CAS失败，可能是因为leader丢了，也可能是因为leadership ABA了，但其他又有其他配置被写入
+ send shard的实现
    + 启动时，send：恢复旧的send
    + 变更时，send：send完毕时，提交事务；如果异常终止，会在启动时恢复事务
+ leadership change通知
    + leadership应该在内存维护一阵子 ，这样可以维护一些软状态，否则所有东西总是要到kv上读
    + 而且，直接在kv上构建更加高级应用并不是一个好主意，因为高级应用同样需要知道leader的存在，这会使得实现复杂许多
+ 基于短路错误处理的方法，看起来越来越像异常了
+ fsm层必须要知道raft层term的变化，否则有些功能无法完成，如shardkv重启时，需要启动未完成的迁移，但是，由于它不知道leader什么时候选举出来，因此会有问题。目前先定时吧。
+ 写着写着我自己都觉得控制不了自己的代码了，明明才五百行啊。
+ 在shard收到pack之后，如果它的term落后，且旧的term中有些pack还没有收到，怎么办？
    + 暂时缓存消息，之后再处理。但是这需要更新复杂的协调机制，且leadership随时会改变
    + 返回EAGAIN，让发送端稍后重试
+ 越写越复杂，shard收到pack之后，需要走状态机，不然没有办法进行复制
    + 推倒重新来，这次直接在raft上构建shardkv
