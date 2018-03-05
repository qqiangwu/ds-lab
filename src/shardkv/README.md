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
