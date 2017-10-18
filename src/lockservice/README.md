# LockService
这是第一个Lab，实现非常简单，但是也模型假设也十分严格，因此，应用程度十分有限

# 错误模型
+ client：不会出错
+ servers：primary或者backup只有一个会fail，且方式为fail-stop
+ network: reliable，无Network Partition

# 错误处理
当primary节点失效时，正在工作的client会超时，之后，它会向backup进行重试，通过retry来屏蔽节点失效。这在实现上很简单，但是引入了重复的RPC调用问题。

解决方法是应用层去重, 或者使用幂等操作。显然，这里只能使用应用层去重，即，在应用层保存所有RPC的结果。那么问题来了，这些用于去重的信息，何时可以回收？

+ 无错误时：单clerk的所有请求总是FIFO，即，前一个请求没有结果，不会issue后一个请求。
+ 有错误时：失败的那个请求会被重试。现实中，它可以执行了，也可以没有执行；本实验中，它总是被执行了。因此，它后后面的请求也具有FIFO关系。

综上，每一个clerk用O(1)空间来去重就足够了。

那么问题又来了，如果clerk会失效，或者会有新的加入，旧的退出，如何回收？实践中，用得更多的是幂等或者定期清除。

+ 幂等：显然，不需要额外的去重逻辑，操作进行多次效果相同
+ 定期清除：基于这样一条假设，如果T时间没有响应，说明对象已经死亡。

# 其他问题
测试代码中有一个问题，即，假设Primary失效时，会正在执行一个RPC（Primary处于dying，clerk1 lock a），但此时，其他clerk已经联系不上它了，如果clerk2 在Backup上lock a，而来自Primary同步的lock a又过来，会冲突。2 在Backup上lock a，而来自Primary同步的lock a又过来，会冲突。
