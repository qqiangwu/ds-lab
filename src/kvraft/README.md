# 尚不清楚的点
+ client需要等待结果，如何等待？
    + 在index上等待
+ server leadership丢失
    + 当等待的index上出现了别的东西，通知client：不一定会出现，因为，新的leader可能不提交任何东西(不提交的话，就无限等待吧)
    + Term改变
    + partitioned：不需要关注，因为client与它在同一个网络中，无限等待即可
+ server.Get：也需要走log
+ client缓存谁是master
+ 什么样的请求要去重？
+ 如何标识一个请求
    + client可以用随机的int64
    + seq呢？wrap了怎么办？
+ kvserver与raft是异步的，如何保证一致性，一把锁够吗？
    + Applier运行在后台
    + 能够根据前台发现的新term，而取消旧的term？
        + 不能，旧的term+index可能已经被应用了，但是还没有apply
    + 把raft所有操作当作原子的，然后，由Applier手动进行一致性同步
+ 编程错误的处理
    + 目前是直接panic，能否屏蔽掉？

# 3A
+ 服务器可靠，网络可靠情况下的线性KV
+ Exactly-once语义

# 实现情况
+ 写完3A了，然后遇到deadlock了，用gdb也无法发现
    + 用print，发现是卡在apply中的rf.GetState上了
    + 发现问题了，raft send结果给applyCh时，applyCh会阻塞，阻碍已经send的东西被消费了。然而，消费需要拿raft.GetState()
    + 本来我是想用队列来解耦applyCh与raft正常流，但是，由于applyCh的阻塞性，raft正常流也被阻塞了，从而，无法完成接下来的请求。（启示：用mutex序列化一个执行流时，一定保证，每个序列中过程不能阻塞）
    + 现在，解决问题的一个比较直接的方法是，让channel无限大；否则，即使设置一个很大的值，依然可能阻塞。
    + 让mutex同步的方法调用理解为一个消息发送操作
    + 在applyNew时，apply一个放一次锁能否解决问题？不确定，因为它将一个大操作分解成了一个一个小操作，这些操作中间可能会插入其他操作，从而可能违反语义。
    + 最后还是applyNew时放了锁，但这必须保证，调用applyNew时，对象保持invariant
+ 第一个死锁问题修正
+ 遇到第二个死锁问题：出现在partition one client中
