# kvpaxos
这个东西断断续续写了两周，写代码花了两天，debug调用两周。最终发现，问题并不是出现在代码问题上，而是出现在自己的设计上。下面，我将描述我的整个实现过程以及其中遇到的坑。

# 错误模型
错误模型与paxos的错误模型相同。由于所有的状态都是在内存里的，所以一旦crash，这些状态就没有了，问题比较大。

# Replicated Log
kvpaxos的实质是一个基于replicated log的线性一致性kv存储。这个存储由一个内存store加一个log组成。所以更新先进log，然后直接返回，并异步应用到store中。Get操作读store（为了保证一致性，也需要走一遍Log，然后同步apply所有未apply的日志项）。

Log由连续的entry组成，每个entry由一个id唯一标识，id是唯一的。Log可以分成三个部分

+ 已遗忘: id < appyPoint的entries
+ 已提交: applyPoint <= id < commitPoint的entries，这些entries都是已经提交了，而没有应用到store上的
+ 待添加: id >= commitPoint的项是空白项，还没有添加

为了保证不同peer上store是相同的，且读写具有线性一致性语义，我们有以下需求：

+ 不同peer上，store初始皆为空
+ 对于任意peer，当log i commit时，其上所有k <= i的log都处于commit状态
+ 对于任意peer，当log i commit时，所有其他节点上的log i与其具有相同的值，并处于commit状态

以上三条规则保证，所有peer上的log状态相同，且store+log会生成相同的状态。同时，若操作j发生在操作i完成后，则其effect也必定排在i后面。

显然，这里的关键是规则三，很自然的，我们使用paxos来对log entry i达成一致。当paxos运行结束，对于任意peer，log entry i上的位置值是一样的且不会改变。这里，我们使用i将作seq值。再考虑规则二，这可以通过以下方式来实现：在运行seq=i的paxos时，先同步等待所有seq\<i的项达到一致状态。

# 线性一致性
上面的规则保证了Put/Append是线性一致性的，现在考虑Get。由于允许多写且错误存储，因此，需要确定当前peer上的值是不落后的，我们需要append一个新的entry，令其seq=100，当其执行完毕后，根据规则二，发生Get请求之前的请求都已经完成了，此时，我们将已经commit但是还没有apply的日志apply到store上，并直接在store上执行Get请求。

# RPC at-most-once
由于clerk在执行一个RPC失效后，会换一个服务器，因此，只在RPC层做去重是不够的，我们需要在应用层去重。

去重逻辑比较简单，每个clerk一个ClerkId，同时，单调增加其所发出的RPC的RpcId，服务端记录其所看到的特定clerk的rpcId，由于clerk在一个RPC完成后才会执行下一个，所以，服务端可以简单拒绝所有特定clerk上，所有小于等于自己看到的值的rpc。

这里有一个实际的问题。在生产环境中，如何保证clerk id的唯一性？并且，实际上，这里的clerk等价于一个线程，在生产环境中，一个client会有多个线程，也即多个clerk。我们想到的办法是服务器端给client分配一个全局唯一的Id，然后，client将Id+线程号做为线程的Id。

然而，这些实际上依然是不够的，根据end-to-end原理，真正的去重是在业务层做的。毕竟client也会挂，client挂了之后，重试请求，依然会重复。

# Catchup
考虑三个服务器s1-3，当s3长时间partition，而s1/s2处理了许多请求。此时，s3的网络恢复，它需要怎么catchup？

目前的实现时，如果有请求到s3，则s3尝试分配log index，由于它长时间流离，所以它的commitPoint相当落后。比如，s1/s2的commitPoint都是100，而s3的commitPoint是10。于是，s3就令log index=10进行append，显然会失败，于是index=11/12/13...100。直到100时才会成功。

对应的，由于允许多点写，s1/s2可能同时收到请求，于是，同时发起index=100的append，最终，一个会因此冲突也abort，并尝试index=101。

显然，这样的写效率太低了。

# 踩过的坑
现在提一下我做这个Lab时踩过的坑。

+ RPC exactly-once：
    + 处理exactly-once的方法是retry+应用层去重，而一开始，由于想着，幂等操作不需要应用层去重，因此，我只对append进行了去重，对于get/put都没有去重（毕竟Put也是幂等的），于是结果就是，单只有一个clerk时，它先put了k=1，又put了k=2，然后get，结果读到了k=1，这里因此put k=1时，有一个消息没有响应，于是就重试了。最终，没有响应的消息在put k=2后之后执行了，然后k=1了。显然，我对幂等的理解出现了偏差。幂等是一个操作重复多个次，其结果不变，但是中间不能有别的操作，不然基础条件都不一样，其产生的结果自己也就不一样了。
    + 需要注意的是，这里，在传输层处理是不够的，因此clerk重试时，可能会换一个服务器
+ Replicated Log实现：整个协议我前后改正了三版。。。
    + 第一版本：log append不加锁；log append时，选取seq = paxos.Max()。其结果是，一个后来的操作，选取了一个paxos.Max()值比较小的peer，然后append，于是，它可以排在之前操作的前面。
    + 第二版本：log append不加锁；log append时，选取seq = commitPoint++(令seq=255)，然后发起paxos。其结果是，peer1执行k append 1。之后，它又执行k append 2，结果丢包了，于是它到peer2上执行k append 2。结果，peer2上seq=commitPoint++的值是254，且对seq=254运行paxos时，由于之前的paxos没有成功，而这个实例刚才成功了，于是乎，append 2发生在了append 1前面。注意，这里的问题是，log append不加锁，于是乎，一个新的log append只是简单拿了一个seq号，就开始运行。当一个seq=100的paxos运行完毕，可能还有一个seq=99的正在运行，于是乎，这个seq=99的可以会被后来的操作抢走。
    + 第三版本：log append加锁，从而保证，log append i时，之前的k < i都已经commit了
