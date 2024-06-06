# Lab 3: Raft
## Part 3A: leader election
每个服务器有一个`election timeout`计时器，超时后如果当前处于`Follower`或者`Candidate`状态，则开始进行选举。

选举时，对于每个服务器都创建一个对应的goroutine，该routine负责周期性地向服务器发起`RequestVote RPC`。

一个服务器通过选举成功成为Leader后，对于每个服务器都会创建一个对应的goroutine，该routine负责通过`AppendEntries RPC`周期性地向`Follower`发送心跳信息。

### RPC请求处理和响应处理
对`RequestVote`和`AppendEntries` RPC请求的处理可以参考论文的Figure2。

对RPC响应处理的步骤如下：
1. 判断是否发现更大的term，若是，更新当前term，转为Follower（如果当前为Candidate或者Leader），直接返回
2. 判断服务器当前的term与RPC发起时的term是否一致，若不一致，直接返回
3. 根据响应内容更新当前服务器状态

### RPC的幂等性
值得注意的是，每个RPC的调用都在一个单独的goroutine中进行，为了防止RPC调用阻塞后续执行。

RequestVote和AppendEntries都是幂等的（即如果参数相同，则任意多次执行所产生的影响均与一次执行的影响相同），可以同时对一个服务器发起多次相同调用。在进行RPC请求和响应处理时需要注意这一问题。


## Part 3B: log