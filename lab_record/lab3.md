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
### 每个服务器的线程

### Figure 8

### 问题记录
1. TestBackup3B测试中遇到的问题，如果每收到一个RequestVote就重置自己的选举计时器，会导致拥有up-to-date的服务器久久不能成为leader，达成一致的时间较长
只有投赞成票才重置超时计时器


## Part 3C: persistence
1. 为什么需要持久化

2. 什么时候需要进行持久化
* 执行RequestVote函数时
* 执行AppendEntries函数时
* Start函数中向Leader追加日志时
* 由Follower转为Cnadidate时保存term和voteFor
* 处理RequestVote响应时
* 处理AppendEntries响应时

3. 优化nextIndex的回退以通过TestFigure8Unreliable3C测试
Leader第一次与Follower同步完成之后，就将剩余日志全部发送给Follower

## Part 3D: log compaction
1. 应用层调用Snapshot()函数，
    在该函数中，raft持久化snapshot，裁剪自己的log
2. leader与follower进行日志同步时，如果要发送的log entry在snapshot中，要调用InstallSnapshot PRC，将最新的snapshot同步给follower
3. server重新启动时
    * 恢复raft状态（currentTerm，voteFor，log）和snapshotde的lastIncludedIndex/Term
    * 将snapshot发送给应用层
    * 裁剪log（如果log包含snapshot中的日志条目）

引入snapshot后其他需要改变的地方：
1. RequestVote函数
    进行update-to-date检查（checkUpToDate函数）时需要获取最后一个日志条目的真实index和term
2. AppendEntreis函数
* 判断prevLogIndex和prevLogTerm是否一致
* 若一致，更新日志
* 若不一致，找到冲突的term的第一个index
3. Start函数返回值需要修改
3. submit函数中修改获取log时的index
4. ticker函数中
* 发出RequestVote请求时，要获取自己最后一个日志条目的真实index和term
* updateCommitIndex函数中获取最后一个条目的index
* sendAppendEntries函数中，如果nextIndex在snapshot中，发起InstallSnapshot RPC
* handleAppendEntriesReply函数中


### 问题记录
1. updateLog函数要返回日志更新后最后一个匹配的条目的index
更新follower的commitIndex时需要注意
2. submit中加锁，之后可能调用Snapshot再次加锁，造成死锁，-race检测不出来，snapshot函数中不加锁，在submit函数中最后提交一个无效命令确保前面的命令都被应用了
3. 执行InstallSnapshot函数时需要确保状态机中没有待应用的日志条目
    如果有，则可能在应用这些日志条目时该服务器自身会产生新的snapshot，导致之前的判断错误，造成snapshot回退，状态机状态丢失的情况
4. 引入snapshot之后，调用persister.Save两个参数都不能为空，否则会丢失持久化的数据
5. state machine 调用Snapshot函数时传递的数据包含lastIncludedIndex和键值对，raft传递回去时一致
6. config.go中start1函数启动raft，会自动将snapshot同步给state machine，所以raft的readSnapshot函数中不需要再将snapshot发送给应用层
7. raft持久化的snapshot与state machine传递的一致，lastIncludedTerm作为raft status持久化
8. 需要确保将snapshot发送给state machine之后，下一条命令的index为lastIncludedIndex+1
