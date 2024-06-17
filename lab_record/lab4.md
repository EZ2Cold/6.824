# Lab 4: Fault-tolerant Key/Value Service
## Part A: Key/value service without snapshots
Lab2实现了一个单机的key/value服务器，本实验则是基于Lab3的raft实现一个具有容错能力的分布式的key/value服务器。

client可以进行如下操作：
* Get：返回key对应的值，若key不存在，返回空字符串。
* Put：设置一个key的值
* Append：追加一个key的值，无返回值，若key不存在，在空串后追加。


### no network/server failure
当服务器收到Get/Put/Append RPC时，首先通过`Start()`函数向raft提交该command，有以下两种结果：
1. 如果该服务器不是Leader，直接返回错误，client尝试向其他服务器发出请求
2. 该服务器为Leader，等待该条命令被raft提交且被应用于状态机。
    * 服务器保存应用于状态机的最后一条命令的下标`lastApplied`，当lastApplied小于Start函数返回的下标idx时，RPC调用阻塞于一个条件变量。
    * 一个后台线程不断地从`applyCh`中读取并执行命令，更新lastApplied，并唤醒阻塞于条件变量的线程。
    * RPC调用被唤醒之后回复client。


### network/server failures exist
服务器不一定能够对一个日志条目达成大多数共识（Leader可能在调用Start()之后失去leadership），此时需要通知client重试。当lastApplied大于等于idx时，还需要判断应用于状态机的下标为idx的日志的term与Start()函数返回的term是否一致，若一致则达成共识，若不一致则通知客户端向新的服务器重试。

对于一个Get/Put/Append操作，client可能会同时向一个或多个服务器发出多次重复请求（由于请求长时间未响应/请求失败等原因），直到操作成功为止。因此，raft的日志中可能存在关于一个操作的多个日志条目，需要进行duplicate detection，保证每个操作只执行一次。

## Part B: Key/value service with snapshots
### snapshot要包含哪些东西
* lastIncludedIndex和状态机状态（与lab3兼容）
* 每个用户上一个执行的操作编号，确保安装了snapshot之后还可以进行去重检测

### 何时进行snapshot
每次执行一个command之后判断raft state size 是否大于maxraftstate，若是，则调用Snapshot函数。

key/value服务器启动时，若有snapshot，则通过其恢复状态机状态。

SnapshotValid为true的命令没有CommandIndex，该条命令不存在于raft的log中。

