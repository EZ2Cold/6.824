# Lab 3: Raft
## Part 3A: leader election
RequestVote和AppendEntries都是幂等的，可以同时对一个服务器发起多次调用。
