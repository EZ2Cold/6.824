package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type ServerStatus int

const (
	Follower ServerStatus = iota
	Candidate
	Leader
)

// 定义每个日志条目
type logEntry struct {
	// 添加条目的leader的term
	Term int
	// 命令
	Command interface{}
}

// 基础选举超时时间
const ElectionTimeout = 1000 * time.Millisecond

// 心跳周期
const HeartbeatTimeout = 200 * time.Millisecond

// RequestVote重发时间
const RequestVoteTimeout = 200 * time.Millisecond

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 选举超时计时器开始时间
	electionStartTime time.Time
	// 当前投票结果
	electionRes map[int]bool
	// 获得的选票数
	voteGot int
	// 服务器当前状态
	status ServerStatus
	// 该服务器当前的任期号
	currentTerm int
	// 该服务器在当前任期内投了票的候选人ID
	votedFor int
	// 日志
	log []logEntry
	// 已提交日志中的最大index
	commitIndex int
}

func (rf *Raft) printState() {
	DPrintf("======================")
	DPrintf("state of S%d\n", rf.me)
	switch rf.status {
	case Follower:
		DPrintf("status - %s\n", "Follower")
	case Candidate:
		DPrintf("status - %s\n", "Candidate")
	case Leader:
		DPrintf("status - %s\n", "Leader")
	}
	DPrintf("term - %d\n", rf.currentTerm)
	DPrintf("votedFor - %d\n", rf.votedFor)
	DPrintf("======================")

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	// candidate的term
	Term int
	// candidate的ID
	Candidateid int
	// candidate最后一个日志条目的index和term，用于检查是否up-to-date
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	// 当前服务器的term
	Term int
	// 是否投票给candidate
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		DPrintf("S%d do not vote for S%d since it has larger term\n", rf.me, args.Candidateid)
	} else if args.Term == rf.currentTerm {
		if rf.votedFor == -1 {
			// 在该term还没有投过票
			// TODO 检验是否up-to-date
			if true {
				rf.votedFor = args.Candidateid
				reply.VoteGranted = true
			} else {
				reply.VoteGranted = false
			}
		} else {
			// 在该term已经投过票
			reply.VoteGranted = (rf.votedFor == args.Candidateid)
		}
	} else {
		// 更新该结点的term
		rf.currentTerm = args.Term
		// 如果当前节点为Leader或者Candidate，转为Follower
		rf.status = Follower
		// 重置选举计时器
		rf.electionStartTime = time.Now()
		rf.votedFor = -1
		// TODO 检查是否up-to-date
		if true {
			rf.votedFor = args.Candidateid
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}

	}
	if reply.VoteGranted {
		DPrintf("S%d vote for S%d\n", rf.me, args.Candidateid)
	} else {
		DPrintf("S%d do not vote for S%d\n", rf.me, args.Candidateid)
	}
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	// leader的term
	Term int
	// leader的id，用户重定向用户请求
	LeaderId int
	// 要追加的日志
	Entries []logEntry
	// 待追加日志前一条日志的index和term
	PrevLogIndex int
	PrevLogTerm  int
	// leader的commitIndex
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("S%d receive a heartbeat from S%d", rf.me, args.LeaderId)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else {
		// 当前服务器可能为老的leader，或者选举失败的candidate，或者已经是follower了
		if args.Term > rf.currentTerm {
			rf.votedFor = -1
		}
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.electionStartTime = time.Now()
		DPrintf("S%d turn to a follower of leader S%d\n", rf.me, args.LeaderId)
		// 判断preLogIndex和preLogTerm是否一致
		if len(rf.log) >= args.PrevLogIndex+1 && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
			reply.Success = true
		} else {
			reply.Success = false
		}
	}
	reply.Term = rf.currentTerm
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		// 如果选举计时器超时，且当前服务器为处于Follower/Candidate状态，开始选举
		rf.mu.Lock()
		if (time.Since(rf.electionStartTime) >= ElectionTimeout) && (rf.status == Follower || rf.status == Candidate) {
			// 重置选举计时器
			rf.electionStartTime = time.Now()
			// 状态变更为Candidate
			rf.status = Candidate
			// 增加自己的term
			rf.currentTerm++
			// 为自己投票
			rf.votedFor = rf.me
			// 重置选举结果
			rf.electionRes = map[int]bool{}
			// 给自己投一票
			rf.voteGot = 1
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go func(i int, term int) {
					// 该goroutine周期性地向一个特定的服务器发起RequestVote
					// 每个RPC都在一个go routine中发起，避免阻塞，一个例子是，给一个服务器掉线了的服务器发送RPC，如果不向该服务器继续发出新的RPC，则永远不能得到它的票

					// 在以下条件下不再发送
					// 如果发出时的term与当前term不一致，可能开始了新一轮的选举，也可能成为了更高term的follower，直接返回
					// 某次调用成功后得到了投票结果，不再发送
					// 如果当前状态为follower（其他具有相同term的服务器成为leader）或leader（已经选举成功），直接返回
					for !rf.killed() {
						rf.mu.Lock()
						if term != rf.currentTerm {
							rf.mu.Unlock()
							return
						}
						_, exist := rf.electionRes[i]
						if exist {
							rf.mu.Unlock()
							return
						}
						if rf.status == Follower || rf.status == Leader {
							rf.mu.Unlock()
							return
						}
						args := &RequestVoteArgs{Term: rf.currentTerm, Candidateid: rf.me, LastLogIndex: len(rf.log) - 1, LastLogTerm: rf.log[len(rf.log)-1].Term}
						DPrintf("S%d RequestVote from S%d\n", rf.me, i)
						go func() {
							reply := &RequestVoteReply{}
							ok := rf.peers[i].Call("Raft.RequestVote", args, reply)
							if ok {
								rf.mu.Lock()
								if reply.Term > rf.currentTerm {
									rf.status = Follower
									rf.currentTerm = reply.Term
									rf.votedFor = -1
									// 重置选举计时器
									rf.electionStartTime = time.Now()
									rf.mu.Unlock()
									return
								}
								if term != rf.currentTerm {
									// 发出时的term与当前term不一致，直接返回
									rf.mu.Unlock()
									return
								}
								_, exist := rf.electionRes[i]
								if !exist {
									rf.electionRes[i] = reply.VoteGranted
									if reply.VoteGranted {
										rf.voteGot++
									}
								}
								// 恰好拿到（N+1）/2个票数时启动后台routine发出心跳信息
								if rf.voteGot == 1+len(rf.peers)/2 {
									rf.status = Leader
									DPrintf("S%d win the election!\n", rf.me)
									rf.sendAppendEntries()
								}
								rf.mu.Unlock()
							}
						}()
						rf.mu.Unlock()
						time.Sleep(RequestVoteTimeout)
					}
				}(i, rf.currentTerm)
			}

		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) sendAppendEntries() {
	// 启动多个goroutine并行地向follower发送AppendEntries RPC
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int, term int) {
			// 参数term保存该goroutine属于的term
			for !rf.killed() {
				// 可以同时向同一个follower发送多个AppendEntries RPC
				// TODO raft中的RPC是幂等的，需要注意这个问题
				rf.mu.Lock()
				if rf.currentTerm != term {
					// 如果服务器的term改变了，不再发起AppendEntries RPC
					rf.mu.Unlock()
					DPrintf("S%d's AppendEntries goroutine exit\n", rf.me)
					return
				}
				args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
				go func() {
					reply := &AppendEntriesReply{}
					ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)
					if !ok {
						// 调用失败，直接返回
						return
					}
					// 调用成功，判断是否需要更新当前服务器的状态
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						// 发现更大的term，转为follower
						rf.currentTerm = reply.Term
						rf.status = Follower
						rf.votedFor = -1
						rf.electionStartTime = time.Now()
						rf.mu.Unlock()
						return
					}
					if rf.currentTerm != args.Term {
						// 发出请求到得到响应期间，term发生了改变，直接返回
						rf.mu.Unlock()
						return
					}
					// TODO 调整nextIndex和matchIndex

					rf.mu.Unlock()
				}()
				rf.mu.Unlock()
				time.Sleep(HeartbeatTimeout)
			}
		}(i, rf.currentTerm)
	}

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.status = Follower
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.electionStartTime = time.Now()
	rf.voteGot = 0
	rf.electionRes = map[int]bool{}

	// log下标从1开始，所以初始时添加一个空条目
	rf.log = append(rf.log, logEntry{Term: 0})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
