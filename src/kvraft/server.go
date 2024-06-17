package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type CmdType int

const (
	GetCmd CmdType = iota
	PutCmd
	AppendCmd
)
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	// 命令的类型：Get/Put/Append
	Type     CmdType
	Key      string
	Value    string
	ClientId int64
	OpId     int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister
	// 保存所有键值对
	kva map[string]string
	// 上一条被应用的命令在raft log中的index
	lastApplied int
	// 条件变量，用于通知RPC请求响应client
	cond *sync.Cond
	// 保存每个用户上一个被执行的操作的Id
	lastOpIds map[int64]int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("S%d receive Get(%v)\n", kv.me, args.Key)
	kv.mu.Lock()
	lastOpId, exist := kv.lastOpIds[args.ClientId]
	if exist && args.OpId <= lastOpId {
		// 该命令已经执行过了，直接返回
		reply.Err = OK
		reply.Value = kv.kva[args.Key]
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:     GetCmd,
		Key:      args.Key,
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}
	idx, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("S%d is not leader, immediately return\n", kv.me)
		// 当前服务器不是Leader，直接返回
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("S%d append a log entry at index %d\n", kv.me, idx)
	// 等待大多数服务器对该命令达成共识，且当前服务器将命令应用于状态机之后，响应client
	kv.mu.Lock()
	for kv.lastApplied < idx {
		kv.cond.Wait()
	}
	kv.mu.Unlock()
	currentTerm, _ := kv.rf.GetState()
	if term != currentTerm {
		// 如果Leader的term发生了改变，则返回错误
		// 用来解决该日志条目没有达成大多数共识的情况，要求client重试
		// 可能存在false positive的情况，导致响应给client的时间变长
		reply.Err = ErrWrongLeader
	} else {
		DPrintf("command %d has been committed, reply to client\n", idx)
		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.kva[args.Key]
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("S%d receive Put(%v, %v)\n", kv.me, args.Key, args.Value)

	kv.mu.Lock()
	lastOpId, exist := kv.lastOpIds[args.ClientId]
	if exist && args.OpId <= lastOpId {
		// 该命令已经执行过了，直接返回
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:     PutCmd,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}
	idx, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("S%d is not leader, immediately return\n", kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("S%d append a log entry at index %d\n", kv.me, idx)
	kv.mu.Lock()
	for kv.lastApplied < idx {
		kv.cond.Wait()
	}
	kv.mu.Unlock()
	currentTerm, _ := kv.rf.GetState()
	if term != currentTerm {
		// 如果Leader的term发生了改变，则返回错误
		// 用来解决该日志条目没有达成大多数共识的情况，要求client重试
		// 可能存在false positive的情况，导致响应给client的时间变长
		reply.Err = ErrWrongLeader
	} else {
		DPrintf("command %d has been committed, reply to client\n", idx)
		reply.Err = OK
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("S%d receive Append(%v, %v)\n", kv.me, args.Key, args.Value)

	kv.mu.Lock()
	lastOpId, exist := kv.lastOpIds[args.ClientId]
	if exist && args.OpId <= lastOpId {
		// 该命令已经执行过了，直接返回
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:     AppendCmd,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}
	idx, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("S%d is not leader, immediately return\n", kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("S%d append a log entry at index %d\n", kv.me, idx)
	kv.mu.Lock()
	for kv.lastApplied < idx {
		kv.cond.Wait()
	}
	kv.mu.Unlock()
	currentTerm, _ := kv.rf.GetState()
	if term != currentTerm {
		// 如果Leader的term发生了改变，则返回错误
		// 用来解决该日志条目没有达成大多数共识的情况，要求client重试
		// 可能存在false positive的情况，导致响应给client的时间变长
		reply.Err = ErrWrongLeader
	} else {
		DPrintf("command %d has been committed, reply to client\n", idx)
		reply.Err = OK
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kva = map[string]string{}
	kv.lastApplied = 0
	kv.lastOpIds = map[int64]int64{}
	kv.recoverFromSnapshot()

	kv.cond = sync.NewCond(&kv.mu)

	go kv.applier()
	return kv
}

func (kv *KVServer) recoverFromSnapshot() {
	snapshot := kv.persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		// 没有snapshot
		DPrintf("no snapshot\n")
		return
	}
	var lastApplied int
	var kva map[string]string
	var lastOpIds map[int64]int64
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&lastApplied) != nil || d.Decode(&kva) != nil || d.Decode(&lastOpIds) != nil {
		log.Fatalf("recoverFromSnapshot: snapshot decode error")
	}
	kv.lastApplied = lastApplied
	kv.kva = kva
	kv.lastOpIds = lastOpIds
}

// 该函数不断地从applyCh中读取命令并应用于状态机
func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			cmd := msg.Command.(Op)
			kv.mu.Lock()
			lastOpId, exist := kv.lastOpIds[cmd.ClientId]
			if exist && cmd.OpId <= lastOpId {
				// 该命令已经执行过了
				kv.lastApplied = msg.CommandIndex
				kv.cond.Broadcast()
				DPrintf("S%d finish applying cmds until index %d, invoke signal\n", kv.me, kv.lastApplied)
				kv.mu.Unlock()
				continue
			}

			switch cmd.Type {
			case GetCmd:
			case PutCmd:
				kv.processPut(&cmd)
			case AppendCmd:
				kv.processAppend(&cmd)
			}
			// 更新最后执行的操作的Id
			kv.lastOpIds[cmd.ClientId] = cmd.OpId
			// 更新应用于状态机的最后一条命令的在raft log中的index
			kv.lastApplied = msg.CommandIndex

			// 判断是否需要调用Snapshot函数
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.lastApplied)
				e.Encode(kv.kva)
				e.Encode(kv.lastOpIds)
				DPrintf("S%d invoke Snapshot(%d, x)\n", kv.me, kv.lastApplied)
				kv.rf.Snapshot(kv.lastApplied, w.Bytes())
			}
			kv.mu.Unlock()

		} else if msg.SnapshotValid {
			DPrintf("S%d install a snapshot\n", kv.me)
			kv.mu.Lock()
			var lastApplied int
			var kva map[string]string
			var lastOpIds map[int64]int64
			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)
			if d.Decode(&lastApplied) != nil || d.Decode(&kva) != nil || d.Decode(&lastOpIds) != nil {
				log.Fatalf("applier: snapshot decode error")
			}
			kv.lastApplied = lastApplied
			kv.kva = kva
			kv.lastOpIds = lastOpIds
			kv.mu.Unlock()
		} else {
			// 忽略其他命令
		}
		if msg.CommandValid || msg.SnapshotValid {
			kv.cond.Broadcast()
			DPrintf("S%d finish applying cmds until index %d, invoke signal\n", kv.me, kv.lastApplied)
		}
	}
}

func (kv *KVServer) processPut(op *Op) {
	kv.kva[op.Key] = op.Value
}

func (kv *KVServer) processAppend(op *Op) {
	kv.kva[op.Key] = kv.kva[op.Key] + op.Value
}
