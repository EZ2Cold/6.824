package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Result struct {
	ReqID uint8
	Value string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	// 保存所有键值对
	kva map[string]string
	// 存放上一次请求的结果
	history map[int64]Result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.kva[args.Key]
	delete(kv.history, args.ClientID)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	res, exist := kv.history[args.ClientID]
	if exist && res.ReqID == args.ReqID {
		// 该请求已经执行过
		return
	}
	kv.kva[args.Key] = args.Value

	// 保存
	kv.history[args.ClientID] = Result{ReqID: args.ReqID}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	res, exist := kv.history[args.ClientID]
	if exist && res.ReqID == args.ReqID {
		// 该请求已经执行过
		reply.Value = res.Value
		return
	}
	value := kv.kva[args.Key]
	kv.kva[args.Key] = value + args.Value
	reply.Value = value

	// 保存
	kv.history[args.ClientID] = Result{ReqID: args.ReqID, Value: value}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kva = map[string]string{}
	kv.history = map[int64]Result{}
	return kv
}
