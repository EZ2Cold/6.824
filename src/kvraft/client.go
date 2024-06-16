package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

const RPCTimeout = 100 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	// 当前的leader服务器的编号（推测）
	leaderId int
	// 用户编号
	clientId int64
	// 操作编号
	opId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.opId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{Key: key, ClientId: ck.clientId, OpId: ck.opId}
	res := ""
	finish := false
	var mu sync.Mutex
	done := make(chan bool)
	retry := make(chan bool)
	ticker := time.NewTicker(RPCTimeout)
	for {
		go func(i int) {
			reply := GetReply{}
			DPrintf("Client %d send Get(%v) to Server(handler num %d)\n", ck.clientId, key, ck.leaderId)
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
			if ok && reply.Err == OK {
				mu.Lock()
				if !finish {
					finish = true
					res = reply.Value
					ck.opId++
					DPrintf("Client %d sucessfully receive Get(%v) reply from Server(handler num %d)\n", ck.clientId, key, ck.leaderId)
					done <- true
				}
				mu.Unlock()
			} else {
				// 请求失败重试
				defer func() {
					if r := recover(); r != nil {
						// retry channel可能已经被关闭了，不产生panic
					}
				}()
				retry <- true
			}
		}(ck.leaderId)
		select {
		case <-done:
			close(retry)
			ticker.Stop()
			return res
		case <-retry:
			// 当前尝试的服务器不可达或者不是Leader，向其他服务器发送请求
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			ticker.Reset(RPCTimeout)
		case <-ticker.C:
			// 请求长时间未响应，重试
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{Key: key, Value: value, ClientId: ck.clientId, OpId: ck.opId}
	finish := false
	var mu sync.Mutex
	done := make(chan bool)
	retry := make(chan bool)
	ticker := time.NewTicker(RPCTimeout)
	for {
		go func(i int) {
			reply := PutAppendReply{}
			DPrintf("Client %d send %v(%v, %v) to Server(handler num %d)\n", ck.clientId, op, key, value, ck.leaderId)
			ok := ck.servers[ck.leaderId].Call("KVServer."+op, &args, &reply)
			if ok && reply.Err == OK {
				mu.Lock()
				if !finish {
					finish = true
					ck.opId++
					DPrintf("Client %d sucessfully receive %v(%v, %v) reply from Server(handler num %d)\n", ck.clientId, op, key, value, ck.leaderId)
					done <- true
				}
				mu.Unlock()
			} else {
				// 请求失败重试
				defer func() {
					if r := recover(); r != nil {

					}
				}()
				retry <- true
			}
		}(ck.leaderId)
		select {
		case <-done:
			close(retry)
			ticker.Stop()
			return
		case <-retry:
			// 当前尝试的服务器不可达或者不是Leader，向其他服务器发送请求
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			ticker.Reset(RPCTimeout)
		case <-ticker.C:
			// 请求长时间未响应，重试
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
