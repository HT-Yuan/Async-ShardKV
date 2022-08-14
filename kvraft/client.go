package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

const retransmission_interval = 300 * time.Millisecond

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderid int
	id       int64 //每一个Clerk具有唯一的索引
	cmdindex int   //command索引
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
	ck.leaderid = 0
	ck.id = nrand()
	ck.cmdindex = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
// Get是只读
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	args := GetArgs{Key: key}

	for {

		var reply GetReply
		ok := ck.servers[ck.leaderid].Call("KVServer.Get", &args, &reply)
		// fmt.Printf("leaderid[%d] reply.Err:%s  state:%t\n", ck.leaderid, reply.Err, ok)
		if ok {
			if reply.Err == OK { // RPC 发送成功 收到回复
				return reply.Value
			}
			if reply.Err == ErrNoKey {

				return ""
			}
			if reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
				ck.leaderid = (ck.leaderid + 1) % (len(ck.servers))
				continue
			}

		}

		ck.leaderid = (ck.leaderid + 1) % (len(ck.servers))
		//在不切换leaderid的情况下重传
		time.Sleep(retransmission_interval)
	}

	return ""

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.id, CommandId: ck.cmdindex}

	for {
		var reply PutAppendReply
		ok := ck.servers[ck.leaderid].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			if reply.Err == OK || reply.Err == ErrNoKey {

				ck.cmdindex++ //成功后 cmdindex递增
				args.CommandId = ck.cmdindex

				return
			}

			// DPrintf("Put append failure %d\n", reply.Err)

			if reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
				ck.leaderid = (ck.leaderid + 1) % (len(ck.servers))
				continue
			}
		}
		ck.leaderid = (ck.leaderid + 1) % (len(ck.servers))
		//在不切换leaderid的情况下重传

		time.Sleep(retransmission_interval)
	}

}

func (ck *Clerk) Put(key string, value string) {

	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {

	ck.PutAppend(key, value, "Append")
}
