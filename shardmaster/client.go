package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

// 与kvraft类似 client
type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	// leaderid int
	id       int64 //每一个Clerk具有唯一的索引
	cmdindex int   //command索引

}

// func (ck *Clerk) args() Args {
// 	return Args{ClientId: ck.cid, CommandId: ck.cmdindex}
// }

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// ck.leaderid = 0
	ck.id = nrand()
	ck.cmdindex = 0
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &CommandArgs{}
	// Your code here.
	args.Num = num
	args.Op = "Query"
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CommandReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// func (ck *Clerk) Join(servers map[int][]string) {
// 	args := &CommandArgs{}
// 	// Your code here.
// 	args.Servers = servers
// 	args.ClientId = ck.id
// 	args.Cmdindex = ck.cmdindex
// 	args.Op = "Join"
// 	for {
// 		// try each known server.
// 		for _, srv := range ck.servers {
// 			var reply CommandReply
// 			ok := srv.Call("ShardMaster.Command", args, &reply)
// 			if ok && reply.WrongLeader == false {
// 				ck.cmdindex++ //成功后 cmdindex递增
// 				args.Cmdindex = ck.cmdindex
// 				return
// 			}
// 		}
// 		time.Sleep(100 * time.Millisecond)
// 	}
// }

// func (ck *Clerk) Leave(gids []int) {
// 	args := &CommandArgs{}
// 	// Your code here.
// 	args.GIDs = gids
// 	args.ClientId = ck.id
// 	args.Cmdindex = ck.cmdindex
// 	args.Op = "Leave"

// 	for {
// 		// try each known server.
// 		for _, srv := range ck.servers {
// 			var reply CommandReply
// 			ok := srv.Call("ShardMaster.Command", args, &reply)
// 			if ok && reply.WrongLeader == false {
// 				ck.cmdindex++ //成功后 cmdindex递增
// 				args.Cmdindex = ck.cmdindex
// 				return
// 			}
// 		}
// 		time.Sleep(100 * time.Millisecond)
// 	}
// }

// func (ck *Clerk) Move(shard int, gid int) {
// 	args := &CommandArgs{}
// 	// Your code here.
// 	args.Shard = shard
// 	args.GID = gid
// 	args.ClientId = ck.id
// 	args.Cmdindex = ck.cmdindex
// 	args.Op = "Move"

// 	for {
// 		// try each known server.
// 		for _, srv := range ck.servers {
// 			var reply CommandReply
// 			ok := srv.Call("ShardMaster.Command", args, &reply)
// 			if ok && reply.WrongLeader == false {
// 				ck.cmdindex++ //成功后 cmdindex递增
// 				args.Cmdindex = ck.cmdindex
// 				return
// 			}
// 		}
// 		time.Sleep(100 * time.Millisecond)
// 	}
// }

func (ck *Clerk) WriteCore(args *CommandArgs) {
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply CommandReply
			ok := srv.Call("ShardMaster.Command", args, &reply)
			if ok && reply.WrongLeader == false {
				// DPrintf("client[%d] done[%v][%d]", ck.id, args.Op, args.Cmdindex)
				ck.cmdindex++ //成功后 cmdindex递增

				// args.Cmdindex = ck.cmdindex
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &CommandArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.id
	args.Cmdindex = ck.cmdindex
	args.Op = "Join"

	ck.WriteCore(args)
}
func (ck *Clerk) Leave(gids []int) {
	args := &CommandArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.id
	args.Cmdindex = ck.cmdindex
	args.Op = "Leave"
	ck.WriteCore(args)
}
func (ck *Clerk) Move(shard int, gid int) {
	args := &CommandArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.id
	args.Cmdindex = ck.cmdindex
	args.Op = "Move"
	ck.WriteCore(args)
}
