package shardmaster

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	done          map[int]chan int
	lastoperation map[int64]int //防止重写
	serversLen    int
}

const INT_MAX = int(^uint(0) >> 1)
const Debug = 0
const TimeoutInterval = 500 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// type Op struct {
// 	// Your data here.
// }

// func (sm *ShardMaster) Join(args *CommandArgs, reply *CommandReply) {
// 	// Your code here.
// 	_, reply.WrongLeader, reply.Err = sm.Command("Join", *args)
// }

// func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
// 	// Your code here.
// }

// func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
// 	// Your code here.
// }

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(CommandArgs{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.done = make(map[int]chan int)
	sm.lastoperation = make(map[int64]int)
	sm.serversLen = len(servers)
	go sm.applier()

	// Your code here.

	return sm
}

func (sm *ShardMaster) Query(args *CommandArgs, reply *CommandReply) {
	// Your code here.
	val := sm.QueryCore(args, reply)

	if reply.Err == OK && !reply.WrongLeader {
		sm.mu.Lock()
		reply.Config = sm.configs[val]
		sm.mu.Unlock()
	}
}
func (sm *ShardMaster) QueryCore(args *CommandArgs, reply *CommandReply) (result int) {
	index, _, isLeader := sm.rf.Start(*args) //日志索引 term 是否是leader
	if !isLeader {
		reply.Err = OK
		reply.WrongLeader = true
		return -1
	}
	ch := make(chan int, 1)
	sm.mu.Lock()
	sm.done[index] = ch
	sm.mu.Unlock()
	select {
	case v := <-ch:
		reply.Err = OK
		reply.WrongLeader = false

		return v
	case <-time.After(TimeoutInterval):
		reply.Err = ErrTimeout
		reply.WrongLeader = false
		return -1
	}

}

// func (sm *ShardMaster) Command(args *CommandArgs, reply *CommandReply) {

// }

func (sm *ShardMaster) Command(args *CommandArgs, reply *CommandReply) {
	index, _, isLeader := sm.rf.Start(*args) //日志索引 term 是否是leader
	if !isLeader {
		reply.Err = OK
		reply.WrongLeader = true
		return
	}
	ch := make(chan int, 1)
	sm.mu.Lock()
	sm.done[index] = ch
	sm.mu.Unlock()
	select {
	case <-ch:
		reply.Err = OK
		reply.WrongLeader = false

		return
	case <-time.After(TimeoutInterval):
		reply.Err = ErrTimeout
		reply.WrongLeader = false
		return
	}

}

// 这是server 需要读取applych 并应用
func (sm *ShardMaster) apply(command CommandArgs) {

	switch command.Op {
	case "Join":
		if cmdindex, ok := sm.lastoperation[command.ClientId]; ok && cmdindex == command.Cmdindex {
			DPrintf("lastoperation Joined %d %d", command.ClientId, command.Cmdindex)
			return
		}
		newconf := sm.newconf()
		for k, v := range command.Servers {
			newconf.Groups[k] = v
		}

		sm.rebalance(&newconf)
		sm.mu.Lock()
		sm.configs = append(sm.configs, newconf)
		sm.mu.Unlock()

	case "Leave":
		if cmdindex, ok := sm.lastoperation[command.ClientId]; ok && cmdindex == command.Cmdindex {
			DPrintf("lastoperation Leaved %d %d", command.ClientId, command.Cmdindex)
			return
		}
		newconf := sm.newconf()
		map_gid2listshard := map[int][]int{0: {}} //

		for gid := range newconf.Groups {
			map_gid2listshard[gid] = []int{}
		}

		for i, gid := range newconf.Shards {
			map_gid2listshard[gid] = append(map_gid2listshard[gid], i)
		}
		DPrintf("[%d] Leave[gid:%v] before %v", sm.me, command.GIDs, map_gid2listshard)
		for _, k := range command.GIDs {
			for i, gid := range newconf.Shards { //被删除的gid 重还给0
				if gid == k {
					newconf.Shards[i] = 0
				}
			}
			delete(newconf.Groups, k)
		}
		map_gid2listshard = map[int][]int{0: {}} //

		for gid := range newconf.Groups {
			map_gid2listshard[gid] = []int{}
		}

		for i, gid := range newconf.Shards {
			map_gid2listshard[gid] = append(map_gid2listshard[gid], i)
		}
		DPrintf("[%d] LeaveLeave[gid:%v] after rebalance before%v", sm.me, command.GIDs, map_gid2listshard)
		sm.rebalance(&newconf)
		map_gid2listshard = map[int][]int{0: {}} //

		for gid := range newconf.Groups {
			map_gid2listshard[gid] = []int{}
		}

		for i, gid := range newconf.Shards {
			map_gid2listshard[gid] = append(map_gid2listshard[gid], i)
		}
		DPrintf("[%d]  rebalance after %v", sm.me, map_gid2listshard)
		// var gid_order []int                       //为了方便各节点统一顺序遍历map 列出一个gid数组
		// map_gid2listshard := map[int][]int{0: {}} //

		// for gid := range newconf.Groups {
		// 	map_gid2listshard[gid] = []int{}
		// 	gid_order = append(gid_order, gid)
		// }
		// sort.Ints(gid_order)

		// for i, gid := range newconf.Shards {
		// 	map_gid2listshard[gid] = append(map_gid2listshard[gid], i)
		// }
		// orphanShards := make([]int, 0)

		// for _, gid := range command.GIDs {
		// 	if _, ok := newconf.Groups[gid]; ok {
		// 		delete(newconf.Groups, gid)
		// 	}
		// 	if shards, ok := map_gid2listshard[gid]; ok {
		// 		orphanShards = append(orphanShards, shards...)
		// 		delete(map_gid2listshard, gid)
		// 	}
		// }
		// var newShards [NShards]int
		// // load balancing is performed only when raft groups exist
		// if len(newconf.Groups) != 0 {
		// 	for _, shard := range orphanShards {
		// 		target := GetGIDWithMinimumShards(map_gid2listshard, gid_order)
		// 		map_gid2listshard[target] = append(map_gid2listshard[target], shard)
		// 	}
		// 	for gid, shards := range map_gid2listshard {
		// 		for _, shard := range shards {
		// 			newShards[shard] = gid
		// 		}
		// 	}
		// }
		// newconf.Shards = newShards
		sm.mu.Lock()
		sm.configs = append(sm.configs, newconf)
		sm.mu.Unlock()

	case "Move":
		if cmdindex, ok := sm.lastoperation[command.ClientId]; ok && cmdindex == command.Cmdindex {
			DPrintf("lastoperation Joined %d %d", command.ClientId, command.Cmdindex)
			return
		}
		newconf := sm.newconf()
		newconf.Shards[command.Shard] = command.GID
		sm.mu.Lock()
		sm.configs = append(sm.configs, newconf)
		sm.mu.Unlock()

	}

	if command.Op != "Query" {
		sm.mu.Lock()
		sm.lastoperation[command.ClientId] = command.Cmdindex
		sm.mu.Unlock()
	}

	// DPrintf("Applied {raft[%d] %v command:%d}", sm.me, command.Op, command.Cmdindex)

}

func (sm *ShardMaster) applier() {
	for {

		select {
		case message := <-sm.applyCh:
			// DPrintf("[server %d] tries to apply message %v", kv.me, message)
			if message.CommandValid { // 对 message.Command 进行解析
				// if message.Command == nil {
				// 	continue
				// }
				command := message.Command.(CommandArgs)
				if command.Op != "Query" {
					sm.Debug("message.Index:%d command:%+v", message.CommandIndex, message.Command)
				}

				// fmt.Printf("[shardmaster] [%d] message.Index %d command:%+v", message.CommandIndex, message.Command)

				sm.apply(command)

				if _, isLeader := sm.rf.GetState(); !isLeader {
					continue
				}
				sm.mu.Lock()
				ch := sm.done[message.CommandIndex]
				num := -1
				if command.Op == "Query" {

					num = command.Num
					if num < 0 || num > sm.ConfigsTail().Num {
						num = sm.ConfigsTail().Num
					}

				}

				sm.mu.Unlock()
				go func() {
					ch <- num
				}()

			}
		}
	}
}

func (sm *ShardMaster) rebalance(conf *Config) {
	map_gid2listshard := map[int][]int{0: {}} //
	var gid_order []int                       //为了方便各节点统一顺序遍历map 列出一个gid数组
	for gid := range conf.Groups {
		map_gid2listshard[gid] = []int{}
		gid_order = append(gid_order, gid)
	}
	sort.Ints(gid_order)
	// DPrintf("gid_order: %v", gid_order)
	for i, gid := range conf.Shards {
		map_gid2listshard[gid] = append(map_gid2listshard[gid], i)
	}
	for {
		source, target := GetGIDWithMaximumShards(map_gid2listshard, gid_order), GetGIDWithMinimumShards(map_gid2listshard, gid_order)
		if source == -1 || target == -1 {
			break
		}

		if source != 0 && len(map_gid2listshard[source])-len(map_gid2listshard[target]) <= 1 {
			break
		}
		map_gid2listshard[target] = append(map_gid2listshard[target], map_gid2listshard[source][0])
		map_gid2listshard[source] = map_gid2listshard[source][1:]
	}
	for gid, shards := range map_gid2listshard {
		for _, shard := range shards {
			conf.Shards[shard] = gid
		}
	}

}

// func (sm *ShardMaster) rebalance1(conf *Config) {
// 	ct := map[int][]int{0: {}} // gid -> shard
// 	var gs []int
// 	for gid := range conf.Groups {
// 		ct[gid] = []int{}
// 		gs = append(gs, gid)
// 	}
// 	sort.Ints(gs)
// 	for i, gid := range conf.Shards {
// 		ct[gid] = append(ct[gid], i)
// 	}
// 	// sc.LeaderDebug("%d ct=%v", conf.Num, ct)
// 	for max, maxi, min, mini := maxmin(ct, gs); max-min > 1; max, maxi, min, mini = maxmin(ct, gs) {
// 		c := (max - min) / 2
// 		ts := ct[maxi][0:c]
// 		// sc.LeaderDebug("%d max=%d maxi=%d min=%d mini=%d ts=%v", conf.Num, max, maxi, min, mini, ts)
// 		ct[maxi] = ct[maxi][c:]
// 		ct[mini] = append(ct[mini], ts...)
// 		for _, t := range ts {
// 			conf.Shards[t] = mini
// 		}
// 		// sc.LeaderDebug("%d ct=%v conf.Shards=%v", conf.Num, ct, conf.Shards)
// 	}
// }
func maxmin(ct map[int][]int, gs []int) (max int, maxi int, min int, mini int) {
	max = 0
	min = INT_MAX
	for _, g := range gs {
		l := len(ct[g])
		if l > max {
			max = l
			maxi = g
		}
		if l < min {
			min = l
			mini = g
		}
	}
	if len(ct[0]) > 0 { // empty ct[0] before doing anything else
		if min == INT_MAX {
			return 0, 0, 0, 0
		} else {
			return len(ct[0]) * 2, 0, 0, mini
		}
	} else {
		return max, maxi, min, mini
	}
}

func GetGIDWithMinimumShards(map_gid2listshard map[int][]int, gid_order []int) int {

	// find GID with minimum shards
	index, min := -1, NShards+1
	for _, gid := range gid_order {
		if gid != 0 && len(map_gid2listshard[gid]) < min {
			index, min = gid, len(map_gid2listshard[gid])
		}
	}
	return index
}

func GetGIDWithMaximumShards(map_gid2listshard map[int][]int, gid_order []int) int {
	// always choose gid 0 if there is any  如果gid->0 还存在未分配的shard 优先分配0号
	if shards, ok := map_gid2listshard[0]; ok && len(shards) > 0 {
		return 0
	}

	// find GID with maximum shards
	index, max := -1, -1
	for _, gid := range gid_order {
		if len(map_gid2listshard[gid]) > max {
			index, max = gid, len(map_gid2listshard[gid])
		}
	}
	return index
}
func (sm *ShardMaster) ConfigsTail() Config {
	return sm.configs[len(sm.configs)-1]
}

func (sm *ShardMaster) newconf() Config {
	var conf Config
	conf.Groups = make(map[int][]string)
	conf.Num = sm.ConfigsTail().Num + 1
	for k, v := range sm.ConfigsTail().Groups {
		conf.Groups[k] = v
	}
	for i, s := range sm.ConfigsTail().Shards {
		conf.Shards[i] = s
	}
	return conf
}

const Padding = "    "

func (sm *ShardMaster) Debug(format string, a ...interface{}) {
	quiet := true
	if quiet {
		return
	}
	preamble := strings.Repeat(Padding, sm.me)
	epilogue := strings.Repeat(Padding, sm.serversLen-sm.me-1)
	l := ""
	_, isleader := sm.rf.GetState()
	if isleader {
		l = "L "
	} else {
		return
	}
	prefix := fmt.Sprintf("%s%s S%d %s[ShardMaster] %s", preamble, raft.Microseconds(time.Now()), sm.me, epilogue, l)
	format = prefix + format
	log.Print(fmt.Sprintf(format, a...))
}
