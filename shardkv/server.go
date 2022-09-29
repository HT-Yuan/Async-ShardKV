package shardkv

// import "../shardmaster"
import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key       string
	Value     string
	Operation string
	Clientid  int64
	Cmdindex  int

	Source_gid int //源gid
	Source_me  int //源me

	NewConfig    shardmaster.Config
	Transfer     TransferArgs
	TransferDone TransferDoneArgs
}

const (
	Debug                = 1
	TimeoutInterval      = 500 * time.Millisecond
	UpdateConfigInterval = 100 * time.Millisecond
	DoPollInterval       = 200 * time.Millisecond
	GETOp                = "Get"
	PUTOp                = "Put"
	APPENDOp             = "Append"
	NewConfigOp          = "Newconfig"
	TransferOp           = "Transfer"
	TransferDoneOp       = "TransferDone"
)

type ShardState int
type Done chan GetReply

const (
	Serving ShardState = iota
	Pulling
	Pushing
)

type Transfer struct {
	body    TransferArgs
	target  int      //目标gid
	servers []string //该集群的server序列
}

type TransferArgs struct {
	// ClientId    int               //cid Int
	// SequenceNum int               // 发送RPC的序列号
	Num        int               // config.Num
	Source_gid int               // 源gid
	Shards     []int             // 移交的分片id号
	Kv         map[string]string //slice 要移交的kv片
	Dedup      map[int64]int     //源gid的去重表
}
type TransferReply struct {
	Err Err
}
type TransferDoneArgs struct {
	Num      int      // config.Num
	Receiver int      // 收到config 并完成更改的gid
	Keys     []string // 源gid 需要根据key进行删除
	Shards   []int    // 移交的分片id号
}
type TransferDoneReply struct {
	Err Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int   // snapshot if log grows this big
	dead         int32 // set by Kill()

	sm         *shardmaster.Clerk // server需要轮询sm 以便进行负载均衡 查询数据等操作
	config     shardmaster.Config
	lastconfig shardmaster.Config //用以计算diff
	groups     map[int][]string   // gid -> servers[]
	serversLen int

	// Your definitions here.
	// stateMachine KVStateMachine
	kv map[string]string

	//command 结束操作后，通知上层kv 采用channel的方式进行同步

	done   map[int]Done
	doneMu sync.Mutex

	//防止重复性写 这里简化了 一个client不会并发的发出请求
	lastoperation map[int64]int //上一次得到成功应用的 clientid与cmdindex的映射 这是为了避免写操作的重传 而读操作幂等
	lastapplied   int           // 得到应用的comand在log上的index

	shardStates [shardmaster.NShards]ShardState //每个server维护其负责分片的状态 不负责的为正常状态

	transerfCh chan Transfer //apply config后  单独协程采集通道信息sendRPC，优势在于防阻塞、无锁。
	// seqnum     int           //rpc去重
	lasttransfered map[int]int //transfer gid->config.num
}

func (kv *ShardKV) checkKey(key string) Err {

	shard := key2shard(key)

	if kv.config.Shards[shard] == kv.gid {

		if kv.shardStates[shard] == Serving {
			return OK
		}
		return ErrTimeout //该分片正在工作 不可提供服务
	}
	return ErrWrongGroup
}
func (kv *ShardKV) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}
func (kv *ShardKV) copyDup() map[int64]int {
	dup := make(map[int64]int)
	for cid, cmd := range kv.lastoperation {
		dup[cid] = cmd
	}
	return dup
}

/***************************************************上层交互*****************************************/
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// 读有多种写法：LogRead/ReadIndex(心跳、空日志)/LeaseRead(租约、空日志)/FollowerRead(wait同步)
	// 如果写请求在应用后返回结果 则ReadIndex LeaseRead 可wait-free, 因为原则上空日志应用了原leader应用的log
	command := Op{Key: args.Key, Value: "", Operation: GETOp}
	v, err := kv.Command(args.Key, command)
	reply.Err = err
	if err == OK {
		reply.Value = v
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	command := Op{Key: args.Key, Value: args.Value, Operation: args.Op, Clientid: args.ClientId, Cmdindex: args.CommandId}
	_, reply.Err = kv.Command(args.Key, command)
}

// Command args needs to be raw type (not pointer)
func (kv *ShardKV) Command(key string, op Op) (val string, err Err) {

	if op.Operation == GETOp || op.Operation == PUTOp || op.Operation == APPENDOp {
		if !kv.isLeader() {
			return "", ErrWrongLeader
		}
		if err := kv.checkKey(key); err != OK {

			return "", err
		}
	}

	i, _, isLeader := kv.rf.Start(op)
	kv.Debug("raft start %s i=%d %+v  config: %+v states=%v", op.Operation, i, op, kv.config, kv.shardStates)

	if !isLeader {
		return "", ErrWrongLeader
	}
	ch := make(Done, 1)

	kv.doneMu.Lock()
	kv.done[i] = ch
	kv.doneMu.Unlock()

	select {
	case reply := <-ch:
		kv.Debug("raft %s done => %v  cmd: %+v ", op.Operation, reply, op)
		return reply.Value, reply.Err
	case <-time.After(TimeoutInterval):
		return "", ErrTimeout
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sm = shardmaster.MakeClerk(masters)
	kv.serversLen = len(servers)

	kv.kv = make(map[string]string)

	kv.groups = make(map[int][]string) // gid->[]servers 的映射
	kv.done = make(map[int]Done)
	kv.lastoperation = make(map[int64]int)
	kv.lasttransfered = make(map[int]int) //去重

	kv.transerfCh = make(chan Transfer, shardmaster.NShards)
	kv.readSnapshot(persister.ReadSnapshot())
	kv.resumeTransfer()
	kv.Debug("StartServer  config=%v shardStates=%v  lasttransfered=%v kv=%v", kv.config, kv.shardStates, kv.lasttransfered, kv.kv)

	// You may need initialization code here.
	go kv.applier()
	go kv.DoUpdateConfig()
	go kv.DoTranPoll()

	return kv

}

/***************************************************************对接raft层 commit的op进行apply[Command\ Config]*************************************/
func (kv *ShardKV) applier() {
	for !kv.killed() {
		message := <-kv.applyCh //阻塞
		// DPrintf("[server %d] tries to apply message %v", kv.me, message)
		if message.CommandValid { // 非快照类型的log
			// if message.Command == nil {
			// 	continue
			// }
			command := message.Command.(Op)
			//apply 日志时需要防止状态机回退： follower 对于 leader 发来的 snapshot 和本地 commit 的多条日志
			// 在向 applyCh 中 push 时无法保证原子性，可能会有 snapshot 夹杂在多条 commit 的日志中
			if message.CommandIndex <= kv.lastapplied {
				kv.Debug("reject Command due to <= Index. lastApplied=%d latest=%+v", kv.lastapplied, command)
				// DPrintf("{Node %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.me, message, kv.lastapplied)
				continue
			}
			kv.Debug("message.Index %d command:%+v", message.CommandIndex, message.Command)
			if command.Operation == NewConfigOp {

				kv.applyConfig(command, message.CommandIndex)
			} else {
				// DPrintf("gid:%d [me:%d] applyMsg:%v", kv.gid, kv.me, command)
				val, err := kv.applyMsg(command)
				if kv.isLeader() {
					kv.doneMu.Lock()
					ch := kv.done[message.CommandIndex]
					kv.doneMu.Unlock()
					if ch != nil {
						ch <- GetReply{err, val}
					}
				}
			}
			kv.lastapplied = message.CommandIndex

			// 检查 rf.log是否达到快照标准
			if kv.rf.GetstateSize() >= kv.maxraftstate && kv.maxraftstate != -1 {
				kv.Debug("checkSnapshot: kv.rf.GetStateSize(%d) >= kv.maxraftstate(%d)", kv.rf.GetstateSize(), kv.maxraftstate)
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.lastoperation)

				e.Encode(kv.kv)
				if err := e.Encode(kv.shardStates); err != nil {
					panic(err)
				}
				if err := e.Encode(kv.lastconfig); err != nil {
					panic(err)
				}
				if err := e.Encode(kv.config); err != nil {
					panic(err)
				}
				if err := e.Encode(kv.groups); err != nil {
					panic(err)
				}
				data := w.Bytes()

				kv.rf.SaveStateAndSnapshot(kv.lastapplied, data)

			}

		} else { //raft层传递过来的是快照  这里应当判断快照是否先进于当前？
			success := kv.rf.CondInstallSnapshot(message.SnapTerm, message.SnapIndex, message.SnapData)
			kv.Debug("CondInstallSnapshot %t SnapshotTerm=%d SnapshotIndex=%d ", success, message.SnapTerm, message.SnapIndex)
			// DPrintf("CondInstallSnapshot ")
			if success {
				kv.lastapplied = message.SnapIndex
				kv.readSnapshot(message.SnapData)
			}
		}

	}
}

func (kv *ShardKV) applyMsg(command Op) (string, Err) {
	var key string
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if command.Operation == GETOp || command.Operation == PUTOp || command.Operation == APPENDOp {
		key = command.Key
		if err := kv.checkKey(key); err != OK {
			// DPrintf("reject ApplyMsg due to failed checkKey=%s", err)
			kv.Debug("reject ApplyMsg due to failed checkKeyL=%s  v=%+v", err, command)
			return "", err
		}

	}

	switch command.Operation {
	case GETOp:
		return kv.kv[key], OK
	case PUTOp:
		if v, ok := kv.lastoperation[command.Clientid]; ok && v >= command.Cmdindex {
			// DPrintf("lastoperation put %d %d", command.Clientid, command.Cmdindex)
			kv.Debug("PUTOp duplicate found for %d  args=%+v", v, command)

		} else {
			kv.kv[key] = command.Value
			kv.lastoperation[command.Clientid] = command.Cmdindex
			kv.Debug("applied PUTOp => %s  args: {%d %+v} shardstate:%v config: %+v", kv.kv[key], command.Cmdindex, command, kv.shardStates, kv.config)
		}
		return "", OK

	case APPENDOp:
		if v, ok := kv.lastoperation[command.Clientid]; ok && v >= command.Cmdindex {
			// DPrintf("lastoperation append  %d %d", command.Clientid, command.Cmdindex)
			kv.Debug("APPENDOp duplicate found for %d  args=%+v  kv.lastoperation:%v ", v, command, kv.lastoperation)

		} else {

			kv.kv[key] += command.Value

			kv.Debug("applied APPENDOp => %s  args: {%d %+v} config: %+v kv.lastoperation:%v ", kv.kv[key], command.Cmdindex, command, kv.config, kv.lastoperation)
			kv.lastoperation[command.Clientid] = command.Cmdindex
		}
		return "", OK
	case TransferOp: //完成转换  必须在config的版本号一致时转换
		if command.Transfer.Num > kv.config.Num {
			kv.Debug("reject TransferOp due to command.Transfer.Num %d > kv.config.Num %d. current=%+v args=%+v", command.Transfer.Num, kv.config.Num, kv.config, command.Transfer)
			return "", ErrTimeout
		}
		if command.Transfer.Num < kv.config.Num {
			kv.Debug("reject TransferOp due to command.Transfer.Num %d < kv.config.Num %d. current=%+v args=%+v", command.Transfer.Num, kv.config.Num, kv.config, command.Transfer)
			return "", OK
		}

		if v, ok := kv.lasttransfered[command.Transfer.Source_gid]; !ok || (ok && command.Transfer.Num > v) {
			allServing := true
			for _, shard := range command.Transfer.Shards {
				if kv.shardStates[shard] != Serving {
					allServing = false
				}
			}
			if allServing {
				kv.Debug("ignore TransferOp due to allServing. current=%+v args=%+v", kv.config, command.Transfer)
				return "", OK
			}
			for k, v := range command.Transfer.Kv {
				kv.kv[k] = v
			}
			for _, shard := range command.Transfer.Shards {
				kv.shardStates[shard] = Serving
			}

			kv.lasttransfered[command.Transfer.Source_gid] = command.Transfer.Num
			// 去重表需要合并
			for clientid, cmdindex := range command.Transfer.Dedup {

				if cmdindex >= kv.lastoperation[clientid] {
					kv.lastoperation[clientid] = cmdindex
				}
			}

			kv.Debug("applied TransferOp from gid %d => %v states: %v  args: %v", command.Transfer.Source_gid, kv.config, kv.shardStates, command.Transfer)
			return "", OK
		}
		kv.Debug("TransferOp duplicate found for %d  args=%+v", kv.lasttransfered[command.Transfer.Source_gid], command.Transfer)

		return "", OK

	case TransferDoneOp:
		if command.TransferDone.Num > kv.config.Num {
			kv.Debug("reject tansferDoneArgs due to command.TransferDone.Num %d > kv.config.Num %d. current=%+v args=%+v", command.TransferDone.Num, kv.config.Num, kv.config, command.TransferDone)
			return "", ErrTimeout
		}
		if command.TransferDone.Num < kv.config.Num {
			kv.Debug("ignore tansferDoneArgs due to command.TransferDone.Num %d < kv.config.Num %d. current=%+v args=%+v", command.TransferDone.Num, kv.config.Num, kv.config, command.TransferDone)
			return "", OK
		}
		// allServing := true
		// for _, shard := range command.Transfer.Shards {
		// 	if kv.shardStates[shard] != Serving {
		// 		allServing = false
		// 	}
		// }
		// if allServing {
		// 	kv.Debug("ignore tansferDoneArgs due to allServing command=%+v", command.TransferDone)
		// 	return "", OK
		// }
		for _, k := range command.TransferDone.Keys {
			delete(kv.kv, k)
		}
		for _, shard := range command.TransferDone.Shards {
			kv.shardStates[shard] = Serving
		}
		kv.Debug("tansferDone %v to %d done  states=%v", command.TransferDone.Shards, command.TransferDone.Receiver, kv.shardStates)
		return "", OK
	}
	return "", OK

}

/*********************************************************快照相关**************************************/
func (kv *ShardKV) readSnapshot(snapshot []byte) {
	//没有锁 调用时注意

	var kvmap map[string]string
	var lastop map[int64]int
	var lastconfig shardmaster.Config
	var config shardmaster.Config
	var shard [shardmaster.NShards]ShardState
	var gs map[int][]string
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if e := d.Decode(&lastop); e != nil {
		lastop = make(map[int64]int)
	}
	if e := d.Decode(&kvmap); e != nil {
		kvmap = make(map[string]string)
	}
	if e := d.Decode(&shard); e == nil {
		kv.shardStates = shard
	}
	if e := d.Decode(&lastconfig); e == nil {
		kv.lastconfig = lastconfig
	}
	if e := d.Decode(&config); e == nil {
		kv.config = config
	}
	if e := d.Decode(&gs); e == nil {
		kv.groups = gs
	}
	kv.kv = kvmap
	kv.lastoperation = lastop
}

/**************************************************config相关*************************************************/

func (kv *ShardKV) DoUpdateConfig() {
updateConfig:
	for !kv.killed() {
		time.Sleep(UpdateConfigInterval)
		if !kv.isLeader() {
			continue
		}
		kv.mu.Lock()

		for _, state := range kv.shardStates {
			if state != Serving {
				kv.mu.Unlock()
				continue updateConfig
			}
		}
		lastConfigNum := kv.config.Num
		// kv.Debug("DoUpdateConfig states=%v currentConf=%v", kv.shardStates, kv.config)
		kv.mu.Unlock()
		newestConfig := kv.sm.Query(lastConfigNum + 1)
		// DPrintf("gid:%d[%d] Got a new Config%v", kv.gid, kv.me, newestConfig)
		if newestConfig.Num == lastConfigNum+1 {
			// Got a new Config
			op := Op{Operation: NewConfigOp, NewConfig: newestConfig, Source_gid: kv.gid, Source_me: kv.me}
			kv.Debug("DoUpdateConfig states=%v currentconf.num : %d -> nextnum :%d currentConf=%v nextconf:%v", kv.shardStates, kv.config.Num, newestConfig.Num, kv.config, newestConfig)
			// DPrintf("gid:%d [me:%d] Got a new Config%v", kv.gid, kv.me, op.NewConfig.Shards)
			kv.rf.Start(op)
		}

	}

}
func (kv *ShardKV) applyConfig(command Op, cmdindex int) {
	// 计算diff
	// 该gid中途 掉线后 重新上线后 首先会重播log 同时新leader也会从0开始query 为避免冲突 此处需要作此限制
	if command.NewConfig.Num <= kv.config.Num {
		return
	}

	kv.mu.Lock()
	kv.lastconfig = kv.config
	kv.config = command.NewConfig

	// DPrintf("[%d] shards is %v", kv.gid, kv.config.Shards)
	// 更新raft集群成员
	for gid, servers := range command.NewConfig.Groups {
		kv.groups[gid] = servers //各gid 对应的 raft集群 servers
	}
	// 计算diff 构建需要push的map  target_gid-->[]shard
	kv.Debug("applying Config cmdindex:%d currentnum:%d nextnum:%d states=%v", cmdindex, kv.lastconfig.Num, kv.config.Num, kv.shardStates)
	diff := make(map[int][]int)

	// 对shard 进行遍历 捕捉shard 匹配的gid的前后变化
	for shard, source_gid := range kv.lastconfig.Shards { //source 源shard 源gid
		target_gid := kv.config.Shards[shard]             //目标gid
		if source_gid == kv.gid && target_gid != kv.gid { // 原来是我的 现在不是我的 需要push
			diff[target_gid] = append(diff[target_gid], shard)
			kv.shardStates[shard] = Pushing
		} else if source_gid != 0 && source_gid != kv.gid && target_gid == kv.gid { // 原来不是我的 现在变成我的 需要pull
			kv.shardStates[shard] = Pulling
		}
	}

	kv.mu.Unlock()
	if kv.isLeader() {
		kv.Debug("applied Config  diff= %+v lastConfig=%+v latest=%+v updatedStates=%v", diff, kv.lastconfig, kv.config, kv.shardStates)
		kv.totransfer(diff, command.NewConfig)
	}

}

/**************************************************Transfer相关*************************************************/
func (kv *ShardKV) resumeTransfer() {
	diff := make(map[int][]int) // gid -> shards
	for shard, gid := range kv.lastconfig.Shards {
		if kv.shardStates[shard] == Pushing {
			target := kv.config.Shards[shard]
			if gid == kv.gid && target != kv.gid { // move from self to others
				diff[target] = append(diff[target], shard)
			}
		}
	}
	kv.Debug("resumed diff: %v", diff)
	kv.totransfer(diff, kv.config)
}
func (kv *ShardKV) totransfer(diff map[int][]int, latest shardmaster.Config) {
	for gid, shards := range diff {
		slice := make(map[string]string)
		for key := range kv.kv {

			for _, shard := range shards {
				// fmt.Println(key, key2shard(key), shard)
				if key2shard(key) == shard {
					slice[key] = kv.kv[key]
				}
			}
		}
		kv.Debug("totransfer shards %v to gid %d: slice:%v kv.kv:%+v", shards, gid, slice, kv.kv)

		if servers, ok := latest.Groups[gid]; ok {
			// DPrintf("gid:%d [me:%d] transfer [%v] to [%d]", kv.gid, kv.me, shards, gid)
			kv.transerfCh <- Transfer{TransferArgs{Num: latest.Num, Source_gid: kv.gid, Shards: shards, Kv: slice, Dedup: kv.copyDup()}, gid, servers}
		} else {
			panic("no group to transfer")
		}
	}
}
func (kv *ShardKV) DoTranPoll() {
	for transfer := range kv.transerfCh {
		// transerf.body.ClientId = kv.gid
		// transerf.body.SequenceNum = kv.seqnum

	nextTransfer:
		for {

			for _, server := range transfer.servers { //目标gid 的集群列表
				var reply TransferReply
				ok := kv.sendTransfer(server, &transfer.body, &reply)
				kv.Debug("tansfer to target %d %s ok=%t reply.Err=%s  args=%+v\n", transfer.target, server, ok, reply.Err, transfer.body)

				if ok && reply.Err == OK {
					// kv.seqnum++
					break nextTransfer
				}
				if ok && reply.Err == ErrWrongGroup {
					panic("DoTranPoll reply.Err == ErrWrongGroup")
				}
			}
			time.Sleep(DoPollInterval)

		}

	}
}

// 将tansfer后的响应通过RPC发回给source 以供其更改状态以及更改状态
func (kv *ShardKV) DoTranDonePoll(args TransferDoneArgs, source_gid int) {
	for {
		kv.mu.Lock()
		servers := kv.groups[source_gid]
		kv.mu.Unlock()
		if len(servers) <= 0 {
			panic("no servers to TransferDone")
		}
		for _, server := range servers {
			var reply TransferDoneReply
			ok := kv.sendTransferDone(server, &args, &reply)
			kv.Debug("TransferDone to origin %d %s ok=%t reply.Err=%s  args=%v+\n", source_gid, server, ok, reply.Err, args)
			if ok && reply.Err == OK {
				return
			}
			if ok && reply.Err == ErrWrongGroup {
				panic("Transfer reply.Err == ErrWrongGroup")
			}
		}
		time.Sleep(DoPollInterval)
	}
}

//*******************************************************configRPC相关**************************************************//
func (kv *ShardKV) sendTransfer(server string, args *TransferArgs, reply *TransferReply) bool {
	return kv.make_end(server).Call("ShardKV.Transfer", args, reply)
}
func (kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) {

	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}

	v, ok := kv.lasttransfered[args.Source_gid]
	if !ok || args.Num > v {
		command := Op{Operation: TransferOp, Transfer: *args}
		_, reply.Err = kv.Command("", command)
	} else {
		reply.Err = OK
	}
	if reply.Err == OK { //重发 或者 有效
		var doneArgs TransferDoneArgs
		doneArgs.Receiver = kv.gid
		doneArgs.Num = args.Num
		doneArgs.Shards = args.Shards
		for key := range args.Kv {
			doneArgs.Keys = append(doneArgs.Keys, key)
		}
		go kv.DoTranDonePoll(doneArgs, args.Source_gid)
	}

}
func (kv *ShardKV) sendTransferDone(server string, args *TransferDoneArgs, reply *TransferDoneReply) bool {
	return kv.make_end(server).Call("ShardKV.TransferDone", args, reply)
}
func (kv *ShardKV) TransferDone(args *TransferDoneArgs, reply *TransferDoneReply) {
	if !kv.isLeader() {
		reply.Err = ErrWrongLeader
		return
	}
	command := Op{Operation: TransferDoneOp, TransferDone: *args}
	_, reply.Err = kv.Command("", command)

}

//*************************************************************Debug******************************************************//

const Padding = "    "

func (kv *ShardKV) Debug(format string, a ...interface{}) {
	quiet := true
	if quiet {
		return
	}
	preamble := strings.Repeat(Padding, kv.me)
	epilogue := strings.Repeat(Padding, kv.serversLen-kv.me-1)
	l := ""
	if kv.isLeader() {
		l = "L "
	} else {
		return
	}
	prefix := fmt.Sprintf("%s%s S%d %s[SHARDKV] %s%d ", preamble, raft.Microseconds(time.Now()), kv.me, epilogue, l, kv.gid)
	format = prefix + format
	log.Print(fmt.Sprintf(format, a...))
}

// func init() {
// 	v := os.Getenv("SHARDKV_VERBOSE")
// 	level := 1
// 	if v != "" {
// 		level, _ = strconv.Atoi(v)
// 	}
// 	if level < 1 {
// 		log.SetOutput(ioutil.Discard)
// 	}
// 	_, quiet = os.LookupEnv("SHARDCTL_QUIET")
// }
