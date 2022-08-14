package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const TimeoutInterval = 700 * time.Millisecond

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Operation string
	Clientid  int64
	Cmdindex  int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg // raft与kv间传递command的通道
	dead    int32              // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// stateMachine KVStateMachine
	kv map[string]string

	//command 结束操作后，通知上层kv 采用channel的方式进行同步
	getnotifychan map[int]chan string
	putnotifychan map[int]chan struct{}

	//防止重复性写 这里简化了 一个client不会并发的发出请求
	lastoperation map[int64]int //上一次得到成功应用的 clientid与cmdindex的映射 这是为了避免写操作的重传 而读操作幂等
	lastapplied   int           // 得到应用的comand在log上的index
	// Your definitions here.
}

/*******************************************************Client 与 KVserver交互************************************************************************/
// 通过RPC 调用raft上的start代码实现GET APPEND  这是个只读的op 但由于强一致性的存在 也应该发送到leader的
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{Key: args.Key, Value: "", Operation: "Get"}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan string, 1)
	kv.mu.Lock()
	kv.getnotifychan[index] = ch
	kv.mu.Unlock()
	select {
	case val := <-ch: //无值 仅用作同步
		reply.Value = val
		reply.Err = OK
		return
	case <-time.After(TimeoutInterval):

		reply.Err = ErrTimeout

		return
	}
	go func() {
		kv.mu.Lock()
		close(kv.getnotifychan[index])
		kv.mu.Unlock()
	}()

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	command := Op{Key: args.Key, Value: args.Value, Operation: args.Op, Clientid: args.ClientId, Cmdindex: args.CommandId}

	index, _, isLeader := kv.rf.Start(command)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := make(chan struct{}, 1)
	kv.mu.Lock()
	kv.putnotifychan[index] = ch
	kv.mu.Unlock()
	select {
	case <-ch: //无值 仅用作同步
		reply.Err = OK
		return
	case <-time.After(TimeoutInterval):

		reply.Err = ErrTimeout

		return
	}
	go func() {
		kv.mu.Lock()
		close(kv.putnotifychan[index])
		kv.mu.Unlock()
	}()

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	DPrintf("Kill %d", kv.me)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// 当编解码中有一个字段是

	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// kv.stateMachine = NewMemoryKV()

	// fmt.Println(kv.stateMachine)
	// kv.memoryKV = NewMemoryKV()

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.getnotifychan = make(map[int]chan string)
	kv.putnotifychan = make(map[int]chan struct{})
	kv.lastoperation = make(map[int64]int)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.readSnapshot(persister.ReadSnapshot())

	// You may need initialization code here.
	go kv.applier()

	return kv
}

// 这是server 需要读取applych 并应用
func (kv *KVServer) applier() {
	for kv.killed() == false {

		select {
		case message := <-kv.applyCh:
			// DPrintf("[server %d] tries to apply message %v", kv.me, message)
			if message.CommandValid { // 对 message.Command 进行解析
				// if message.Command == nil {
				// 	continue
				// }
				command := message.Command.(Op)
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastapplied {
					DPrintf("{Node %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.me, message, kv.lastapplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastapplied = message.CommandIndex
				_, isLeader := kv.rf.GetState()

				if command.Operation == "Get" {
					getCh := kv.getnotifychan[message.CommandIndex]
					val := ""
					// s, err := kv.stateMachine.Get(command.Key)
					if s, ok := kv.kv[command.Key]; ok {
						val = s
					}

					// if err == OK {
					// 	val = s

					// }

					if isLeader {
						go func() {
							getCh <- val
						}()
					}

				} else if command.Operation == "Put" {
					putCh := kv.putnotifychan[message.CommandIndex]
					if v, ok := kv.lastoperation[command.Clientid]; ok && v == command.Cmdindex {
						DPrintf("lastoperation put %d %d", command.Clientid, command.Cmdindex)

						if isLeader {
							go func() {
								putCh <- struct{}{}
							}()
						}

					} else {
						// err := kv.stateMachine.Put(command.Key, command.Value)
						kv.kv[command.Key] = command.Value
						err := OK

						if err == OK {
							kv.lastoperation[command.Clientid] = command.Cmdindex

							if isLeader {
								go func() {
									putCh <- struct{}{}
								}()
							}
						}
					}

				} else if command.Operation == "Append" {
					putCh := kv.putnotifychan[message.CommandIndex]
					if v, ok := kv.lastoperation[command.Clientid]; ok && v == command.Cmdindex {
						DPrintf("lastoperation put %d %d", command.Clientid, command.Cmdindex)

						if isLeader {
							go func() {
								putCh <- struct{}{}
							}()
						}

					} else {
						// DPrintf("two Append %d %d %v", message.CommandIndex, command.Cmdindex, command.Value)
						// err := kv.stateMachine.Append(command.Key, command.Value)

						kv.kv[command.Key] += command.Value
						err := OK
						if err == OK {
							kv.lastoperation[command.Clientid] = command.Cmdindex

							if isLeader {
								go func() {
									putCh <- struct{}{}
								}()
							}
						}
					}

				}
				// //检查 rf.log是否达到快照标准
				if kv.rf.GetstateSize() >= kv.maxraftstate && kv.maxraftstate != -1 {
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					e.Encode(kv.lastoperation)

					e.Encode(kv.kv)
					data := w.Bytes()

					kv.rf.SaveStateAndSnapshot(kv.lastapplied, data)

				}
				kv.mu.Unlock()

			} else { //raft层传递过来的是快照  这里应当判断快照是否先进于当前？
				b := kv.rf.CondInstallSnapshot(message.SnapTerm, message.SnapIndex, message.SnapData)

				DPrintf("CondInstallSnapshot ")
				if b {
					kv.lastapplied = message.SnapIndex
					kv.readSnapshot(message.SnapData)
				}
			}

		}
	}
}

/************************************************************状态机 抽象 接口*************************************************************/

// type KVStateMachine interface {
// 	Get(key string) (string, Err)
// 	Put(key, value string) Err
// 	Append(key, value string) Err
// }

// type MemoryKV struct {
// 	KV map[string]string
// }

// func NewMemoryKV() *MemoryKV {
// 	return &MemoryKV{make(map[string]string)}
// }

// func (memoryKV *MemoryKV) Get(key string) (string, Err) {

// 	if value, ok := memoryKV.KV[key]; ok {
// 		DPrintf("Get value : %v  %t", memoryKV.KV[key], ok)
// 		return value, OK
// 	}
// 	DPrintf("Get value : key: %v value:%v  %v", key, memoryKV.KV[key], ErrNoKey)
// 	return "", ErrNoKey
// }

// func (memoryKV *MemoryKV) Put(key, value string) Err {
// 	// DPrintf("Put value : %v", value)
// 	memoryKV.KV[key] = value
// 	return OK
// }

// func (memoryKV *MemoryKV) Append(key, value string) Err {

// 	memoryKV.KV[key] += value
// 	DPrintf("Append value : %v", memoryKV.KV[key])
// 	return OK
// }

/*********************************************************快照相关**************************************/
func (kv *KVServer) readSnapshot(snapshot []byte) {
	//没有锁 调用时注意

	var kvmap map[string]string
	var lastop map[int64]int
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if e := d.Decode(&lastop); e != nil {
		lastop = make(map[int64]int)
	}
	if e := d.Decode(&kvmap); e != nil {
		kvmap = make(map[string]string)
	}
	kv.kv = kvmap
	kv.lastoperation = lastop
}
