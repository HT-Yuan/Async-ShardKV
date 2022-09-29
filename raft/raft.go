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
	"bytes"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//"bytes"

	"../labgob"
	"../labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapValid bool
	SnapData  []byte
	SnapTerm  int
	SnapIndex int
}

type entry struct {
	Index   int         // 索引 之所以不用数组自带的 是因为由于快照的存在 log有一部分要被删除 但index不应该改变
	Command interface{} //命令
	Term    int         //任期
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //候选者记录的任期序号
	CandidateId  int //候选者的ID号
	LastLogIndex int //候选人最后一个日志条目的索引
	LastLogTerm  int //候选者最后一个日志条目的任期
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //candidate 发送投票给自己的请求
	VoteGranted bool //true means candidate received vote
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int //leader's 领导者的任期
	LeaderId     int //leader's ID号
	PrevLogIndex int //紧跟在新日志项之前的日志项索引
	PrevLogTerm  int //prevLog 的任期
	Entries      []entry
	LeaderCommit int //leader's commitIndex

}
type AppendEntriesReply struct {
	// Your data here (2A, 2B).
	Term    int  //用于leader的自我更新
	Success bool //执行结果

	// 关于快速跳过conflict的优化
	XTerm  int //Follower中与Leader冲突的Log对应的任期号  如果没有log 返回-1
	XIndex int //XTerm 有值时： Follower 对应任期号为XTerm的第一条Log条目的index
	XLen   int //XTerm为-1时：Follower 的len(空白log)
	// 增加快照后 会出现prev落后快照点的情况 优化
}
type InstallSnapshotArgs struct {
	Term              int    // leader term
	LeaderId          int    //leader的Id  论文为了重定向 好像目前的策略不需要这个(遍历)
	LastIncludedIndex int    //快照所包含的最后一个日志的索引 为了一致性检查
	LastIncludedTerm  int    //同上
	Data              []byte //
}
type InstallSnapshotReply struct {
	Term int //最高的term
}

// rf 的 STATE 状态
type Rf_state int

const (
	FOLLOWER Rf_state = iota
	CANDIDATE
	LEADER
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// persistent state
	currentTerm int     // 只递增 该server的最新任期  每一个server重启后不能仅从其log处知晓任期情况(大概率滞后) 需要每次currentTerm变动后立即存储
	votedFor    int     // 这是为了防止1个term内出现两个leader 持久化的原因：收到请求后，如果投票给1了，然后故障，如果没有记录votefor 它会认为没投过票，投票给其它。
	logs        []entry //日志

	// volatile state on all server
	commitIndex int //已知要提交的最高日志项的index
	lastApplied int //状态机应用的最高index

	// volatile state on leaders
	nextIndex  []int //对于每台server 要发送到该服务器的 下一个日志项索引
	matchIndex []int //对于每台server ，已知要复制的最高日志项索引

	// 一些额外添加的变量
	state          Rf_state
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	cond           *sync.Cond
	applyCh        chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

/************************************************选举相关******************************************************/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	// DPrintf("[term %d]:Raft[%d] state[%d] send  vote to Raft [%d] args.term[%d]", rf.currentTerm, args.CandidateId, rf.me, args.Term)
	if args.Term < rf.currentTerm { //发出投票请求的候选者任期小于收到的请求的server则拒绝投票
		reply.Term = rf.currentTerm // reply.term 应该是二者最大的 主要用于leader更新任期和状态
		return
	}

	if args.Term > rf.currentTerm { //当收到其它人的更高任期的请求 需要切换

		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()

	}
	reply.Term = args.Term

	// 根据规则 评价谁的日志更新 如果candidate日志更新 则投票
	lastlog := rf.getlastlog()
	if args.LastLogTerm > lastlog.Term || ((args.LastLogTerm == lastlog.Term) && args.LastLogIndex >= lastlog.Index) {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId { // rf.votedFor == args.CandidateId 的含义在于如果Candidate 在发出RPC后 没收到结果 再次重发RPC Follower需要再次投票

			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.state = FOLLOWER
			rf.persist()
			if !rf.electionTimer.Stop() {
				select {
				case <-rf.electionTimer.C: // try to drain the channel
				default:
				}
			}
			rf.electionTimer.Reset(RandomizedElectionTimeout())
		}

	}
	//DPrintf("candateId [%d] argsTerm[%d] replyTerm[%d]", args.CandidateId, args.Term, reply.Term)

}

func (rf *Raft) AttemptElection() {
	rf.mu.Lock()
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getlastlog().Index,
		LastLogTerm:  rf.getlastlog().Term,
	}
	rf.persist()

	// DPrintf("[term %d]: Raft [%d] state [%d] attempting an election", rf.currentTerm, rf.me, rf.state)
	rf.mu.Unlock()

	votesum := 1 //用于统计rf的票数

	for server := range rf.peers {
		if server == rf.me {
			// DPrintf("[term %d]: Raft [%d] state [%d] vote for self", rf.currentTerm, rf.me, rf.state)
			continue
		}
		go func(server int) {
			// voteGranted := rf.callRequestVote(server, term, lastlogidx, lastlogterm, candidateid)

			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 收到过时的RPC 回复时 不作为
			if args.Term != rf.currentTerm || rf.state != CANDIDATE {
				return
			}
			//存在更高级的server时 直接自闭转为跟随者
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = FOLLOWER
				rf.votedFor = -1
				rf.persist()
				return
			}

			// DPrintf("[term %d]: Raft [%d] state [%d] reveive vote form [%d], totally %d", rf.currentTerm, rf.me, rf.state, server, votesum)

			if reply.VoteGranted {
				votesum++
				if votesum*2 > len(rf.peers) {

					rf.state = LEADER

					DPrintf("[term %d]: Raft[%d] is Leader logs:%v", rf.currentTerm, rf.me, rf.logs)
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.getlastlog().Index + 1
						rf.matchIndex[i] = 0
					}
					go rf.BroadcastHeartbeat()     //广播心跳
					go rf.allocateAppendCheckers() //广播日志
					go rf.commitedCheck()          // commitindex更新

					if !rf.heartbeatTimer.Stop() {
						select {
						case <-rf.heartbeatTimer.C: // try to drain the channel
						default:
						}
					}
					rf.heartbeatTimer.Reset(StableHeartbeatTimeout())

				}

			}

		}(server)

	}
}

func RandomizedElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Intn(500)+500) * time.Millisecond
}

// func (rf *Raft) callRequestVote(server int, term int, lastlogidx int, lastlogterm int, cid int) bool {
// 	args := RequestVoteArgs{
// 		Term:         term,
// 		CandidateId:  cid,
// 		LastLogIndex: lastlogidx,
// 		LastLogTerm:  lastlogterm,
// 	}
// 	reply := RequestVoteReply{}
// 	ok := rf.sendRequestVote(server, &args, &reply)

// 	if !ok {
// 		return false
// 	}
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	defer rf.persist()
// 	// 收到过时的RPC 回复时 之间返回false
// 	if term != rf.currentTerm {
// 		return false
// 	}
// 	if reply.Term > rf.currentTerm {
// 		rf.currentTerm = reply.Term
// 		rf.state = FOLLOWER
// 		rf.votedFor = -1
// 	}
// 	return reply.VoteGranted
// }

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok
}

/****************************************************日志相关***************************************************/

func (rf *Raft) allocateAppendCheckers() {

	rf.mu.Lock()
	peer_len := len(rf.peers)
	meid := rf.me
	if rf.killed() || rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	// 对每一个floower 开启goroutine进行日志的发送
	for i := 0; i < peer_len; i++ {
		if i == meid {
			continue
		}

		go rf.appendChecker(i)
	}
}
func (rf *Raft) appendChecker(server int) {
	for {
		rf.mu.Lock()
		if rf.killed() || rf.state != LEADER {
			rf.mu.Unlock()
			return
		}

		lastlogidx := rf.getlastlog().Index //leader日志项的 最后一个索引
		firstlogidx := rf.getfirstlog().Index

		// 这里经常会出错 有人使用nil  我可以限幅 但先试
		nextIndex := rf.nextIndex[server] //leader记录下的 下一个索引
		if nextIndex <= firstlogidx {     // 应用快照
			var installReply InstallSnapshotReply
			installArgs := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: firstlogidx,
				LastIncludedTerm:  rf.getfirstlog().Term,
				Data:              rf.persister.ReadSnapshot(),
			}
			rf.mu.Unlock()

			installOk := rf.sendInstallSnapshot(server, installArgs, &installReply)
			if !installOk {
				continue
			}

			rf.mu.Lock()

			if rf.currentTerm != installArgs.Term {
				rf.mu.Unlock()
				continue
			}
			if installReply.Term > rf.currentTerm {

				rf.state = FOLLOWER
				rf.currentTerm = installReply.Term
				rf.votedFor = -1
				rf.persist()
				rf.mu.Unlock()
				continue
			}
			rf.nextIndex[server] = installArgs.LastIncludedIndex + 1
			rf.mu.Unlock()
			continue
		} else {
			var entries []entry
			for j := nextIndex; j <= rf.getlastlog().Index; j++ {
				atIndex := rf.logs[j-rf.getfirstlog().Index]
				if atIndex.Command == nil {
					panic("Error atIndex.Command == nil")

				}

				entries = append(entries, atIndex)
			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: nextIndex - 1,
				PrevLogTerm:  rf.logs[nextIndex-1-firstlogidx].Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex, //leader's commitIndex
			}

			rf.mu.Unlock()

			if lastlogidx >= nextIndex { //即entries非空

				reply := AppendEntriesReply{}
				// success := rf.callAppendEntries(server, term, prevLogIndex, prevLogTerm, entries, leaderCommit, &reply, leaderid)
				DPrintf("[term %d]:Raft[%d] send real appendEntries to Raft[%d] Entries %v\n", rf.currentTerm, rf.me, server, args.Entries)

				ok := rf.sendAppendEntries(server, &args, &reply)

				if !ok {
					continue
				}
				rf.mu.Lock()

				// defer rf.persist()

				// *** to avoid term confusion !!! ***
				// compare the current term with the term you sent in your original RPC.
				// If the two are different, drop the reply and return
				if args.Term != rf.currentTerm {
					rf.mu.Unlock()
					continue
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					rf.votedFor = -1
					rf.persist()
				} else {
					if reply.Success {

						rf.nextIndex[server] = nextIndex + len(args.Entries)
						rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries) //guide 指出 可防止term confusion

					} else {
						//由于不一致导致的return false 需要快速递减 重试
						// rf.nextIndex[server] = int(math.Max(1.0, float64(rf.nextIndex[server]-1)))
						// DPrintf("[term %d]: Raft[%d] no meet prev same rf.nextIndex[%d]:%d", rf.currentTerm, rf.me, server, rf.nextIndex[server])
						// 快速递减 优化

						if reply.XTerm != -1 { // Xterm有值
							// 需要根据leader有无term的log进行选择
							leder_Xtermlogindex := args.PrevLogIndex

							// leder有term的log
							for ; leder_Xtermlogindex >= firstlogidx; leder_Xtermlogindex-- {
								if rf.logs[leder_Xtermlogindex-firstlogidx].Term == reply.XTerm {
									rf.nextIndex[server] = leder_Xtermlogindex + 1
									break
								}
							}
							// leder无term的log
							if leder_Xtermlogindex < firstlogidx {

								rf.nextIndex[server] = reply.XIndex

							}

							// if reply.XIndex != -1{
							// 	rf.nextIndex[server] =
							// }

						} else { // Xterm无值

							rf.nextIndex[server] = nextIndex - reply.XLen
						}

					}
					DPrintf("[term %d]:Raft[%d] state[%d] rf.nextIndex[%d] update to  %d  reply.Success %t", rf.currentTerm, rf.me, rf.state, server, rf.nextIndex[server], reply.Success)
				}

				/***********************************************************/

				rf.mu.Unlock()
			}

		}

		time.Sleep(time.Millisecond * 10) // 实验指导书的推荐
	}

}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	//DPrintf("[term %d]: Raft[%d] [state %d] receive AppendEntries from Raft[%d] argsCommited %d  rf.commitIndex %d", rf.currentTerm, rf.me, rf.state, args.LeaderId, args.LeaderCommit, rf.commitIndex)

	if rf.currentTerm > args.Term { // follow的任期高于发送心跳或日志更新的leader
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	//接受到来自新的leader 则Candiate 转为 Follower

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER

	}

	// rf.currentTerm = args.Term
	// rf.state = FOLLOWER
	// rf.votedFor = -1
	if rf.state == CANDIDATE { // 如果是候选者 收到了新leader的心跳 则需要切换到follow
		rf.state = FOLLOWER
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm

	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C: // try to drain the channel
		default:
		}
	}
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	// 一致性检验
	// if args.PrevLogIndex > rf.getlastlog().Index || rf.logs[args.PrevLogIndex-rf.getfirstlog().Index].Term != args.PrevLogTerm {
	// 	reply.Success = false

	// 	return
	// }
	// 优化
	if args.PrevLogIndex > rf.getlastlog().Index {
		reply.Success = false
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = args.PrevLogIndex - rf.getlastlog().Index
		return
	}
	if args.PrevLogIndex < rf.getfirstlog().Index {
		// DPrintf("[term %d]:Raft[%d] state[%d] args.PrevLogIndex:%d fistlog:%d", rf.currentTerm, rf.me, rf.state, args.PrevLogIndex, rf.getfirstlog().Index)
		// reply.Success = false
		// return
		reply.Success = false
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = args.PrevLogIndex - rf.getfirstlog().Index
		return
	}

	if rf.logs[args.PrevLogIndex-rf.getfirstlog().Index].Term != args.PrevLogTerm {
		reply.XTerm = rf.logs[args.PrevLogIndex-rf.getfirstlog().Index].Term
		n := args.PrevLogIndex
		// for {
		// 	if n == rf.getfirstlog().Index {
		// 		break
		// 	}
		// 	if rf.logs[n-rf.getfirstlog().Index].Term == reply.XTerm {
		// 		n--
		// 	} else {
		// 		break
		// 	}
		// }
		for {
			if n > rf.getfirstlog().Index {
				n--
				if rf.logs[n-rf.getfirstlog().Index].Term != reply.XTerm {
					break
				}

			} else { //已经是第一个索引
				break
			}
		}
		reply.XIndex = n + 1
		reply.Success = false
		return
	}

	// 通过一致性检验 首先要删除 有冲突的日志项 (从 prevlogindex+1 -> lastindex), 然后entries替代

	if args.PrevLogIndex <= rf.getlastlog().Index {
		rf.logs = rf.logs[:args.PrevLogIndex+1-rf.getfirstlog().Index] //切片左闭右开
	}
	//这是加入新的日志
	rf.logs = append(rf.logs, args.Entries...)
	DPrintf("[term %d]:Raft[%d] state[%d] rf.logs NEW append:%v", rf.currentTerm, rf.me, rf.state, args.Entries)

	reply.Success = true
	// 更新rf.commitIndex
	if args.LeaderCommit > rf.commitIndex {
		// set commitIndex = min(leaderCommit, index of last **new** entry)
		oriCommitIndex := rf.commitIndex
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.getlastlog().Index)))
		if rf.commitIndex > oriCommitIndex {
			// wake up sleeping applyCommit Go routine
			rf.cond.Broadcast()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// func (rf *Raft) callAppendEntries(server int, term int, prevLogIndex int, prevLogTerm int, entries []entry, leaderCommit int, reply *AppendEntriesReply, leaderid int) bool {
// 	args := AppendEntriesArgs{
// 		Term:         term,
// 		LeaderId:     leaderid,
// 		PrevLogIndex: prevLogIndex,
// 		PrevLogTerm:  prevLogTerm,
// 		Entries:      entries,
// 		LeaderCommit: leaderCommit, //leader's commitIndex
// 	}
// 	// reply := AppendEntriesReply{}

// 	ok := rf.sendAppendEntries(server, &args, reply)

// 	if !ok {
// 		return false
// 	}
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	defer rf.persist()

// 	// *** to avoid term confusion !!! ***
// 	// compare the current term with the term you sent in your original RPC.
// 	// If the two are different, drop the reply and return
// 	if term != rf.currentTerm {
// 		return false
// 	}
// 	if reply.Term > rf.currentTerm {
// 		rf.currentTerm = reply.Term
// 		rf.state = FOLLOWER
// 		rf.votedFor = -1
// 	}
// 	return reply.Success
// }

/***************************************************************InstallSnapshot RPC*******************************************/
// leader需要为落后的follower传递快照(因为要发送的log已被discarded)
// 这里当leader的term>=server时 由于此时系统无法进行一些必要的合法判断 比如快照中是有一些新内容 但已被raft传给kv了 只是还未来得及应用
// 所以此时只能无条件接受快照 并异步传递给kv 但kv到底应用于否需要视自身情况 即当接收到snap时 调用函数去检测

func (rf *Raft) InstallSnapshotRPC(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm { //leader的term落后于当前server 则此快照不可用 立即返回
		reply.Term = rf.currentTerm
		return
	}
	// 重置选举超时
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C: // try to drain the channel
		default:
		}
	}
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	if rf.state == CANDIDATE {
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1

	}
	rf.persist()

	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			CommandValid: false,
			SnapValid:    true,
			SnapData:     args.Data,
			SnapTerm:     args.LastIncludedTerm,
			SnapIndex:    args.LastIncludedIndex,
		}
	}()

}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshotRPC", args, reply)
}

func (rf *Raft) SaveStateAndSnapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//先修改log
	if index > rf.getlastlog().Index || index <= rf.getfirstlog().Index {
		DPrintf("[term %d]:Raft[%d] state[%d] SaveStateAndSnapshot Index too long or small", rf.currentTerm, rf.me, rf.state)
		return
	}
	rf.logs = rf.logs[index-rf.getfirstlog().Index:]
	rf.logs[0].Command = nil

	//序列化persist
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.logs)
	e.Encode(rf.votedFor)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	DPrintf("[term %d]:Raft[%d] state[%d] SaveStateAndSnapshot at index[%d]", rf.currentTerm, rf.me, rf.state, index)

}
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("rejected outdated CondInstallSnapshot")
		return false
	}
	//剪辑log
	if lastIncludedIndex >= rf.getlastlog().Index {
		rf.logs = rf.logs[0:1]
	} else {
		rf.logs = rf.logs[lastIncludedIndex-rf.getfirstlog().Index:]
	}
	rf.logs[0].Index = lastIncludedIndex
	rf.logs[0].Term = lastIncludedTerm
	rf.logs[0].Command = nil

	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.logs)
	e.Encode(rf.votedFor)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	DPrintf("[term %d]:Raft[%d] state[%d] CondInstallSnapshot at index[%d]", rf.currentTerm, rf.me, rf.state, lastIncludedIndex)
	return true
}

/**************************************************** Commited********************************************/
// leader周期性更新自己的commitedindex
func (rf *Raft) commitedCheck() {
	for {
		rf.mu.Lock()

		if rf.killed() || rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		lastindex := rf.getlastlog().Index
		prevcommitIndex := rf.commitIndex

		if prevcommitIndex < lastindex {

			for N := prevcommitIndex + 1; N <= lastindex; N++ {
				commitsum := 1 // leader自己
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					if rf.matchIndex[i] >= N {
						commitsum++
					}

				}
				// DPrintf(" leader is commitedCheck, now%d  len%d,rf.logs[N].Term:%d", rf.commitIndex, len(rf.logs)-1, rf.logs[N].Term)
				// fmt.Println(rf.logs)
				firstIndex := rf.getfirstlog().Index
				if N < firstIndex {
					DPrintf("[term %d]:Raft[%d] state[%d] N:%d firstIndex %d logs:%v commitIndex:%d", rf.currentTerm, rf.me, rf.state, N, firstIndex, rf.logs, rf.commitIndex)
				}
				if commitsum*2 > len(rf.peers) && rf.logs[N-firstIndex].Term == rf.currentTerm {
					DPrintf("[term %d]:Raft[%d] state[%d] commit log entry %d successfully ", rf.currentTerm, rf.me, rf.state, N)
					rf.commitIndex = N
					rf.cond.Broadcast()

				}

			}

		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 10) // 实验指导书的推荐
	}
}

// periodically apply log[lastApplied] to state machine
func (rf *Raft) applyCommited() {
	for {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.cond.Wait() //由于rf.commitIndex 会在其它go routine更新 所以使用条件变量
		}
		// rf.lastApplied++
		// //DPrintf("[term %d]:Raft [%d] [state %d] apply log entry %d to the service", rf.currentTerm, rf.me, rf.state, rf.lastApplied)
		// //DPrintLog(rf)
		// cmtidx := rf.lastApplied
		// command := rf.logs[cmtidx].Command
		// rf.mu.Unlock()
		// // commit the log entry
		// msg := ApplyMsg{
		// 	CommandValid: true,
		// 	Command:      command,
		// 	CommandIndex: cmtidx,
		// }
		// // this line may be blocked
		// rf.applyCh <- msg
		firstIndex, commitIndex, lastApplied := rf.getfirstlog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]entry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		index := lastApplied
		rf.mu.Unlock()
		for _, entry := range entries {
			index++
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: index,
			}

		}
		rf.lastApplied = int(math.Max(float64(rf.lastApplied), float64(commitIndex)))
		DPrintf("[term %d]:Raft[%d] state[%d] apply log entry %d to the service successfully", rf.currentTerm, rf.me, rf.state, rf.lastApplied)
	}
}

/****************************************************心跳相关**********************************************/
// 广播心跳
func StableHeartbeatTimeout() time.Duration {
	return time.Duration(100) * time.Millisecond
}

func (rf *Raft) BroadcastHeartbeat() {

	// if the server is dead or is not the leader, just return
	//  这里有点浪费 可以将其与真正的日志叠加融合在一起

	rf.mu.Lock()

	if rf.killed() || rf.state != LEADER {
		rf.mu.Unlock()
		return
	}

	preLogIndex := rf.getlastlog().Index //一致性检查

	// rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: preLogIndex,
		PrevLogTerm:  rf.logs[preLogIndex-rf.getfirstlog().Index].Term,
		Entries:      make([]entry, 0),
		LeaderCommit: rf.commitIndex, //leader's commitIndex
	}
	rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := AppendEntriesReply{}
			//rf.callAppendEntries(server, term, preLogIndex, preLogTerm, make([]entry, 0), leaderCommit, &reply)

			// reply := AppendEntriesReply{}

			ok := rf.sendAppendEntries(server, &args, &reply)

			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// *** to avoid term confusion !!! ***
			// compare the current term with the term you sent in your original RPC.
			// If the two are different, drop the reply and return
			if args.Term != rf.currentTerm {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = FOLLOWER
				rf.votedFor = -1
				rf.persist()
			}

			//success := rf.callAppendEntries(server, term, preLogIndex, preLogTerm, make([]entry, 0), leaderCommit)
			//DPrintf("id: [%d] state:[%d] send heartbeat to server [%d] at term[%d]", rf.me, rf.state, server, rf.currentTerm)

		}(server)
	}
}

//
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
//

//
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
//
/******************************************主要协程 和make 函数（可理解为raft的调用处）************************/
func (rf *Raft) ticker() {
	for rf.killed() == false {

		select {
		case <-rf.electionTimer.C: //
			rf.mu.Lock()
			if rf.state != LEADER {
				go rf.AttemptElection()
			}
			if !rf.electionTimer.Stop() {
				select {
				case <-rf.electionTimer.C: // try to drain the channel
				default:
				}
			}
			rf.electionTimer.Reset(RandomizedElectionTimeout())
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C: // leader 周期性发送心跳
			rf.mu.Lock()
			if rf.state == LEADER {
				go rf.BroadcastHeartbeat()
			}
			if !rf.heartbeatTimer.Stop() {
				select {
				case <-rf.heartbeatTimer.C: // try to drain the channel
				default:
				}
			}
			rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			rf.mu.Unlock()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]entry, 0),
		state:          FOLLOWER,
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()), //推荐用 time.sleep()

		applyCh: applyCh,
	}
	rf.cond = sync.NewCond(&rf.mu)
	rf.logs = append(rf.logs, entry{Index: 0, Term: 0, Command: nil}) // log 真实日志的索引从1开始 这里插入了一个空日志

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()        //协程 负责开始选举+心跳
	go rf.applyCommited() //单独协程 在apply上运行commited
	return rf
}

/**************************************************持久化相关********************************************/
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.logs)
	e.Encode(rf.votedFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentterm int
	var votedFor int
	var logs []entry
	if d.Decode(&currentterm) != nil ||
		d.Decode(&logs) != nil || d.Decode(&votedFor) != nil {
		DPrintf("readPersist Decoden Error")
	} else {
		rf.mu.Lock()
		rf.currentTerm = currentterm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.commitIndex = rf.logs[0].Index
		rf.lastApplied = rf.logs[0].Index
		rf.mu.Unlock()
	}
}
func (rf *Raft) GetstateSize() int {
	return rf.persister.RaftStateSize()
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//

/***********************************************其它 如读取状态 日志入口等*******************************/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1

	if rf.state != LEADER || rf.killed() {
		return index, term, false
	}

	// index = len(rf.logs)

	index = rf.getlastlog().Index + 1
	term = rf.currentTerm
	rf.logs = append(rf.logs, entry{Index: index, Term: term, Command: command})
	rf.persist()
	DPrintf("[term %d]:Raft[%d] state[%d] reveive logs at %d ", rf.currentTerm, rf.me, rf.state, index)

	return index, term, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	rf.mu.Unlock()
	return term, isleader
}
func (rf *Raft) getlastlog() entry {
	index := len(rf.logs)
	return rf.logs[index-1]
}
func (rf *Raft) getfirstlog() entry {

	return rf.logs[0]
}
