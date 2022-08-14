package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task_state int

const (
	Task_Unallocate Task_state = iota
	Task_Executing
	Task_End
)

type Master struct {
	// Your definitions here.
	nReduce        int          // reduce任务数
	nMap           int          // map任务数
	files          []string     //maptask的输入
	mapfinished    int          // 结束的map task的数量
	maptasklog     []Task_state //维护的maptask日志 0:未加载 1：正在执行 2：执行结束
	reducefinished int
	reducetasklog  []Task_state
	mu             sync.Mutex //master的内容是要被所有worker进程修改共享的 需要锁
}

// Your code here -- RPC handlers for the worker to call.

// maptask结束
func (m *Master) MapTaskFinished(args *ExampleArgs, reply *ExampleReply) error {
	m.mu.Lock()
	m.mapfinished++
	m.maptasklog[args.Id] = Task_End
	m.mu.Unlock()
	return nil
}
func (m *Master) ReduceTaskFinished(args *ExampleArgs, reply *ExampleReply) error {
	m.mu.Lock()
	m.reducefinished++
	m.reducetasklog[args.Id] = Task_End
	m.mu.Unlock()
	return nil
}
func (m *Master) AskTask(args *ExampleArgs, reply *ExampleReply) error {
	/*伪代码：
	1. if mapfinished < nmap  遍历log 如果存在0 则将此task分配 运行 注意心跳检测; 如果不存在 就证明存在正在运行的maptask 则需要等待
	2. if mapfinished == nmap and reducefinished < nreduce  对reducetask 进行同样处理
	3. 都执行完毕 */
	m.mu.Lock()
	if m.mapfinished < m.nMap {
		allocateid := -1
		for i := 0; i < m.nMap; i++ {
			if m.maptasklog[i] == Task_Unallocate { //查询是否存在未加载的maptask
				allocateid = i
				break
			}
		}
		if allocateid == -1 { // 没有未加载的task
			reply.State = State_Is_Waiting
			m.mu.Unlock()
		} else { //存在未加载的task
			reply.Id = allocateid
			reply.State = State_Is_Map //map任务
			reply.Filename = m.files[allocateid]
			reply.Nmap = m.nMap
			reply.Nreduce = m.nReduce
			m.maptasklog[allocateid] = Task_Executing
			// 状态检测 原理是woker RPC调用该函数 启动子进程延时10s 查看日志 如果还没有结束 即认为死亡 所以本质上不是实时监测 而是超时检测
			m.mu.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second)
				m.mu.Lock()
				if m.maptasklog[allocateid] == Task_Executing {
					m.maptasklog[allocateid] = Task_Unallocate
				}
				m.mu.Unlock()

			}()
		}
	} else if m.reducefinished < m.nReduce { // map执行完 reduce未执行完
		allocateid := -1
		for i := 0; i < m.nReduce; i++ {
			if m.reducetasklog[i] == Task_Unallocate { //查询是否存在未加载的maptask
				allocateid = i
				break
			}
		}
		if allocateid == -1 { // 没有未加载的task
			reply.State = State_Is_Waiting
			m.mu.Unlock()
		} else { //存在未加载的task
			reply.Id = allocateid
			reply.State = State_Is_Reduce //reduce任务
			reply.Nmap = m.nMap
			reply.Nreduce = m.nReduce
			m.reducetasklog[allocateid] = Task_Executing
			// 状态检测 原理是woker RPC调用该函数 启动子进程延时10s 查看日志 如果还没有结束 即认为死亡 所以本质上不是实时监测 而是超时检测
			m.mu.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second)
				m.mu.Lock()
				if m.reducetasklog[allocateid] == Task_Executing {
					m.reducetasklog[allocateid] = Task_Unallocate
				}
				m.mu.Unlock()
			}()
		}
	} else { //全部结束
		reply.State = State_Is_Allfinished
		m.mu.Unlock()
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := m.reducefinished == m.nReduce // master执行结束 的判断标准是 所有reduce 任务执行完毕

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	m := Master{}

	// Your code here.
	m.files = files
	m.nMap = len(files)
	m.nReduce = nReduce

	m.maptasklog = make([]Task_state, m.nMap)
	m.reducetasklog = make([]Task_state, m.nReduce)

	m.server()
	return &m
}
