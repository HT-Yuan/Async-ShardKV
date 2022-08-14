package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type Worker_State int

const (
	State_Is_Map Worker_State = iota
	State_Is_Reduce
	State_Is_Waiting
	State_Is_Allfinished
)

type ExampleArgs struct {
	Id int //任务序列号
}

type ExampleReply struct {
	State    Worker_State // 0等待 1job结束 2 map 3 reduce
	Id       int          //如果分配任务 则任务的id号
	Filename string       // 仅在maptask中使用
	Nreduce  int          // reduce task任务数
	Nmap     int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.

func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
