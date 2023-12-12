package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type Task struct {
	TaskType  TaskType
	TaskId    int
	ReduceNum int
	FileName  []string //输入文件
	Status    TaskStatus
}
type TaskType int
type ArgsType int

type TaskStatus int

type TaskArgs struct {
	ArgsType   int
	DoneTaskId int
}
type MiddleFileMap struct {
	TaskId int
	M      map[int][]string
}

const (
	reduceTask = 1
	mapTask    = 2
	exitTask   = 3
)

const (
	Get = iota
	Set
)

// 2 is over ; 1 is running;0 is ready
const (
	Ready = iota
	Running
	Over
	TimeOut
)

//rpc

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
