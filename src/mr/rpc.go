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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Task struct {
	Task_type   TaskType // type of the task
	Id          int      // task Id
	Reducer_num int      // number of reducers
	Filename    []string // input files
}

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask
	ExitTask
)

// empty args
type TaskArgs struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
