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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type GetMapTaskRequest struct {
	Id      int
	ReqName string
}
type GetMapTaskResponse struct {
	TaskInfo *TaskInfo
}

type GetReduceTaskRequest struct {
	Id      int
	ReqName string
}
type GetReduceTaskResponse struct {
	TaskInfo *TaskInfo
}

// WorkFinishedRequest work完成信息信息发送
type WorkFinishedRequest struct {
	Id       int
	TaskType string
	TaskInfo *TaskInfo
}

// WorkFinishedResponse 返回其他的任务信息
type WorkFinishedResponse struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
