package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// 用于通知coordinator的退出，有效防止数据竞争
var ch = make(chan bool)

// 任务状态
type Status int

const (
	TaskNoProcess Status = 0 // 未下发
	TaskInProcess Status = 1 // 任务正在处理
	TaskFinished  Status = 2 // 任务已完成
)

type err string

const (
	MapTaskFinished     err = "101"
	ReduceTaskFinished  err = "102"
	AllTaskSendFinished err = "103" // 搜所有的任务已经下发完成
)

type Name string // 任务名
// TaskInfo 需要分配的任务信息
type TaskInfo struct {
	TaskId        int    // 任务id
	TaskName      Name   // 任务名， 具体任务，map任务
	TaskCurStatus Status // 具体任务当前的状态
	NReduce       int    // 用于map任务文件名处理
	NMap          int    // 用于reduce任务文件名处理
}

type Coordinator struct {
	// Your definitions here.
	MapTaskQue            []*TaskInfo // map待处理任务队列
	ReduceTaskQue         []*TaskInfo // reduce待处理任务队列
	MapTaskFinishedNum    int         // map已经完成数量
	ReduceTaskFinishedNum int         // reduce已经完成任务数量
	MapTaskAllNum         int         // map任务全部数量
	ReduceTaskAllNum      int         // reduce全部任务数量
	mutex                 sync.Mutex  // 请求任务和创建任务，更新任务完成数量都应该加锁
	finishMutex           sync.Mutex  // 完成任务更新结构数据加锁
}

// Your code here -- RPC handlers for the worker to call.

// GetMapTask work获取map任务
func (c *Coordinator) GetMapTask(req *GetMapTaskRequest, resp *GetMapTaskResponse) error {
	log.Println(fmt.Sprintf("recv map work request, workId:%d", req.Id))
	// 扫描是否还有任务没有完成
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if len(c.MapTaskQue) == c.MapTaskFinishedNum {
		log.Println("all map task is finished")
		resp.TaskInfo = nil
		return errors.New(string(MapTaskFinished))
	}
	// 分配任务
	for i, task := range c.MapTaskQue {
		if task.TaskCurStatus == TaskNoProcess {
			resp.TaskInfo = task
			// 该任务已经下发
			log.Println(fmt.Sprintf("send map task to workId:%d, workName:%s", req.Id, task.TaskName))
			c.MapTaskQue[i].TaskCurStatus = TaskInProcess
			return nil
		}
	}
	return errors.New(string(AllTaskSendFinished))
}

// GetReduceTask work获取Reduce任务
func (c *Coordinator) GetReduceTask(req *GetReduceTaskRequest, resp *GetReduceTaskResponse) error {
	log.Println(fmt.Sprintf("recv reduce work request, workId:%d", req.Id))
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// 扫描是否还有任务没有完成
	if len(c.ReduceTaskQue) == c.ReduceTaskFinishedNum {
		log.Println("all reduce task is finished")
		resp.TaskInfo = nil
		ch <- true
		return errors.New(string(ReduceTaskFinished))
	}
	// 分配任务
	for i, task := range c.ReduceTaskQue {
		// Reduce任务是在map完成过程中初始化，且长度手动输入
		// 防止长度过大会有部分没有初始化
		if task == nil {
			continue
		}
		if task.TaskCurStatus == TaskNoProcess {
			resp.TaskInfo = task
			// 该任务已经下发
			log.Println(fmt.Sprintf("send reduce task to workId:%d,", req.Id))
			c.ReduceTaskQue[i].TaskCurStatus = TaskInProcess
			return nil
		}
	}
	return errors.New(string(AllTaskSendFinished))
}

// WorkFinished work处理完成
func (c *Coordinator) WorkFinished(req *WorkFinishedRequest, resp *WorkFinishedResponse) error {
	c.finishMutex.Lock()
	defer c.finishMutex.Unlock()
	log.Println(fmt.Sprintf("%s worker finished, task id:%d, task name:%s",
		req.TaskType,
		req.TaskInfo.TaskId,
		req.TaskInfo.TaskName,
	))
	// 更改任务信息
	switch req.TaskType {
	case "map":
		c.MapTaskQue[req.TaskInfo.TaskId].TaskCurStatus = TaskFinished
		c.MapTaskFinishedNum++
	case "reduce":
		c.ReduceTaskQue[req.TaskInfo.TaskId].TaskCurStatus = TaskFinished
		c.ReduceTaskFinishedNum++
	}
	return nil
}

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// ret := false

	// // Your code here.
	// // 当所有任务完成，退出
	// if c.ReduceTaskAllNum == c.ReduceTaskFinishedNum {
	// 	ret = true
	// 	return ret
	// }
	// return ret
	return <-ch
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	log.Println("init Coordinator")
	// Your code here.
	// 初始化任务队列
	for i, file := range files {
		mapTaskInfo := &TaskInfo{
			TaskId:        i,
			TaskName:      Name(file),
			TaskCurStatus: TaskNoProcess,
			NReduce:       nReduce,
			NMap:          len(files),
		}
		c.MapTaskQue = append(c.MapTaskQue, mapTaskInfo)
	}
	for i := 0; i < nReduce; i++ {
		reduceTaskInfo := &TaskInfo{
			TaskId:        i,
			TaskCurStatus: TaskNoProcess,
			NReduce:       nReduce,
			NMap:          len(files),
		}
		c.ReduceTaskQue = append(c.ReduceTaskQue, reduceTaskInfo)
	}
	c.MapTaskAllNum = len(files)
	c.ReduceTaskAllNum = nReduce
	log.Println("init Coordinator successfully")
	c.server()
	return &c
}
