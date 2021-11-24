package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

const (
	MiddleFileNameSuffix string = "map-out-"
	ReduceFileNameSuffix string = "mr-out-"
)

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	workID := os.Getpid()
	// map任务
	for {
		err := WorkerMap(mapf, workID)
		if err != nil && err.Error() == string(MapTaskFinished) {
			break
		}
		time.Sleep(time.Second)
	}
	// reduce任务
	for {
		err := WorkerReduce(reducef, workID)
		if err != nil && err.Error() == string(ReduceTaskFinished) {
			break
		}
		time.Sleep(time.Second)
	}
}

func WorkerMap(mapf func(string, string) []KeyValue, workID int) error {
	workReq := &GetMapTaskRequest{
		Id:      workID,
		ReqName: "map",
	}
	workResp := &GetMapTaskResponse{}
	_, err := call("Coordinator.GetMapTask", &workReq, &workResp)
	if err != nil {
		if err.Error() == string(MapTaskFinished) {
			log.Println(fmt.Sprintf("work %d return", workID))
		}
		if err.Error() == string(AllTaskSendFinished) {
			log.Println("all map task send finished")
		}
		return err
	}
	// 读取文件
	file, err := os.Open(string(workResp.TaskInfo.TaskName))
	if err != nil {
		log.Fatalf("Worker os.Open %s failed, err:%s", workResp.TaskInfo.TaskName, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Worker ioutil.ReadAll err:%s", err)
	}
	// 获取文件键值对
	mapKV := mapf(string(workResp.TaskInfo.TaskName), string(content))
	// 写入中间文件
	for _, mp := range mapKV {
		num := ihash(mp.Key) % workResp.TaskInfo.NReduce
		// 每一个任务生成map-out-TaskId-reduceId的文件名
		fileName := MiddleFileNameSuffix + strconv.Itoa(workResp.TaskInfo.TaskId) + strconv.Itoa(num)
		file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666|os.ModeAppend)
		if err != nil {
			log.Fatalf("Worker os.Open middlefile err:%s", err)
		}
		// 写入文件给读写权限
		str := fmt.Sprintf("%v %v\n", mp.Key, mp.Value)
		file.Write([]byte(str))
		if err != nil {
			log.Fatalf("Worker write middlefile err:%s", err)
		}
		file.Close()
	}
	// 发送任务已经完成
	workFinishedReq := &WorkFinishedRequest{
		Id:       workID,
		TaskType: "map",
		TaskInfo: workResp.TaskInfo,
	}
	WorkFinishedResp := &WorkFinishedResponse{}
	call("Coordinator.WorkFinished", &workFinishedReq, &WorkFinishedResp)
	return nil
}

func WorkerReduce(reducef func(string, []string) string, workID int) error {
	workReq := &GetReduceTaskRequest{
		Id:      workID,
		ReqName: "reduce",
	}
	workResp := &GetReduceTaskResponse{}
	_, err := call("Coordinator.GetReduceTask", &workReq, &workResp)
	if err != nil {
		if err.Error() == string(ReduceTaskFinished) {
			log.Println(fmt.Sprintf("work %d return", workID))
		}
		if err.Error() == string(AllTaskSendFinished) {
			log.Println("all map task send finished")
		}
		return err
	}

	// 以workid命名写入输出输出文件， 防止并发写入问题
	fileNameOut := fmt.Sprintf("%s%d", ReduceFileNameSuffix, workID)
	outFile, _ := os.OpenFile(fileNameOut, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	// 处理所有reduceID文件
	reduceId := strconv.Itoa(workResp.TaskInfo.TaskId)
	// 把所有reduceID产生的键值对存放，然后统计重复的键写入文件
	intermediate := []KeyValue{}
	for mapId := 0; mapId < workResp.TaskInfo.NMap; mapId++ {
		// 读取文件
		fileName := MiddleFileNameSuffix + strconv.Itoa(mapId) + reduceId
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("Worker os.Open %s failed, err:%s", workResp.TaskInfo.TaskName, err)
		}
		read := bufio.NewReader(file)
		for {
			// 包括'\n'
			s, err := read.ReadString('\n')
			if err != nil || err == io.EOF {
				break
			}
			s = s[:len(s)-1]
			sArr := strings.Split(s, " ")
			kv := KeyValue{
				Key:   sArr[0],
				Value: sArr[1],
			}
			intermediate = append(intermediate, kv)
		}
		// 关闭文件
		file.Close()
	}
	// 排序统计数量
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		outFile.Write([]byte(fmt.Sprintf("%v %v\n", intermediate[i].Key, output)))
		i = j
	}
	// 关闭文件
	outFile.Close()

	// 发送任务已经完成
	workFinishedReq := &WorkFinishedRequest{
		Id:       workID,
		TaskType: "reduce",
		TaskInfo: workResp.TaskInfo,
	}
	WorkFinishedResp := &WorkFinishedResponse{}
	call("Coordinator.WorkFinished", &workFinishedReq, &WorkFinishedResp)

	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) (bool, error) {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true, nil
	}
	return false, err
}
