package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type SortedKey []KeyValue

func (k SortedKey) Less(i, j int) bool {
	return k[i].Key < k[j].Key
}
func (k SortedKey) Swap(i, j int) {
	k[i], k[j] = k[j], k[i]
}
func (k SortedKey) Len() int {
	return len(k)
}

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
	// CallExample()

	//一直执行直到有结果
	isOver := false
	for !isOver {

		taskFromMaster := getTaskFromMaster()
		//fmt.Printf("获取到任务：%v \n", taskFromMaster)
		switch taskFromMaster.TaskType {
		case reduceTask:
			{
				DoReduceTask(reducef, &taskFromMaster)
				sendDone(&taskFromMaster)
			}

		case mapTask:
			{
				//将结果放到中间文件中
				DoMapTask(mapf, &taskFromMaster)
				sendDone(&taskFromMaster)
			}
		case exitTask:
			{
				isOver = true
			}
		default:
			{
				time.Sleep(1 * time.Second)
				continue
			}

		}

	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
//将中间文件的kv取出来，并利用sort排好序，方便后面通过循环获取相同KEY的Value
func shuffle(task *Task) []KeyValue {
	//读取中间文件的内容，按照key区别，循环的放到reduce函数里面
	var kva []KeyValue
	fileNames := task.FileName
	//利用JSON  反向解析
	for _, fileName := range fileNames {
		file, _ := os.Open(fileName)
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(SortedKey(kva))

	return kva
}

func DoReduceTask(reducef func(key string, values []string) string, task *Task) {

	intermediate := shuffle(task)
	//fmt.Printf("taskId: %v len: %v", task.TaskId, len(intermediate))
	i := 0
	//一个reduce任务一个文件

	dir, _ := os.Getwd()
	//tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	//fmt.Println("reduce的tempFile文件名字为", tempFile.Name())
	//ofile, _ := os.Create("mr-out-" + strconv.Itoa(task.TaskId))
	for i < len(intermediate) {
		j := i + 1
		//将KEY相同的放到同一个地方
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		//同一个KEY交给同一个Value处理
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		//fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	err = tempFile.Close()
	if err != nil {
		fmt.Println("严重错误：" + err.Error())
	}
	// 在完全写入后进行重命名
	fn := fmt.Sprintf("mr-out-%d", task.TaskId)
	//fmt.Println("fn:" + fn + "    tempN:" + tempFile.Name())
	err = os.Rename(tempFile.Name(), fn)
	if err != nil {
		fmt.Println("严重错误：" + err.Error())
	}

}

func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {
	values := mapf(task.FileName[0], getContentByFileName(task.FileName[0]))

	nReduce := task.ReduceNum
	HashedKV := make([][]KeyValue, nReduce)

	//将每个Map任务的要通过hash的方式，对应到nReduce个中间文件；如果数据倾斜，没有那么多也没关系。
	for _, key := range values {
		index := ihash(key.Key) % nReduce
		HashedKV[index] = append(HashedKV[index], key)
	}

	//mr-tmp-xx-i  这个i是key值相近的reduce
	middleFileMap := make(map[int][]string)
	for i := 0; i < nReduce; i++ {
		oName := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oName)
		//利用JSON编码，便于在网络上传输
		encoder := json.NewEncoder(ofile)
		//对KeyValue进行编码
		for _, value := range HashedKV[i] {
			//落盘
			err := encoder.Encode(value)
			if err != nil {
				return
			}
		}
		//key是nReduce的序号
		middleFileMap[i] = append(middleFileMap[i], oName)
		ofile.Close()
	}

	//将信息告诉Master节点
	setMiddle(middleFileMap)
}

func sendDone(task *Task) {
	args := TaskArgs{}
	args.ArgsType = Set
	args.DoneTaskId = task.TaskId
	ok := call("Coordinator.CallDone", args, &Task{})
	if !ok {
		log.Println("调用失败！")
	}
}

func setMiddle(m map[int][]string) {
	args := MiddleFileMap{M: m}
	ok := call("Coordinator.SetMiddle", args, &Task{})
	if !ok {
		fmt.Println("setMiddle调用失败！")
	}
}

func getContentByFileName(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	defer file.Close()
	return string(content)
}

func getTaskFromMaster() Task {
	args := TaskArgs{}
	task := Task{}
	ok := call("Coordinator.Gettask", &args, &task)
	if !ok {
		fmt.Println("调用失败！")
	} else {
		//fmt.Printf("调用结果为：%v", task)
	}
	return task

}
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
