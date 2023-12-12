package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var mu sync.Mutex //全局的
type Coordinator struct {
	// Your definitions here.
	ReduceNum     int              //reduce的数量
	muxForIndex   sync.Mutex       //Id生成
	TaskIdIndex   int              //生成自增Id的索引
	TimeOut       time.Duration    // time
	TimeOutTaskId []int            //处理超时的节点
	files         []string         //输入文件数组  也是Map任务的个数
	ReadyMapNum   int              //用于判断map任务是否发放完
	DoneReduceNum int              //记录已经完成的reduce任务
	TaskMap       map[int]*Task    //id与任务的映射
	middleFile    map[int][]string // k:NReduce的序号  v:一组key的name的文件列表
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) SetMiddle(args MiddleFileMap, task *Task) error {
	mu.Lock()
	defer mu.Unlock()
	//记录中间状态信息
	//
	//中
	//对超时的直接不处理
	for _, i := range c.TimeOutTaskId {
		if i == args.TaskId {
			return nil
		}
	}

	//每一个Map生产出一系列  mr-tmp-xxx-（0-10）
	// k是nReduce的序号，V只有一个文件mr-tmp-taskId-k（0-10）
	for k, v := range args.M {
		c.middleFile[k] = append(c.middleFile[k], v...)
	}
	//fmt.Printf("传过来的中间状态：%v", args.M)
	//fmt.Printf("中间状态：%v", c.middleFile)
	return nil
}
func (c *Coordinator) Gettask(args *TaskArgs, task *Task) error {
	mu.Lock()
	//fmt.Printf("当前的主节点状态：%v \n", *c)
	defer func() {
		mu.Unlock()
		//故障恢复
		if len(task.FileName) > 0 {
			//fmt.Printf("有效返回的任务为 %v \n", task)
			//c.PrintTaskStatus()
			go func() {
				//
				ticker := time.NewTimer(c.TimeOut)
				select {
				case <-ticker.C:
					mu.Lock()
					defer mu.Unlock()
					//fmt.Printf("可能超时的任务：%v \n", task)
					//fmt.Printf("当前master状态：%v \n", c)
					//
					if c.TaskMap[task.TaskId].Status < Over {
						c.TimeOutTaskId = append(c.TimeOutTaskId, task.TaskId)
						id := c.getTaskId()

						var newTask *Task
						newTask = &Task{}
						*newTask = *task
						newTask.TaskId = id
						newTask.Status = Ready
						fmt.Printf("超时任务为 %v \n", newTask)
						c.TaskMap[id] = newTask
						c.TaskMap[task.TaskId].Status = TimeOut

						if newTask.TaskType == mapTask {
							c.ReadyMapNum++
						} else if newTask.TaskType == reduceTask {
							//reduce还要把middleFile recover
							newTask.FileName = task.FileName
							c.DoneReduceNum--
						} else {
							//exitTask

						}
					}

				}

			}()
		}

	}()
	//如何判断是map任务还是reduce任务
	if c.ReadyMapNum > 0 {
		for _, t := range c.TaskMap {
			if t.TaskType == mapTask && t.Status == Ready {
				*task = *t
				task.Status = Running
				t.Status = Running //重大的BUG所在地，调整之后就成功了
				c.ReadyMapNum--
				break
			}
		}
		return nil
	} else if c.DoneReduceNum < c.ReduceNum && c.CheckMapIfAllDone() {
		//优先处理未超时的reduce任务
		for k, v := range c.middleFile {
			if len(v) == len(c.files) {
				//只进行一次
				*task = Task{
					TaskId:    c.getTaskId(), //用NReduce的序号
					ReduceNum: c.ReduceNum,
					Status:    Running,
					FileName:  v,
					TaskType:  reduceTask,
				}
				delete(c.middleFile, k)
				c.TaskMap[task.TaskId] = task
				c.DoneReduceNum++
				return nil

			}

		}

		//
		for _, t := range c.TaskMap {
			if t.TaskType == reduceTask {
				if t.Status == Ready {
					*task = *t
					t.Status = Running
					c.DoneReduceNum++
					return nil
				}
			}
		}

	}

	//fmt.Printf("当前master的任务状态为：%v \n", c)
	//判断是否结束
	var isOver bool = true
	for _, v := range c.TaskMap {
		if v.Status < Over {
			isOver = false
		}
	}
	if isOver {
		task.TaskType = exitTask
	}

	return nil
}
func (c *Coordinator) PrintTaskStatus() {
	mu.Lock()
	defer mu.Unlock()
	for _, task := range c.TaskMap {
		fmt.Printf("当前任务Id为 %d,status: % d", task.TaskId, task.Status)
	}
}

// CheckMapIfAllDone 保证Map都是超时或者是完成的
func (c *Coordinator) CheckMapIfAllDone() bool {
	for _, task := range c.TaskMap {
		if task.TaskType == mapTask {

			if task.Status < Over {
				return false
			}

		}
	}
	return true
}
func (c *Coordinator) CallDone(args TaskArgs, task *Task) error {
	mu.Lock()
	defer mu.Unlock()

	if args.ArgsType == Set {
		c.TaskMap[args.DoneTaskId].Status = Over
	}
	return nil
}

func (c *Coordinator) getTaskId() int {
	c.muxForIndex.Lock()
	defer c.muxForIndex.Unlock()
	temp := c.TaskIdIndex
	c.TaskIdIndex++
	//fmt.Println("生成数字" + strconv.Itoa(temp))
	return temp
}

//
// start a thread that listens for RPCs from worker.go
//启动一个RPC服务
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
func (c *Coordinator) Done(i int) bool {
	mu.Lock()
	defer mu.Unlock()
	//用于判断是否结束
	//等待所有的任务都变成超时或结束
	var isOver bool = true
	for _, v := range c.TaskMap {
		if v.Status < Over {
			isOver = false
		}
	}
	return isOver
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		ReduceNum:   nReduce,
		TaskIdIndex: 0,
		//TaskChannelReduce: make(chan *Task),
		//TaskChannelMap:    make(chan *Task),
		TimeOut:       10 * time.Second,
		TimeOutTaskId: make([]int, 0),
		TaskMap:       make(map[int]*Task),
		middleFile:    make(map[int][]string),
	}

	// Your code here.
	//生成Map任务
	for _, file := range files {
		task := &Task{
			TaskId:    c.getTaskId(),
			ReduceNum: c.ReduceNum,
			FileName:  []string{file},
			TaskType:  mapTask,
			Status:    Ready,
		}
		c.TaskMap[task.TaskId] = task
	}
	//
	c.ReadyMapNum = len(files)

	c.server()
	//fmt.Printf("完成主节点的初始化：%v \n", c)
	return &c
}
