package mr

import (
	"fmt"
	"testing"
)

func TestRpc(t *testing.T) {

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

func TestGetTask(t *testing.T) {
	args := TaskArgs{
		DoneTaskId: -1,
	}
	task := Task{TaskId: 1}
	ok := call("Coordinator.Gettask", &args, &task)
	if !ok {
		fmt.Println("调用失败！")
	} else {
		fmt.Printf("调用结果为：%v", task)
	}
}
func TestPtr(t *testing.T) {
	var p *Task
	p.TaskId = 1

	var a *Task
	a = p
	fmt.Println(a)
}
func TestDefer(t *testing.T) {
	defer fmt.Println("Outer defer")

	fmt.Println("Before nested defer")
	defer func() {
		fmt.Println("Inner defer")
		defer fmt.Println("Inner nested defer")
	}()
	fmt.Println("After nested defer")
}
