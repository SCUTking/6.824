package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMy(t *testing.T) {
	mychan := make(chan int, 1)

	fmt.Println(len(mychan))
	fmt.Println(cap(mychan))
	mychan <- 1

	fmt.Println(len(mychan))
	fmt.Println(cap(mychan))
}

func TestMy1(t *testing.T) {

	//ticker := time.NewTicker(1 * time.Second)
	//for {
	//	ticker.Reset(1 * time.Second)
	//}
	//
	//select {
	//case <-ticker.C:
	//	fmt.Printf("触发")
	//}

	var timer *time.Timer
	timer = time.NewTimer(1 * time.Second)

	go func() {
		for {
			timer.Reset(1 * time.Second)
			//timer = time.NewTimer(1 * time.Second) GC与创建对象的时间可能会大于设置的时间，导致timer重置时间失败
		}
	}()

	select {
	case <-timer.C:
		fmt.Printf("触发")
	}

}

// 测试sleep
func TestMy2(t *testing.T) {
	p := make(chan int, 1)
	p <- 1
	for len(p) > 0 { //无条件循环，最后通过break跳出循环   注意循环不会造成栈溢出，因为一层结束后释放本层资源
		v := <-p
		fmt.Println(v)
	}

	fmt.Println(len(p))

}

const A int = 1

// 测试sleep
func TestMy3(t *testing.T) {
	//var a Raft
	//a = Raft{State: 1}
	//a.State = A
	//type test struct {
	//	B State
	//}
	//type State int
	//
	////var b State
	////b=A
	//
	//t2 := test{B: 1}
	//t2.B = A
	//
	//fmt.Println(test)

	timer := time.NewTimer(100 * time.Second)

	// 停止定时器
	timer.Stop()
	//if !timer.Stop() {
	//	fmt.Println("通道里面还有东西")
	//	<-timer.C
	//} else {
	//
	//}

	// 重新启动定时器
	//reset := timer.Reset(2 * time.Second)
	//
	//fmt.Println(reset)

	// 等待定时器到期
	for {
		select {
		case <-timer.C:
			fmt.Println("Timer expired!")
		default:
			continue
		}
	}

}

// 测试sleep
func TestMy4(t *testing.T) {
	var a int = 2
	switch a {
	case 0:
		fmt.Println(1)
	case 1:
		{
			fmt.Println(0)
		}
	case 2:
		fmt.Println(2)
	case 3:
		fmt.Println(3)
	}

}

func TestMy5(t *testing.T) {
	//测试是否
	var mu sync.Mutex
	//mu.Lock()
	lock := mu.TryLock()
	fmt.Println(lock)
	mu.Unlock()
}
