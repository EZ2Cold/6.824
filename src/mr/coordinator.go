package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskType int

const (
	MAP TaskType = iota
	REDUCE
	EXIT
	None
)

// worker进程请求任务时的返回值
type TaskSpec struct {
	// 任务的类型
	Type TaskType
	// 任务编号
	Num uint
	// 任务的输入文件
	Files []string
	// reduce任务的数量
	NRecude int
}

func (t *TaskSpec) Print() {
	fmt.Println("===========================")
	fmt.Println("Task Type: ", t.Type)
	fmt.Println("Task Num: ", t.Num)
	fmt.Println("Input Files: ", t.Files)
	fmt.Println("Reduce Num: ", t.NRecude)
	fmt.Println("===========================")
}

// coordinator内部用于保存一个任务相关信息的数据结构
type Task struct {
	// 任务描述
	Spec TaskSpec
	// 任务开始时间
	STime time.Time
}

type Coordinator struct {
	// Your definitions here.
	// 待分配的任务列表
	idle_list []*Task
	// 正在进行中的任务列表
	processing_list []*Task
	// reduce任务列表
	reduce_task_list []*Task
	// 保护以上三个列表的锁
	mu sync.Mutex
	// 指示map任务是否全部完成
	map_done bool
	// 指示reduce任务是否全部完成
	reduce_done bool
	// reduce任务的数量
	NRecude int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// 不断地从processing_list中取下执行时间过长的任务并把它放到idle_list中
func (c *Coordinator) reschedule() {
	for {
		c.mu.Lock()
		i := 0
		for ; i < len(c.processing_list) && (time.Since(c.processing_list[i].STime) > 10*time.Second); i++ {
		}
		c.idle_list = append(c.idle_list, c.processing_list[:i]...)
		c.processing_list = c.processing_list[i:]
		c.mu.Unlock()
		time.Sleep(time.Second)
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.

	return c.reduce_done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// TODO 启动一个后台线程重分配执行时间久的任务
	c := Coordinator{}

	// Your code here.
	var num uint = 0
	n := len(files)
	// step1：每个map任务处理一个文件，创建n个map任务
	for i := 0; i < n; i++ {
		c.idle_list = append(c.idle_list, &Task{Spec: TaskSpec{Type: MAP, Num: num, Files: []string{files[i]}, NRecude: nReduce}})
		num += 1
	}
	// step2：创建nReduce个reduce任务
	num = 0
	for i := 0; i < nReduce; i++ {
		c.reduce_task_list = append(c.reduce_task_list, &Task{Spec: TaskSpec{Type: REDUCE, Num: num}})
		num += 1
	}
	c.NRecude = nReduce
	go c.reschedule()
	c.server()
	return &c
}
