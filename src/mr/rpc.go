package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
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

// 空参数，当RPC调用没有入参时传入该类型变量
type EmptyArgs struct{}

// 空返回值，当RPC调用没有返回值时传入该类型变量
type EmptyReply struct{}

// FetchTask RPC
// worker调用该进程获取一个任务
// 如果没有任务，t置为nil
func (c *Coordinator) FetchTask(_ EmptyArgs, t *TaskSpec) error {
	if c.Done() {
		// 任务全部完成，返回一个空任务让worker进程退出
		*t = TaskSpec{Type: EXIT}
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.idle_list) > 0 {
		task := c.idle_list[0]
		c.idle_list = c.idle_list[1:]
		// 记录开始执行的时间
		task.STime = time.Now()
		c.processing_list = append(c.processing_list, task)
		*t = task.Spec
		return nil
	} else {
		*t = TaskSpec{Type: None}
		return nil
	}
}

type MapDoneArgs struct {
	Num   uint
	Files []string
}

// MapDone RPC
// 通知coordinator一个map任务完成
// 输入参数为任务的编号以及中间结果的文件名
func (c *Coordinator) MapDone(args *MapDoneArgs, t *EmptyReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// step1：全部map任务已经完成，什么也不做
	if c.map_done {
		*t = EmptyReply{}
		return nil
	}
	// step2：从processing_list列表中取下
	// 如果没有找到，有两种可能情况
	// 1. 任务运行太久，被重新调度，但还没有分配给一个worker;从idle_list中取下
	// 2. 分配给另一个worker且已经完成了;什么也不做
	proc_idx := 0
	for ; proc_idx < len(c.processing_list); proc_idx++ {
		if args.Num == c.processing_list[proc_idx].Spec.Num {
			break
		}
	}
	if proc_idx == len(c.processing_list) {
		idle_idx := 0
		for ; idle_idx < len(c.idle_list); idle_idx++ {
			if args.Num == c.idle_list[idle_idx].Spec.Num {
				break
			}
		}
		if idle_idx < len(c.idle_list) {
			// 从idle_list中取下
			c.idle_list = append(c.idle_list[:idle_idx], c.idle_list[idle_idx+1:]...)
			// 将中间结果的文件名通知给对应的reduce任务
			for i := 0; i < c.NRecude; i++ {
				c.reduce_task_list[i].Spec.Files = append(c.reduce_task_list[i].Spec.Files, args.Files[i])
			}
		}
	} else {
		c.processing_list = append(c.processing_list[:proc_idx], c.processing_list[proc_idx+1:]...)
		// 将中间结果的文件名通知给对应的reduce任务
		for i := 0; i < c.NRecude; i++ {
			c.reduce_task_list[i].Spec.Files = append(c.reduce_task_list[i].Spec.Files, args.Files[i])
		}
	}
	// step3：如果idle_list和proc_list都为空，则全部map任务完成
	if len(c.idle_list) == 0 && len(c.processing_list) == 0 {
		c.map_done = true
		// 开始reduce任务
		c.idle_list = c.reduce_task_list
	}
	*t = EmptyReply{}
	return nil
}

// Reduce RPC
// 通知coordinator一个reduce任务完成
// 输入参数为任务的编号
func (c *Coordinator) ReduceDone(arg uint, t *EmptyReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// step1：全部任务已经完成，什么也不做
	if c.Done() {
		*t = EmptyReply{}
		return nil
	}
	// step2：从processing_list列表中取下
	// 如果没有找到，有两种可能情况
	// 1. 任务运行太久，被重新调度，但还没有分配给一个worker;从idle_list中取下
	// 2. 分配给一个worker且已经完成了;什么也不做
	proc_idx := 0
	for ; proc_idx < len(c.processing_list); proc_idx++ {
		if arg == c.processing_list[proc_idx].Spec.Num {
			break
		}
	}
	if proc_idx == len(c.processing_list) {
		idle_idx := 0
		for ; idle_idx < len(c.idle_list); idle_idx++ {
			if arg == c.idle_list[idle_idx].Spec.Num {
				break
			}
		}
		if idle_idx < len(c.idle_list) {
			// 从idle_list中取下
			c.idle_list = append(c.idle_list[:idle_idx], c.idle_list[idle_idx+1:]...)
		}
		*t = EmptyReply{}
		return nil
	}
	c.processing_list = append(c.processing_list[:proc_idx], c.processing_list[proc_idx+1:]...)
	// step3：如果idle_list和proc_list都为空，则全部map任务完成
	if len(c.idle_list) == 0 && len(c.processing_list) == 0 {
		c.reduce_done = true
	}
	*t = EmptyReply{}
	return nil
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
