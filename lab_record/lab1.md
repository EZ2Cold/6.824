# Lab 1: MapReduce
MapReduce是一个分布式计算框架，利用多台机器并行地处理大型计算任务。本实验要实现coordinator进程和worker进程，其中coordinator负责将map/reduce任务分发给不同的worker进程，而worker进程负责执行具体的map/reduce函数。coordinator和worker进程之间通过RPC进行通信。

coordinator的定义如下：
```
type Coordinator struct {
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
```

定义3个RPC：
```
func (c *Coordinator) FetchTask(_ EmptyArgs, t *TaskSpec) error
// worker进程调用该函数获取任务，如果idle_list不为空，则从中取下一个任务并放到 processing_list中，记录任务开始时间，并分配给worker
```

```
func (c *Coordinator) MapDone(args *MapDoneArgs, t *EmptyReply) error
// map任务结束后调用该函数，从processing_list中取下，并记录返回的中间结果文件名（每个reduce任务对应一个文件）

// 所有map任务都完成后，将reduce_task_list赋值给idle_list，开始调度reduce任务。
```

```
func (c *Coordinator) ReduceDone(arg uint, t *EmptyReply) error 
// reduce任务结束后调用该函数，从processing_list中取下
```

其他实现细节：
1. a "please exit" task
2. 后台线程重新调度运行时间过长的任务（e.g. 10s）
3. Rename由map/reduce任务生成的临时文件
