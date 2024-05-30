package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		spec := new(TaskSpec)
		// 通过RPC从coordinator获取任务
		ok := call("Coordinator.FetchTask", EmptyArgs{}, &spec)
		if !ok {
			log.Fatal("call FetchTask failed!\n")
		}
		// spec.Print()
		if spec.Type == MAP {
			handleMap(spec, mapf)
		} else if spec.Type == REDUCE {
			handleReduce(spec, reducef)
		} else if spec.Type == None {
			// 没有获取到任务，睡眠0.5后接着尝试
			time.Sleep(500 * time.Millisecond)
		} else {
			// EXIT任务 直接退出
			return
		}
	}
}

// 处理map任务
func handleMap(spec *TaskSpec, mapf func(string, string) []KeyValue) {
	// step1：读取文件内容
	intermediate := []KeyValue{}
	for _, filename := range spec.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	// step2：创建nReduce个文件
	bucket := make([]*os.File, spec.NRecude)
	encs := make([]*json.Encoder, spec.NRecude)
	filenames := make([]string, spec.NRecude)
	for i := 0; i < spec.NRecude; i++ {
		filenames[i] = fmt.Sprintf("mr-%d-%d", spec.Num, i)
		file, err := os.CreateTemp(os.TempDir(), filenames[i])
		if err != nil {
			log.Fatal(err)
		}
		bucket[i] = file
		encs[i] = json.NewEncoder(file)
	}
	// step3：将中间结果按照key值划分，序列化后存储到对应文件
	for _, kv := range intermediate {
		i := ihash(kv.Key) % spec.NRecude
		err := encs[i].Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}
	// step4：Rename文件
	for i := 0; i < spec.NRecude; i++ {
		target_filepath := filepath.Join(".", filenames[i])
		_, err := os.Stat(target_filepath)
		if err == nil {
			// 文件已经成功生成，删除临时文件
			tmp := bucket[i].Name()
			bucket[i].Close()
			err := os.Remove(tmp)
			if err != nil {
				log.Fatal("Remove file failed!\n")
			}
		} else {
			// 不存在或者其他错误，覆盖原文件
			err := os.Rename(bucket[i].Name(), target_filepath)
			if err != nil {
				log.Fatal("Rename file failed!\n")
			}
			bucket[i].Close()
		}
	}
	// step5：通知coordinator任务完成，并告知其生成的bucket文件名
	ok := call("Coordinator.MapDone", &MapDoneArgs{Num: spec.Num, Files: filenames}, &EmptyReply{})
	if !ok {
		log.Fatal("call MapDone failed!\n")
	}
}

// 处理reduce任务
func handleReduce(spec *TaskSpec, reducef func(string, []string) string) {
	// step1：读取文件，反序列化键值对
	kva := []KeyValue{}
	for _, filename := range spec.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	// step2：所有键值对按照key值排序
	sort.Sort(ByKey(kva))
	// step3：创建临时文件
	filename := fmt.Sprintf("mr-out-%d", spec.Num)
	file, err := os.CreateTemp(os.TempDir(), filename)
	if err != nil {
		log.Fatal(err)
	}
	// step3：合并具有相同key的value，并调用reduce函数
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)

		i = j
	}
	// step4：Rename文件
	target_filepath := filepath.Join(".", filename)
	_, err = os.Stat(target_filepath)
	if err == nil {
		// 文件已经成功生成，删除临时文件
		tmp := file.Name()
		file.Close()
		err := os.Remove(tmp)
		if err != nil {
			log.Fatal("Remove file failed!\n")
		}
	} else {
		// 不存在或者其他错误，覆盖原文件
		err := os.Rename(file.Name(), target_filepath)
		if err != nil {
			log.Fatal("Rename file failed!\n")
		}
		file.Close()
	}
	// step5：通知coordinator任务完成
	ok := call("Coordinator.ReduceDone", spec.Num, &EmptyReply{})
	if !ok {
		log.Fatal("call ReduceDone failed!\n")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
