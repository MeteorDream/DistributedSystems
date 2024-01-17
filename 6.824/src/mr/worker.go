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

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func doMap(mapf func(string, string) []KeyValue, reply *WordCountReply) {
	// 执行 Map 任务将中间结果输出到 mr-X-Y 中
	// A reasonable naming convention for intermediate files is mr-X-Y,
	// where X is the Map task number, and Y is the reduce task number.

	// step 1: 调用 map 处理文件
	var file, err = os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()
	var kva = mapf(reply.Filename, string(content))

	// step 2: 生成 Nreduce 个中间文件
	var mediate = make([]*os.File, reply.Nreduce)
	var encoder = make([]*json.Encoder, reply.Nreduce)
	for i := range mediate {
		var mname = "mr-" + strconv.Itoa(reply.Id) + "-" + strconv.Itoa(i+1)
		var mfile, _ = os.Create(mname)
		mediate[i] = mfile
		encoder[i] = json.NewEncoder(mfile)
	}

	// step3: 利用哈希函数将 key/value 键值对映射到中间文件中
	for _, kv := range kva {
		var idx = ihash(kv.Key) % reply.Nreduce
		err := encoder[idx].Encode(kv)
		if err != nil {
			log.Fatal("Encoder Error:", err)
		}
	}

	// step 4: 后处理，关闭文件
	for _, f := range mediate {
		_ = f.Close()
	}
}

func doReduce(reducef func(string, []string) string, reply *WordCountReply) {
	// 执行 reduce 函数将结果输出到 mr-out-X 中

	// step 1: 采集名为 mr-*-id的文件
	var kva []KeyValue
	for i := 1; i <= reply.Nmap; i++ {
		var fileName = "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.Id)
		file, err := os.Open(fileName)
		if err != nil {
			log.Println("Can't find ", fileName)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	// step 2: 排序
	sort.Sort(ByKey(kva))

	// step 3: 滑窗调用 reduce 输出到目标文件
	oname := "mr-out-" + strconv.Itoa(reply.Id)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	var args = &WordCountArgs{Id: os.Getpid()}
	var reply = &WordCountReply{}
	// 第一次请求调用 getTask
	// 请求出错退出
	if call("Coordinator.GetTask", args, reply) == false {
		return
	}
	// 循环请求直到收到通知退出或请求出错
	for {
		// 根据返回类型做对应处理
		// fmt.Printf("处理任务：type=%v, id=%v\n", reply.Type, reply.Id)
		switch reply.Type {
		case "map":
			doMap(mapf, reply)
			args.TaskType = "map"
			args.TaskId = reply.Id
			if call("Coordinator.FinishTask", args, reply) == false {
				return
			}
			// fmt.Println("map 完成请求收到回复：", reply)
		case "reduce":
			doReduce(reducef, reply)
			args.TaskType = "reduce"
			args.TaskId = reply.Id
			if call("Coordinator.FinishTask", args, reply) == false {
				return
			}
			// fmt.Println("reduce 完成请求收到回复：", reply)
		case "wait":
			// 等待 3 秒钟后继续
			time.Sleep(time.Second * 3)
			if call("Coordinator.GetTask", args, reply) == false {
				return
			}
		case "exit":
			// exit 任务退出
			return
		default:
			log.Fatal("Unkown reply type:", reply.Type)
		}
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
		return false
		// log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
