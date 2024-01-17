package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	id        int
	startTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	files         []string     // 需要处理的文件列表
	mapCounter    int          // 完成 map 任务的数量
	reduceCounter int          // 完成 reduce 任务的数量
	mapTasks      chan int     // map 任务队列
	reduceTasks   chan int     // reduce 任务队列
	runningMap    map[int]Task // 在执行的任务:key = 任务下标, value = 起始时间
	runningReduce map[int]Task // 在执行的 reduce 任务队列
	nReduce       int          // reduce 数量
	mutex         sync.Mutex   // 并发锁
}

// Your code here -- RPC handlers for the worker to call.
// 分配任务
func (c *Coordinator) GetTask(args *WordCountArgs, reply *WordCountReply) error {
	// step 1: 获取请求 id
	var id = args.Id
	// step 2: 分配任务
	reply.Nreduce = c.nReduce
	reply.Nmap = len(c.files)
	// 任务分配逻辑是：
	// 有 map 任务则分配 map 任务，没有 map 任务且 map 任务未执行完则等待
	// map 任务执行完则分配 reduce 任务，reduce 分配完且未执行完则等待
	c.mutex.Lock() // 先加互斥锁
	defer c.mutex.Unlock()
	if len(c.mapTasks) > 0 {
		// 还有 map 任务，分配 map 任务
		reply.Type = "map"
		idx := <-c.mapTasks
		reply.Id = idx
		reply.Filename = c.files[idx-1]
		c.runningMap[idx] = Task{id, time.Now()}
	} else if c.mapCounter < len(c.files) {
		// 没有 map 可分配的 map 任务但 map 又还没有完成
		reply.Type = "wait"
	} else if len(c.reduceTasks) > 0 {
		// 有 reduce 任务
		reply.Type = "reduce"
		idx := <-c.reduceTasks
		reply.Id = idx
		c.runningReduce[idx] = Task{id, time.Now()}
	} else if c.reduceCounter < c.nReduce {
		// 还有 reduce 任务未完成，等等吧
		reply.Type = "wait"
	} else {
		// 任务完成，结束吧
		reply.Type = "exit"
	}
	// fmt.Printf("分配 %v 类型任务(id = %v) 给 %v \n", reply.Type, reply.Id, id)
	return nil
}

// 完成任务
func (c *Coordinator) FinishTask(args *WordCountArgs, reply *WordCountReply) error {
	c.mutex.Lock()
	// 处理逻辑，检测完成的任务是否在队列中
	// 若在且分配给当前worker，则标记任务完成
	// 否则为超时任务，忽略掉
	switch args.TaskType {
	case "map":
		if t, ok := c.runningMap[args.TaskId]; ok && t.id == args.Id {
			c.mapCounter++
			delete(c.runningMap, args.TaskId)
		}
	case "reduce":
		if t, ok := c.runningReduce[args.TaskId]; ok && t.id == args.Id {
			c.reduceCounter++
			delete(c.runningReduce, args.TaskId)
		}
	default:
		reply.Type = "error"
		log.Fatal("Unknow Finish Task:", args.TaskType)
		return nil
	}
	c.mutex.Unlock()
	// 顺便再分配个任务
	c.GetTask(args, reply)
	// fmt.Printf("收到来自 %v 的 %v(id = %v) 任务完成请求, 分配任务 %v\n", args.Id, args.TaskType, args.TaskId, reply)
	return nil
}

// 探测超时任务并重新分配
func (c *Coordinator) timeout(sec int) {
	for {
		c.mutex.Lock()
		var fm []int
		// 先处理 map 任务
		for k, v := range c.runningMap {
			if time.Since(v.startTime) > time.Second*time.Duration(sec) {
				fm = append(fm, k)
			}
		}
		// 回收超时任务到任务队列
		for _, t := range fm {
			delete(c.runningMap, t)
			c.mapTasks <- t
		}
		// 同样处理 reduce 任务
		var rm []int
		for k, v := range c.runningReduce {
			if time.Since(v.startTime) > time.Second*time.Duration(sec) {
				rm = append(rm, k)
			}
		}
		// 回收超时任务到任务队列
		for _, t := range rm {
			delete(c.runningReduce, t)
			c.reduceTasks <- t
		}
		c.mutex.Unlock()
		// 停 1 秒
		time.Sleep(time.Second)
	}
}

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
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// 如果任务全部完成，那么就可以返回 true
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// fmt.Printf("当前 map 任务：%v/%v(正在进行: %v), reduce 任务: %v/%v(正在进行: %v)\n", c.mapCounter, len(c.files), len(c.runningMap), c.reduceCounter, c.nReduce, len(c.runningReduce))
	if c.mapCounter == len(c.files) && c.reduceCounter == c.nReduce {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:         files,
		mapCounter:    0,
		reduceCounter: 0,
		mapTasks:      make(chan int, len(files)),
		reduceTasks:   make(chan int, nReduce),
		runningMap:    make(map[int]Task),
		runningReduce: make(map[int]Task),
		nReduce:       nReduce,
	}

	// Your code here.
	// 初始化
	for i := range files {
		c.mapTasks <- i + 1
	}
	for i := 1; i <= nReduce; i++ {
		c.reduceTasks <- i
	}

	// 启动定时轮询
	go c.timeout(10)

	c.server()
	return &c
}
