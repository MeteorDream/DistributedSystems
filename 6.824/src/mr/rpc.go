package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// example to show how to declare the arguments
// and reply for an RPC.
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type WordCountArgs struct {
	Id       int    // 请求方的 id
	TaskType string // 完成任务的类型, map / reduce
	TaskId   int    // 完成任务的 id 用于通知任务完成
}

type WordCountReply struct {
	Type     string // 操作类型：map, reduce, wait, exit, error
	Id       int    // map 或 reduce 操作编号，id 为 1 - n (不设置为 0 避免 0 值被 rpc 忽略)
	Filename string // map 操作处理文件名
	Nmap     int    // map 操作文件数量
	Nreduce  int    // reduce 数量
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
