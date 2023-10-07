package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.

type Task struct {
	Type     int    // 任务的类型，包括 Map 和 Reduce
	Filename string // 当 Type 为 Map 时字段有效，表示需要作用 Map 函数的文件的文件名
	TaskId   int    // 用于标记这个 Task 的 Id，在对 Coordinator 回馈结果的时候作为参数返回
	ReduceId int    // Coordinator 要求这个 Worker 统计的 Reduce 的 Id
}

const (
	Map = iota
	Reduce
)

// NewMapTask  创建一个对应 Map 的 Task 的结构
func NewMapTask(filename string, taskId int) Task {
	return Task{
		Type:     Map,
		Filename: filename,
		TaskId:   taskId,
	}
}

// NewReduceTask  创建一个对应 Reduce 的 Task 的结构
func NewReduceTask(reduceId, taskId int) Task {
	return Task{
		Type:     Reduce,
		TaskId:   taskId,
		ReduceId: reduceId,
	}
}

// NeedWorkArgs NeedWork RPC: 用于请求 Coordinator 分配新的任务
// NeedWork 的参数
type NeedWorkArgs struct {
}

// NeedWorkReply NeedWork 的返回值
type NeedWorkReply struct {
	T         Task
	ReduceCnt int
}

// FinishWorkArgs FinishWork RPC: 用于告知 Coordinator 任务已完成
// FinishWork 的参数
type FinishWorkArgs struct {
	TaskId int
}

// FinishWorkReply FinishWork 的返回值
type FinishWorkReply struct {
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
