package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

//rpc请求传入参数，只是worker获取一个Task，不需要参数
type TaskArgs struct {

}

//worker向master请求任务时，除了返回task，还应该返回task状态
//分别为获取task成功； 暂没有未分配的task; 所有worker任务完成
type ReplyState int
const (
	TaskGot    ReplyState = iota
	NoTaskNow
	Finish
)

type TaskReply struct {
	TaskReply Task
	Answer    ReplyState
}

//worker完成后告知master更新任务状态，master去修改TaskMap中的任务状态
type FinArgs struct {
	TaskId int
	IsFinished bool  //标记是否完成（可能有异常）
}
type FinReply struct {

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
