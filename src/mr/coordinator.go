package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"
import "strconv"
import "fmt"
import "strings"

var mu sync.Mutex // 定义全局互斥锁，worker访问Master时加锁
type TaskType int
const (
	MapTask TaskType = iota
	ReduceTask
)

type TaskState int
const (
	Waiting TaskState = iota
	Working
	Finished
)

type Task struct {
	TaskId     int   //任务id，master生成，唯一
	TaskType   TaskType   //map or reduce
	TaskState  TaskState    
	StartTime  time.Time
	InputFile  []string    //一个file对应一个map，但是一个reduce可能要处理多个mr-X-Y中间的file
	ReduceKth  int      //reduce函数输出文件名时需要，mr-X-Y, Y对应应该分到的reduce编号
	ReduceNum  int
}

type Phase int
const (
	MapPhase Phase = iota   //map阶段
	ReducePhase             
	Done                      
)

type Coordinator struct {
	CurrentPhase          Phase  //当前MapReduce的阶段
	TaskIdForGen          int  //用于生成任务唯一id
	MapTaskChannel        chan *Task
	ReduceTaskChannel     chan *Task
	TaskMap               map[int]*Task  //记录所有的task
	MapperNum             int      //每个文件分给一个map去做
	ReduceNum             int      //传入的reducer数量，用于hash
}

//定义想通过RPC提供的方法，rpc.Register(c)会将所有方法都注册
// type CoordinatorRPC interface {
//     AssignTask(args *TaskArgs, reply *TaskReply) error
// 	UpdateTaskState(args *FinArgs, reply *FinReply) error
// }


func (c * Coordinator) GenerateTaskId() int {
	c.TaskIdForGen++
	return c.TaskIdForGen
}
// Your code here -- RPC handlers for the worker to call.

//
// RPC中暴露给client的方法
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	mu.Lock()
	defer mu.Unlock()
	log.Printf("master get a request from worker\n")

	switch c.CurrentPhase {
	case MapPhase:
		if len(c.MapTaskChannel) > 0 {
			taskp := <- c.MapTaskChannel
			if taskp.TaskState == Waiting {
				reply.TaskReply = *taskp
				reply.Answer = TaskGot
				taskp.TaskState = Working
				taskp.StartTime = time.Now()
				c.TaskMap[taskp.TaskId] = taskp
				fmt.Printf("Task[%d] has been assigned\n", taskp.TaskId)
			}
		} else {
			reply.Answer = NoTaskNow
			log.Printf("no task now")
			if c.checkMapTaskDone(){
				c.toNextStage()
			}
			return nil
		}

	case ReducePhase:
		log.Println("reduce phase")
		if len(c.ReduceTaskChannel) > 0 {
			taskp := <- c.ReduceTaskChannel
			if taskp.TaskState == Waiting {
				reply.Answer = TaskGot
				reply.TaskReply = *taskp
				taskp.TaskState = Working
				taskp.StartTime = time.Now()
				c.TaskMap[taskp.TaskId] = taskp
				fmt.Printf("Task[%d] has been assigned\n", taskp.TaskId)
			}
		} else {
			reply.Answer = NoTaskNow
			if c.checkReduceTaskDone(){
				c.toNextStage()
			}
			return nil
		}
	
	case Done:
		reply.Answer = Finish
		fmt.Println("all task have been finished")
	
	default:
		fmt.Println("undefined phase")
	}
	log.Printf("assign successful")
	return nil
}

func (c *Coordinator) UpdateTaskState(args *FinArgs, reply *FinReply) error{
	mu.Lock()
	defer mu.Unlock()
	id := args.TaskId
    task, ok := c.TaskMap[id]
    if !ok {
        return fmt.Errorf("task with ID %d not found", id)
    }
	if args.IsFinished {
		fmt.Printf("Task[%d] has been finished\n", id)
		task.TaskState = Finished
	} else {
		fmt.Printf("Task[%d] has not been finished, resetting to Waiting\n", id)
		task.TaskState = Waiting
		delete(c.TaskMap, task.TaskId)
		// Optionally, re-enqueue the task
		if task.TaskType == MapTask{
			log.Printf("re push task into map channel")
			c.MapTaskChannel <- task
		}
		if task.TaskType == ReduceTask {
			c.ReduceTaskChannel <- task
			log.Printf("re push task into reduce channel")
		}
	}
    return nil
}

//检查task map中所有的map任务是否都完成
func (c *Coordinator) checkMapTaskDone() bool{
	count := 0
	for _, task := range c.TaskMap {
		if task.TaskType == MapTask{
			if task.TaskState == Finished{
				count++
			}
		}
	}
	return count == c.MapperNum
}

//检查task map中所有的reduce任务是否都完成
func (c *Coordinator) checkReduceTaskDone() bool{
	count := 0
	for _, task := range c.TaskMap {
		if task.TaskType == ReduceTask{
			if task.TaskState == Finished{
				count++
			}
		}
	}
	return count == c.ReduceNum
}

func (c *Coordinator) toNextStage(){
	switch c.CurrentPhase{
	case MapPhase:
		c.CurrentPhase = ReducePhase
		//Map阶段done后进入Reduce阶段，要生成Reduce任务
		c.MakeReduceTask()
	case ReducePhase:
		c.CurrentPhase = Done
	}
}


//
// start a thread that listens for RPCs from worker.go
//
//基于UNIX域套接字的RPC，使用HTTP协议通信
func (c *Coordinator) server() {
	rpc.Register(c)     //注册RPC服务
	// rpc.RegisterName("AssignTask", CoordinatorRPC(c))
	// rpc.RegisterName("UpdateTaskState", CoordinatorRPC(c))
	rpc.HandleHTTP()    //RPC使用HTTP协议处理
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	mu.Lock()
	defer mu.Unlock()
	
	if c.CurrentPhase == Done{
		ret = true
	}
	return ret
}

//生成Map任务
func (c *Coordinator) MakeMapTask(files []string) {
	log.Printf("begin to make map task\n")
	for _, file := range files {
		input := []string{file}
		id := c.GenerateTaskId()
		mapTask := Task {
			TaskId: id,
			TaskType: MapTask,
			TaskState: Waiting,
			StartTime: time.Now(),
			InputFile: input,
			ReduceKth: -1,
			ReduceNum: c.ReduceNum,
		}
		log.Printf("make a map task for file: %s, task id: %d\n", file, id)
		c.MapTaskChannel <- &mapTask
	}
}

//生成Reduce任务
func (c *Coordinator) MakeReduceTask(){
	log.Printf("begin to make reduce task\n")
	//Map生成的中间文件命名规范  mr-X-Y
	//Y就是应该分配给哪个reduce
	dir,_ := os.Getwd()
	files, err := os.ReadDir(dir)
	if err != nil {
		log.Printf("read dir error, dir: %s, error: %s", dir, err.Error())
		return
	}
	for i:=0; i < c.ReduceNum; i++ {
		id := c.GenerateTaskId()
		var input []string //获取mr-X-Y中，某个Y对应的全部文件（Y就是i）
		for _, file := range files {
			name := file.Name()
			if strings.HasPrefix(name, "mr-") && strings.HasSuffix(name, strconv.Itoa(i)) {
				input = append(input, name)
			}
		}
		result := strings.Join(input, ";")
		reduceTask := Task{
			TaskId: id,
			TaskType: ReduceTask,
			TaskState: Waiting,
			StartTime: time.Now(),
			InputFile: input,
			ReduceKth: i,
			ReduceNum: c.ReduceNum,
		}
		log.Printf("make a reduce task for file, task id: %d, file name: %s\n", id, result)
		c.ReduceTaskChannel <- &reduceTask
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Printf("begin to make coordinator\n")
	c := Coordinator{
		CurrentPhase: MapPhase,
		TaskIdForGen: 0,
		MapTaskChannel: make(chan* Task, len(files)),
		ReduceTaskChannel: make(chan* Task, nReduce),
		TaskMap: make(map[int]*Task, len(files)+nReduce),
		MapperNum: len(files),
		ReduceNum: nReduce,
	}

	//根据files生成map任务
	c.MakeMapTask(files)

	c.server()

	go c.CrashHandle()//探测并处理crash的协程
	return &c
}

//如果worker发生crash（这里假定10秒没完成），会将这个任务重置，删除TaskMap的对应条，然后放入channel中等待下次分配
func (c *Coordinator) CrashHandle() {
    for {
		time.Sleep(time.Second)
        mu.Lock()

		if c.CurrentPhase == Done {
			mu.Unlock()
			break
		}
		for _, task := range c.TaskMap {
			if task.TaskState == Working && time.Since(task.StartTime) > 10*time.Second {
				fmt.Printf("Task[%d] is crashed\n", task.TaskId)
				task.TaskState = Waiting // 更新状态
				switch task.TaskType {
				case MapTask:
					c.MapTaskChannel <- task
				case ReduceTask:
					c.ReduceTaskChannel <- task
				}
				delete(c.TaskMap, task.TaskId) // 删除taskmap
			}
		}
        mu.Unlock()
    }
}
