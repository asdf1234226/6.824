package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"

type TaskType int
const (
	MapTask TaskType = itoa
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
}

type Phase int
const (
	MapPhase Phase = Itoa   //map阶段
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

func (c * Coordinator) GenerateTaskId() int {
	return ++(c.TaskIdForGen)
}
// Your code here -- RPC handlers for the worker to call.

//
// RPC中暴露给client的方法
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	switch c.CurrentPhase {
	case MapPhase:
		if len(c.MapTaskChannel) > 0 {
			taskp := <- c.MapTaskChannel
			if taskp.TaskState == Waiting {
				reply.Task = *taskp
				task.TaskState = Working
				task.StartTime = time.Now()
				c.TaskMap[*(taskp).TaskId] = taskp
				fmt.Printf("Task[%d] has been assigned\n", taskp.TaskId)
			}
		} else {
			if c.checkMapTaskDone(){
				c.toNextStage()
			}
			return nil
		}

	case ReducePhase:
		if len(c.ReduceTaskChannel) > 0 {
			taskp := <- c.ReduceTaskChannel
			if taskp.TaskState == Waiting {
				reply.Task = *taskp
				task.TaskState = Working
				task.StartTime = time.Now()
				c.TaskMap[*(taskp).TaskId] = taskp
				fmt.Printf("Task[%d] has been assigned\n", taskp.TaskId)
			}
		} else {
			if c.checkReduceTaskDone(){
				c.toNextStage()
			}
			return nil
		}
	
	case Done:
		fmt.Println("all task have been finishe")
	
	default:
		fmt.Println("undefined phase")
	}
	return nil
}

//检查task map中所有的map任务是否都完成
func (c *Coordinator) checkMapTaskDone() bool{
	count := 0
	for _, task := range c.TaskMap {
		if task.TaskType == MapTask && task.TaskState == Finished{
			count++;
		}
	}
	if(count == c.MapperNum){
		return true;
	}
	return false;
}

//检查task map中所有的reduce任务是否都完成
func (c *Coordinator) checkReduceTaskDone() bool{
	count := 0
	for _, task := range c.TaskMap {
		if task.TaskType == ReduceTask && task.TaskState == Finished{
			count++;
		}
	}
	if(count == c.ReduceNum){
		return true;
	}
	return false;
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

	// Your code here.


	return ret
}

//生成Map任务
func (c *Coordinator) MakeMapTask(files []string) {
	log.Printf("begin to make map task\n")
	for _, file := range files {
		input := []string{file}
		id := c.GenerateTaskId
		mapTask := Task {
			TaskId: id,
			TaskType: MapTask,
			TaskState: Waiting,
			StartTime: time.Now(),
			InputFile: input,
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
	for i:=0; i < c.nReduce; i++ {
		id := c.GenerateTaskId()
		var input []string //获取mr-X-Y中，某个Y对应的全部文件（Y就是i）
		for _, file := range files {
			name := file.Name()
			if strings.HasPrefix(name, "mr-") && strings.HasSuffix(name, strconv.Itoa(i)) {
				input = append(input, name)
			}
		}
		reduceTask := Task{
			TaskId: id,
			TaskType: ReduceTask,
			TaskState: Waiting,
			StartTime: time.Now(),
			InputFile: input,
		}
		log.Printf("make a map task for file: %s, task id: %d\n", file, id)
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
		TaskMap: make(map[int]*Task),
		MapperNum: len(files),
		ReduceNum: nReduce,
	}

	//根据files生成map任务
	m.MakeMapTask(files)

	c.server()
	return &c
}
