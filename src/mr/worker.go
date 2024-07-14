package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "strings"
import "bufio"
import "time"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	
	while(true) {
		//发起请求
		re := CallTask()
		switch re.Answer {
		case TaskGot://分到任务
			task := re.TaskReply
			switch task {
			case MapTask:
				fmt.Println("worker get a map task, task id: %d", task.TaskId)
				PerformMapTask(mapf, &task)
				FinshReport(task.TaskId)
			case ReduceTask:
				fmt.Println("worker get a reduce task, task id: %d", task.TaskId)
				PerformReduceTask(reducef, &task)
				FinshReport(task.TaskId)
			}
		}
		case NoTaskNow: //没获取到任务，等1s后再次请求
			time.sleep(time.Second)
		
		case Finish
			break;
		case
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//向master发起RPC请求，获取task
func CallTask() TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		fmt.Println("call rpc assign task failed")
	}
	return reply
}

//worker完成任务后调RPC告知master，并修改任务状态
func FinshReport(id int){
	args := FinArgs{TaskId: id}
	reply := FinReply{}

	ok := call("Coordinator.UpdateTaskState", &args, &reply)
	if !ok {
		fmt.Println("call rpc update task state failed")
	}
	return reply
}

//mapf在wc.go中定义，由于在package main，所以作为参数传入...(不合理的地方)
//RPC请求获取task后,调用mapf函数然后生成中间文件mr-X-Y
func PerformMapTask(mapf func(string, string) []KeyValue, task *Task) {
	intermediate := []KeyValue{}
	for _, filename := range task.InputFile {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	sort.Sort(ByKey(intermediate))

	//中间文件的命名是 mr-X-Y,  X是Map的task id, Y是ihash(key)*
	count := task.ReduceNum
	for i:=0;i<count;i++{
		midFileName := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		midFile, _:= os.Create(midFileName)
	}
    fileMap := make(map[int]*bufio.Writer)
    fileHandles := make(map[int]*os.File)

    for _, word := range intermediate {
        index := int(ihash(word.Key)) % count
        midFileName := fmt.Sprintf("mr-%d-%d", taskID, index)

        // 检查是否已经打开了文件
        if _, ok := fileMap[index]; !ok {
            file, err := os.OpenFile(midFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
            if err != nil {
                return err
            }
            fileHandles[index] = file
            fileMap[index] = bufio.NewWriter(file)
        }

        // 写入数据
        writer := fileMap[index]
        _, err := writer.WriteString(fmt.Sprintf("{%s,%d}\n", word.Key, word.Value))
        if err != nil {
            return err
        }
    }

    // 清理：关闭所有文件
    for _, writer := range fileMap {
        writer.Flush()
    }
    for _, file := range fileHandles {
        file.Close()
    }
}

//reduce的工作是读取中间文件的kv，合并后排序
func PerformReduceTask(mapf func(string, string) []KeyValue, task *Task) {

	intermediate := []KeyValue{}
	oname := "mr-out-" + strconv.Itoa(task.ReduceKth)
	ofile, _ := os.Create(oname)
	for _, filename := range task.InputFile{
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			var key string
			var value int
			line := scanner.Text()
			// 去除花括号
			line = strings.Trim(line, "{}")
			_, err := fmt.Sscanf(line, "%s,%d", &key, &value)
			if err != nil {
				log.Fatalf("fail to parse file %v", filename)
			}
			intermediate = append(intermediate, {key, value})
		}
	}
	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
}

//UNIX域套接字进行RPC通信的example -- client
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
