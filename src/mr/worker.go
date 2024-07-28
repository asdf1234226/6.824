package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "time"
import "io/ioutil"
import "sort"
import "strconv"
import "encoding/json"
//
// Map functions return a slice of KeyValue.
//
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

    for {
		//CallExample()
		//time.Sleep(time.Second)
		//os.Exit(0) // 正常退出程序
        // 发起请求
        re := CallTask()
        switch re.Answer {
        case TaskGot: // 分到任务
            task := re.TaskReply
			var err error
            switch task.TaskType {
            case MapTask:
                fmt.Printf("worker get a map task, task id: %d\n", task.TaskId)
                err = PerformMapTask(mapf, &task)
				if err != nil{
                	fmt.Printf("error encountered in map task: %v, requesting new task\n", err)
                    FinishReport(task.TaskId, false)
					continue // 发生错误时忽略并继续请求新任务
				}
				FinishReport(task.TaskId, true)
                
            case ReduceTask:
                fmt.Printf("worker get a reduce task, task id: %d\n", task.TaskId)
                err = PerformReduceTask(reducef, &task)
                if err != nil {
                    fmt.Printf("error encountered in reduce task: %v, requesting new task\n", err)
                    FinishReport(task.TaskId, false)
					continue // 发生错误时忽略并继续请求新任务
                }
                FinishReport(task.TaskId, true)
            }
        case NoTaskNow: // 没获取到任务，等1s后再次请求
            time.Sleep(time.Second)
        case Finish:
			os.Exit(0)
        }
    }
}

//向master发起RPC请求，获取task
func CallTask() TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}
	log.Printf("try to call task")
	ok := call("Coordinator.AssignTask", &args, &reply)
	if !ok {
		fmt.Println("call rpc assign task failed")
	}
	return reply
}

//worker完成任务后调RPC告知master，并修改任务状态
func FinishReport(id int, isFinished bool){
	args := FinArgs{TaskId: id, IsFinished: isFinished}
	reply := FinReply{}

	ok := call("Coordinator.UpdateTaskState", &args, &reply)
	if !ok {
		fmt.Println("call rpc update task state failed")
	}
}

//mapf在wc.go中定义，由于在package main，所以作为参数传入...(不合理的地方)
//RPC请求获取task后,调用mapf函数然后生成中间文件mr-X-Y
func PerformMapTask(mapf func(string, string) []KeyValue, task *Task) (err error){
    defer func() {
        if r := recover(); r != nil {
            fmt.Printf("Recovered in PerformMapTask: %v\n", r)
            err = fmt.Errorf("map task failed: %v", r)
        }
    }()

	intermediate := []KeyValue{}
	all_file := ""
	for _, filename := range task.InputFile {
		all_file = all_file + filename
		//log.Printf("perform map task, file: %s", filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return err
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
			return err
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
		log.Printf("kva size: %d", len(kva))
	}
	log.Printf("map task, file name: %s, task id: %d", all_file, task.TaskId)
	sort.Sort(ByKey(intermediate))

	//中间文件的命名是 mr-X-Y,  X是Map的task id, Y是ihash(key)*
	rn := task.ReduceNum

	// 先创建所有需要的文件，并为每个文件创建一个JSON编码器
	encoders := make([]*json.Encoder, rn)
	files := make([]*os.File, rn)
	
	for i := 0; i < rn; i++ {
		midFileName := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		file, err := os.Create(midFileName)
		if err != nil {
			log.Fatalf("Failed to create file %s: %v", midFileName, err)
			return err
		}
		files[i] = file
		encoders[i] = json.NewEncoder(file)
	}
	
	// 遍历intermediate数组，根据哈希值将每个kv写入对应的文件
	for _, kv := range intermediate {
		index := ihash(kv.Key) % rn
		err := encoders[index].Encode(&kv)
		if err != nil {
			log.Fatalf("Failed to encode kv: %v", err)
			return err
		}
	}
	
	// 关闭所有文件
	for _, file := range files {
		if err := file.Close(); err != nil {
			log.Fatalf("Failed to close file: %v", err)
			return err
		}
	}
	return nil
}

//reduce的工作是读取中间文件的kv，合并后排序
func PerformReduceTask(reducef func(string, []string) string, task *Task)(err error) {
    defer func() {
        if r := recover(); r != nil {
            fmt.Printf("Recovered in PerformMapTask: %v\n", r)
            err = fmt.Errorf("map task failed: %v", r)
        }
    }()
	intermediate := []KeyValue{}
	for _, filename := range task.InputFile{
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			return err
		}
		// 反序列化JSON格式文件
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			// func (dec *Decoder) Decode(v interface{}) error
			// Decode从输入流读取下一个json编码值并保存在v指向的值里
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			intermediate = append(intermediate, kv) // 将中间文件的每组kv都写入kva
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	//MapReduce论文提到了使用临时文件并在完成写入后原子地重命名它的技巧
	//确保数据的完整性 -- 只有所有数据都成功写入，才会被重命名为最后的输出文件
	// 文件重命名是原子操作
	dir, _ := os.Getwd()
	tmpfile, err := os.CreateTemp(dir, "mr-out-tmpfile-")
	if err != nil {
		log.Fatal("failed to create temp file", err)
		return err
	}
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
		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tmpfile.Close()
	oname := "mr-out-" + strconv.Itoa(task.ReduceKth)
	os.Rename(dir+tmpfile.Name(), dir+oname)
	return nil
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
	log.Printf("call failed: %s", err.Error())
	//fmt.Println(err)
	return false
}
