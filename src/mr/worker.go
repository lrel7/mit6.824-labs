package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// used for sorting
type SortedKey []KeyValue

func (k SortedKey) Len() int           { return len(k) }
func (k SortedKey) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k SortedKey) Less(i, j int) bool { return k[i].Key < k[j].Key }

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

	// Your worker implementation here.
	// keep on asking for a new task
	keepFlag := true
	for keepFlag {
		task := GetTask() // get a new task
		switch task.Task_type {
		case MapTask:
			{
				DoMapTask(mapf, &task) // do the map task
				callDone(&task)        // report that the task is done
			}
		case WaitingTask:
			{
				time.Sleep(time.Second * 5)
			}
		case ReduceTask:
			{
				DoReduceTask(reducef, &task)
				callDone(&task)
			}
		case ExitTask:
			{
				time.Sleep(time.Second)
				fmt.Println("All tasks are done, will be exiting")
				keepFlag = false
			}
		}
	}
	time.Sleep(time.Second)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

// the worker calls this function to get a task from the coordinator
func GetTask() Task {
	args := TaskArgs{}
	reply := Task{}
	call("Coordinator.PollTask", &args, &reply)
	return reply
}

func DoMapTask(mapf func(string, string) []KeyValue, task *Task) {
	// used to store the results of map function
	var intermediate []KeyValue
	// map only has 1 file input
	filename := task.Filename[0]

	// open and read the file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read %v", filename)
	}
	file.Close()

	// call map function
	intermediate = mapf(filename, string(content))
	reduceN := task.Reducer_num
	hashed_kv := make([][]KeyValue, reduceN)
	// go through intermediate, hash the key-value pairs into hashed_kv
	for _, kv := range intermediate {
		hashed_kv[ihash(kv.Key)%reduceN] = append(hashed_kv[ihash(kv.Key)%reduceN], kv)
	}
	// write each bucket into a tmp file
	for i := 0; i < reduceN; i++ {
		// name the file as "mr-tmp-taskid-ihash"
		oname := "mr-tmp-" + strconv.Itoa(task.Id) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range hashed_kv[i] {
			enc.Encode(kv)
		}
		ofile.Close()
	}
}

func DoReduceTask(reducef func(string, []string) string, task *Task) {
	sorted_kv := shuffle(task.Filename) // sort the key-value pairs
	dir, _ := os.Getwd()                // get the rooted path name
	// create a tmp file
	tmpfile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}

	i := 0
	for i < len(sorted_kv) {
		j := i + 1
		// merge the same keys
		for j < len(sorted_kv) && sorted_kv[j].Key == sorted_kv[i].Key {
			j++
		}
		var values []string // save values of the same key
		for k := i; k < j; k++ {
			values = append(values, sorted_kv[k].Value)
		}
		// the reduce function use values to generate a new key-value pair
		output := reducef(sorted_kv[i].Key, values)
		// print the output into the tmp file
		fmt.Fprintf(tmpfile, "%v %v\n", sorted_kv[i].Key, output)
		i = j
	}
	tmpfile.Close()
	// rename the file
	rename := fmt.Sprintf("mr-out-%d", task.Id)
	os.Rename(tmpfile.Name(), rename)
}

// returns a sorted key-value[]
func shuffle(files []string) []KeyValue {
	var res []KeyValue
	// read all the key-value pairs
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			res = append(res, kv)
		}
		file.Close()
	}
	// sort the pairs according to keys
	sort.Sort(SortedKey(res))
	return res
}

// call the rpc to mark the task as done
func callDone(task *Task) {
	args := task
	reply := Task{}
	call("Coordinator.MarkFinished", &args, &reply)
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
