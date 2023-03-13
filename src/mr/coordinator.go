package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// lock
var mu sync.Mutex

type Coordinator struct {
	// Your definitions here.
	reducer_num    int
	task_id        int   // record the current id, in order to get the only id
	schedule_phase Phase // the current phase of the whole map-reduce
	tasks_info     map[int]*TaskMetaInfo
	files          []string
	map_channel    chan *Task
	reduce_channel chan *Task
}

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	AllDone
)

/*type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo // map task_id to its info
}*/
type TaskMetaInfo struct {
	state      State     // state of the task
	start_time time.Time // used for crash detection
	task       *Task     // pointer to the task
}

type State int

const (
	Working State = iota
	Waiting
	Done
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
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
	// Your code here.
	mu.Lock()
	defer mu.Unlock() // defer Unlock() until this function returns
	if c.schedule_phase == AllDone {
		fmt.Println("All tasks are done, the coordinator will exit!")
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:          files,
		reducer_num:    nReduce,
		schedule_phase: MapPhase,
		map_channel:    make(chan *Task, len(files)),
		reduce_channel: make(chan *Task, nReduce),
		tasks_info:     make(map[int]*TaskMetaInfo, len(files)+nReduce), // total # of tasks=len(files)+nReduce
	}
	// Your code here.
	c.makeMapTasks(files)

	c.server()
	// go c.crashDetect()
	return &c
}

// create map tasks and put them into the map channel
func (c *Coordinator) makeMapTasks(files []string) {
	// for each input file
	for _, f := range files {
		// create a map task
		id := c.generateTaskID()
		task := Task{
			Task_type:   MapTask,
			Id:          id,
			Reducer_num: c.reducer_num,
			Filename:    []string{f},
		}
		// store the info of the task
		info := TaskMetaInfo{
			state: Waiting, // waiting to be executed
			task:  &task,
		}
		c.acceptInfo(&info)    // put the info into the map structure
		c.map_channel <- &task // put the task into the channel
	}
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.reducer_num; i++ {
		id := c.generateTaskID()
		task := Task{
			Task_type:   ReduceTask,
			Id:          id,
			Reducer_num: c.reducer_num,
			Filename:    reduceFiles(i),
		}

		info := TaskMetaInfo{
			state: Waiting,
			task:  &task,
		}
		c.acceptInfo(&info) // store the task's info
		c.reduce_channel <- &task
	}
}

func (c *Coordinator) generateTaskID() int {
	id := c.task_id
	c.task_id++ // increment the task_id in the struct
	return id
}

// accept new info into the info map
func (c *Coordinator) acceptInfo(info *TaskMetaInfo) {
	id := info.task.Id
	index, _ := c.tasks_info[id]
	if index != nil {
		fmt.Println("Meta contains task whose id=", id)
		return
	}
	c.tasks_info[id] = info // put info into the map
}

// the worker calls this function to ask for a new task
// distribute tasks
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	// lock to avoid competence between workers
	mu.Lock()
	defer mu.Unlock()

	switch c.schedule_phase {
	case MapPhase:
		{
			if len(c.map_channel) > 0 { // the map channel still has tasks
				*reply = *<-c.map_channel
				if !c.judgeState(reply.Id) { // the task is running already
					fmt.Printf("Map-task id [ %d ] is already running\n", reply.Id)
				}
			} else { // no more map tasks
				reply.Task_type = WaitingTask // set task type to "waiting"
				// all map tasks done, move on to reduce
				if c.checkPhaseDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.reduce_channel) > 0 {
				*reply = *<-c.reduce_channel
				if !c.judgeState(reply.Id) {
					fmt.Printf("Reduce-task id [ %d ] is already running\n", reply.Id)
				} else {
					reply.Task_type = WaitingTask
					if c.checkPhaseDone() {
						c.toNextPhase()
					}
					return nil
				}
			}
		}
	case AllDone:
		{
			reply.Task_type = ExitTask
		}
	}
	return nil
}

// judge if the task is running, return true if it is
func (c *Coordinator) judgeState(task_id int) bool {
	info, ok := c.tasks_info[task_id]
	if !ok || info.state != Waiting {
		return false
	}
	info.state = Working         // update the state
	info.start_time = time.Now() // record start time
	return true
}

// check if the tasks are all done
func (c *Coordinator) checkPhaseDone() bool {
	var (
		map_done_num      = 0
		map_undone_num    = 0
		reduce_done_num   = 0
		reduce_undone_num = 0
	)
	// calculate the number of done and undone tasks
	for _, t := range c.tasks_info {
		// judge the type of task
		if t.task.Task_type == MapTask {
			if t.state == Done {
				map_done_num++
			} else {
				map_undone_num++
			}
		} else if t.task.Task_type == ReduceTask {
			if t.state == Done {
				reduce_done_num++
			} else {
				reduce_undone_num++
			}
		}
	}

	// map tasks all done, and reduce tasks not started yet
	if (map_done_num > 0 && map_undone_num == 0) && (reduce_done_num == 0 && reduce_undone_num == 0) {
		return true
	} else { // reduce tasks all done
		if reduce_done_num > 0 && reduce_undone_num == 0 {
			return true
		}
	}
	return false
}

// move to the next phase
func (c *Coordinator) toNextPhase() {
	if c.schedule_phase == MapPhase {
		// move from MapPhase to ReducePhase
		c.makeReduceTasks()
		c.schedule_phase = ReducePhase
	} else if c.schedule_phase == ReducePhase {
		// move from ReducePhase to AllDone
		c.schedule_phase = AllDone
	}
}

// given a reduce num i, returns the files mr-tmp-(task-id)-i
func reduceFiles(reduce_num int) []string {
	var res []string
	path, _ := os.Getwd() // get the current rooted path
	files, _ := ioutil.ReadDir(path)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "mr-tmp") && strings.HasSuffix(f.Name(), strconv.Itoa(reduce_num)) {
			res = append(res, f.Name())
		}
	}
	return res
}

// the worker calls this function to report that a task is finished
func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch args.Task_type {
	case MapTask:
		{
			meta, ok := c.tasks_info[args.Id]
			if !ok || meta.state != Working {
				fmt.Printf("Map task id [ %d ] is finished already\n", args.Id)
			} else {
				meta.state = Done // update the state
			}
			return nil
		}
	case ReduceTask:
		{
			meta, ok := c.tasks_info[args.Id]
			if !ok || meta.state != Working {
				fmt.Printf("Reduce task id [ %d ] is finished already\n", args.Id)
			} else {
				meta.state = Done
			}
			return nil
		}
	}
	return nil
}

func (c *Coordinator) crashDetect() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.schedule_phase == AllDone {
			mu.Unlock()
			break
		}

		for _, t := range c.tasks_info {
			if t.state == Working && time.Since(t.start_time) > 9*time.Second {
				fmt.Printf("Task [ %d ] is crash, take [ %d ] s\n", t.task.Id, time.Since(t.start_time))
				switch t.task.Task_type {
				case MapTask:
					{
						t.state = Waiting
						c.map_channel <- t.task // reput it into the channel
					}
				case ReduceTask:
					{
						t.state = Waiting
						c.reduce_channel <- t.task
					}
				}
			}
		}
		mu.Unlock()
	}
}
