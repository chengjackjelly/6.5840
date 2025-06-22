package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MapTask struct {
	worker_id   int //1 - M
	status      int
	assign_file string
}
type ReduceTask struct {
	worker_id int // 1 - N
	status    int
}
type Coordinator struct {
	// DS 1  Define List of Map Task
	mapTasks    []MapTask
	reduceTasks []ReduceTask
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// this function will called by worker when they done their map task
func (c *Coordinator) MapTaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	c.mapTasks[args.Task_id].status = 2
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) ReduceTaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mu.Lock()
	c.reduceTasks[args.Task_id].status = 2
	c.mu.Unlock()
	return nil
}
func (c *Coordinator) AllMapTaskDone() bool {

	all_complete := true

	for _, _task := range c.mapTasks {
		if _task.status != 2 {
			all_complete = false
			break
		}
	}
	return all_complete
}
func (c *Coordinator) ReduceTaskDispatch(args *ReduceArgs, reply *ReduceReply) error {

	for !c.AllMapTaskDone() {
		time.Sleep(time.Second)
	}

	c.mu.Lock()
	task_id := -1
	for i, _task := range c.reduceTasks {
		if _task.status == 0 {
			//
			task_id = i
			break
		}
	}
	if task_id == -1 {
		reply.Done = true
	} else {
		reply.Done = false
		c.reduceTasks[task_id].status = 1
		reply.M = len(c.mapTasks)
		reply.Nid = task_id
	}
	c.mu.Unlock()

	return nil
}
func (c *Coordinator) MapTaskDispatch(args *MapArgs, reply *MapReply) error {
	//use lock to protect c to avoid race condition
	c.mu.Lock()
	task_id := -1
	for i, _task := range c.mapTasks {
		if _task.status == 0 {
			//
			task_id = i
			break
		}
	}
	if task_id == -1 {
		reply.Done = true
	} else {
		c.mapTasks[task_id].status = 1
		reply.Done = false
		reply.File = c.mapTasks[task_id].assign_file
		reply.Mid = task_id
		reply.N = len(c.reduceTasks)
	}

	c.mu.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	ret := true
	for _, _task := range c.reduceTasks {
		if _task.status != 2 {
			ret = false
			break
		}
	}

	return ret
}

func (c *Coordinator) InitMapTasks(files []string, nReduce int) {
	c.mapTasks = make([]MapTask, len(files))
	for i, file := range files {
		c.mapTasks[i] = MapTask{
			worker_id:   -1,
			status:      0, //0 = idle, 1=in-progress, 2= complete
			assign_file: file,
		}
	}
}

func (c *Coordinator) InitReduceTasks(numReduce int) {
	c.reduceTasks = make([]ReduceTask, numReduce)
	for i := 0; i < numReduce; i++ {
		c.reduceTasks[i] = ReduceTask{
			worker_id: -1, // -1 = unassigned
			status:    0,  // 0 = idle, 1 = in-progress, 2=complete
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	//Init MapTasks
	c.InitMapTasks(files, nReduce)

	//Init ReduceTasks
	c.InitReduceTasks(nReduce)

	c.server()
	return &c
}
