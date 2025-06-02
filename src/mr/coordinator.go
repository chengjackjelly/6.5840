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

type Coordinator struct {
	// Files: list of file names
	Files []string
	// M: number of left Map tasks
	LeftM int
	// N: number of Reduce tasks
	N int

	LeftN int

	M int

	// All Map Task has been done
	MDone bool

	mu sync.Mutex
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
func (c *Coordinator) MapTaskDone() {

}

func (c *Coordinator) ReduceTaskDispatch(args *ReduceArgs, reply *ReduceReply) error {

	for c.LeftM > 0 {
		//Map tasks havent all done, put this thread into sleep
		time.Sleep(time.Second)
	}

	c.mu.Lock()
	if c.LeftN > 0 {
		reply.Done = false
		reply.M = c.M
		reply.Nid = c.LeftN - 1
		c.LeftN = c.LeftN - 1
	} else {
		reply.Done = true
	}
	c.mu.Unlock()
	return nil
}
func (c *Coordinator) MapTaskDispatch(args *MapArgs, reply *MapReply) error {
	//use lock to protect c to avoid race condition
	c.mu.Lock()
	if c.LeftM > 0 {
		reply.Done = false
		reply.N = c.N
		reply.Mid = c.LeftM - 1
		reply.File = c.Files[c.LeftM-1]

		c.LeftM = c.LeftM - 1
	} else {
		reply.Done = true
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Save files into the attr of coordinator struct
	// save files = > c.Files
	// number of files will be the numbers of map task while each map task only handle one file
	// save len(files) = > c.M
	// save nReduce => c.N
	c.Files = files
	c.LeftM = len(files)
	c.M = len(files)
	c.N = nReduce
	c.LeftN = nReduce
	c.server()
	return &c
}
