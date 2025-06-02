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
)

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	DoMapTask(mapf)
	DoReduceTask(reducef)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func DoMapTask(mapf func(string, string) []KeyValue) {
	for {
		//  RPC for Map task

		args := MapArgs{}
		reply := MapReply{}

		ok := call("Coordinator.MapTaskDispatch", &args, &reply)

		if ok {
			if reply.Done {
				//No more map task
				break
			}
			//Start Map Task

			file, err := os.Open(reply.File)
			if err != nil {
				log.Fatal("cannot open &v", reply.File)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.File)
			}
			file.Close()
			kva := mapf(reply.File, string(content))

			intermediate := make(map[string][]KeyValue)
			for _, kv := range kva {
				y := ihash((kv.Key)) % reply.N
				x := reply.Mid
				filename := fmt.Sprintf("mr-%d-%d", x, y)
				intermediate[filename] = append(intermediate[filename], kv)
			}

			//write the intermediate kva to local fs
			for filename, kvs := range intermediate {
				// Create or truncate the file
				file, err := os.Create(filename)
				if err != nil {
					fmt.Printf("cannot create file %v: %v\n", filename, err)
					continue
				}

				// Use JSON encoder to write slice of KeyValue
				enc := json.NewEncoder(file)
				for _, kv := range kvs {
					if err := enc.Encode(&kv); err != nil {
						fmt.Printf("cannot encode kv %v: %v\n", kv, err)
					}
				}

				file.Close()
			}

		} else {
			fmt.Printf("call failed!\n")
		}
		//

	}
}

func DoReduceTask(reducef func(string, []string) string) {
	for {
		args := ReduceArgs{}
		reply := ReduceReply{}

		ok := call("Coordinator.ReduceTaskDispatch", &args, &reply)
		if ok {
			if reply.Done {
				break
			}

			intermediate := []KeyValue{}
			for i := 0; i < reply.M; i++ {

				filename := fmt.Sprintf("mr-%d-%d", i, reply.Nid)
				//decode TODO
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open file %v: %v", filename, err)
				}
				defer file.Close()
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}

			//sort

			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", reply.Nid)
			ofile, _ := os.Create(oname)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
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

		} else {
			fmt.Printf("call failed!\n")
		}
	}

}

// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
