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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	for {
		args := ExampleArgs{}
		reply := ExampleReply{}
		ok := call("Master.AskTask", &args, &reply)
		if !ok {
			break //call 出错 或者所有任务都已结束 则退出循环
		}
		switch reply.State {
		case State_Is_Allfinished:
			return
		case State_Is_Map:
			//do map task
			do_maptask(mapf, &reply)
		case State_Is_Reduce:
			// do reduce task
			do_reducetask(reducef, &reply)
		case State_Is_Waiting:
			time.Sleep(1 * time.Second)
		default:
			panic(fmt.Sprintf("unexpected reply State %v", reply.State))
		}
	}

}

func do_maptask(mapf func(string, string) []KeyValue, reply *ExampleReply) {
	//intermediate := []KeyValue{} //中间键值对
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()
	intermediate := mapf(reply.Filename, string(content))
	// sort.Sort(ByKey(intermediate))

	//中间文件的生成 mr-X-Y 思路是该map生成的东西hash为nreduce个桶 即中间键值对的一行
	buckets := make([][]KeyValue, reply.Nreduce)
	for i := range buckets {
		buckets[i] = []KeyValue{}
	}
	for _, kva := range intermediate {
		buckets[ihash(kva.Key)%reply.Nreduce] = append(buckets[ihash(kva.Key)%reply.Nreduce], kva)
	}
	for i := range buckets {
		oname := "mr-" + strconv.Itoa(reply.Id) + "-" + strconv.Itoa(i) // mr-X-Y
		ofile, _ := ioutil.TempFile("", oname+"*tmp")
		enc := json.NewEncoder(ofile)
		for _, kv := range buckets[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write into %v", oname)
			}
		}
		os.Rename(ofile.Name(), oname)
		ofile.Close()
	}
	finishedArgs := ExampleArgs{reply.Id}
	finishedReply := ExampleReply{}
	call("Master.MapTaskFinished", &finishedArgs, &finishedReply)

}
func do_reducetask(reducef func(string, []string) string, reply *ExampleReply) {
	intermediate := []KeyValue{}
	for i := 0; i < reply.Nmap; i++ {
		iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.Id)
		file, err := os.Open(iname)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(reply.Id)
	ofile, _ := ioutil.TempFile("", oname+"*tmp")

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
	os.Rename(ofile.Name(), oname)
	ofile.Close()
	finishedArgs := ExampleArgs{reply.Id}
	finishedReply := ExampleReply{}
	call("Master.ReduceTaskFinished", &finishedArgs, &finishedReply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	// args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	// fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
