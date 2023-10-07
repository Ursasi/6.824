package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// Len for sorting by key.
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

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		needWorkReply := NeedWorkReply{}
		ok := call("Coordinator.NeedWork", &NeedWorkArgs{}, &needWorkReply)
		if !ok {
			break
		}
		switch needWorkReply.T.Type {
		case Map:
			filename := needWorkReply.T.Filename
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
				return
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
				return
			}
			file.Close()
			kva := mapf(filename, string(content))
			intermidiate := make([][]KeyValue, needWorkReply.ReduceCnt)
			for _, kv := range kva {
				reduceId := ihash(kv.Key) % needWorkReply.ReduceCnt
				intermidiate[reduceId] = append(intermidiate[reduceId], kv)
			}
			for i := 0; i < needWorkReply.ReduceCnt; i++ {
				ofilename := fmt.Sprintln("mr-%d-%d", needWorkReply.T.TaskId, i)
				tf, _ := os.CreateTemp("./", ofilename)
				enc := json.NewEncoder(tf)
				for _, kv := range intermidiate[i] {
					enc.Encode(&kv)
				}
				tf.Close()
				os.Rename(tf.Name(), ofilename)
			}
			return
		case Reduce:
			var filenames []string
			files, err := os.ReadDir(".")
			if err != nil {
				log.Fatalf("cannot read dir")
				return
			}
			for _, file := range files {
				if file.IsDir() {
					continue
				}
				filename := file.Name()
				prefix := "mr-"
				suffix := fmt.Sprintf("-%d", needWorkReply.T.ReduceId)
				if strings.HasPrefix(filename, prefix) && strings.HasSuffix(filename, suffix) {
					filenames = append(filenames, filename)
				}
			}

			var kva []KeyValue
			for _, filename := range filenames {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
					return
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}

			sort.Sort(ByKey(kva))
			oname := fmt.Sprintf("mr-out-%d", needWorkReply.T.ReduceId)
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			ofile.Close()
		}
		call("Coordinator.FinishWork", &FinishWorkArgs{TaskId: needWorkReply.T.TaskId}, &FinishWorkReply{})
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
