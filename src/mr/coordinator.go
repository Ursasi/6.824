package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.

	nReduce        int
	taskQueue      chan Task
	taskWg         sync.WaitGroup
	taskIdCounter  int
	taskNotifyMap  map[int]chan struct{}
	taskNotifyLock sync.Mutex
	done           bool
	doneLock       sync.Mutex
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

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.doneLock.Lock()
	defer c.doneLock.Unlock()
	return c.done
}

// NewTaskId 获得一个新的 taskId
func (c *Coordinator) NewTaskId() int {
	c.taskIdCounter++
	return c.taskIdCounter
}

// NeedWork NewTaskId 获得一个新的 task
func (c *Coordinator) NeedWork(args *NeedWorkArgs, reply *NeedWorkReply) error {
	if c.Done() {
		return fmt.Errorf("job is done")
	}
	t, ok := <-c.taskQueue
	if !ok {
		return fmt.Errorf("cannot get task from taskQueue")
	}
	reply.T = t
	reply.ReduceCnt = c.nReduce
	c.taskNotifyLock.Lock()
	c.taskNotifyMap[t.TaskId] = make(chan struct{})
	c.taskNotifyLock.Unlock()

	go func(taskChan chan struct{}) {
		select {
		case <-taskChan:
			return
		case <-time.After(10 * time.Second):
			c.taskQueue <- t
		}
	}(c.taskNotifyMap[t.TaskId])

	return nil
}

// FinishWork FinishWork RPC: 用于告知 Coordinator 任务已完成
func (c *Coordinator) FinishWork(args *FinishWorkArgs, reply *FinishWorkReply) error {
	taskId := args.TaskId
	c.taskNotifyLock.Lock()
	defer c.taskNotifyLock.Unlock()
	notifyChan, ok := c.taskNotifyMap[taskId]
	if !ok {
		return fmt.Errorf("invalid taskId %d", taskId)
	}
	notifyChan <- struct{}{}
	c.taskWg.Done()
	delete(c.taskNotifyMap, taskId)
	return nil
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:       nReduce,
		taskQueue:     make(chan Task, 100),
		taskWg:        sync.WaitGroup{},
		taskIdCounter: 0,
		taskNotifyMap: make(map[int]chan struct{}),
		done:          false,
	}

	c.taskWg.Add(len(files))

	go func(files []string) {
		for _, filename := range files {
			c.taskQueue <- NewMapTask(filename, c.taskIdCounter)
		}
		c.taskWg.Wait()
		c.taskWg.Add(nReduce)
		for i := 0; i < nReduce; i++ {
			c.taskQueue <- NewReduceTask(i, c.taskIdCounter)
		}
		go func() {
			c.taskWg.Wait()
			c.doneLock.Lock()
			c.done = true
			c.doneLock.Unlock()
		}()
	}(files)

	c.server()
	return &c
}
