package mapreduce

import (
	"container/list"
	"fmt"
	"sync"
)

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

//Send RPC to Worker
//To handle worker failures, re-assign the job given to the failed worker
//to another worker
func (mr *MapReduce) DeliverJobs(wk string, args *DoJobArgs) {
	var reply DoJobReply
	ok := call(wk, "Worker.DoJob", args, &reply)
	if ok == false {
		fmt.Printf("DoWork: RPC %s deliver jobs error\n", wk)
		delete(mr.Workers, wk)
		wk := mr.GetWorker()
		mr.DeliverJobs(wk, args)
	} else {
		mr.registerChannel <- wk
	}
}

//Get next worker in the registerChannel
//Use a mutex to avoid two threads use workers at same time
//Seems that removing mutex also works
func (mr *MapReduce) GetWorker() string {
	var mutex sync.Mutex
	wk := <-mr.registerChannel
	mutex.Lock()
	mr.Workers[wk] = &WorkerInfo{wk}
	mutex.Unlock()
	return wk
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	mr.Workers = make(map[string]*WorkerInfo)

	for i := 0; i < mr.nMap; i++ {
		wk := mr.GetWorker()
		args := &DoJobArgs{mr.file, Map, i, mr.nReduce}
		go mr.DeliverJobs(wk, args)
	}

	for i := 0; i < mr.nReduce; i++ {
		wk := mr.GetWorker()
		args := &DoJobArgs{mr.file, Reduce, i, mr.nMap}
		go mr.DeliverJobs(wk, args)
	}

	return mr.KillWorkers()
}
