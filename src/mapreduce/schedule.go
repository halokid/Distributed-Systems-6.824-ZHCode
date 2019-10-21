package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	wg := sync.WaitGroup{}
	wg.Add(ntasks)
	timeoutCh := make(chan int, ntasks)
	sendTaskToWorker := func(addr string, idx int) {
		args := DoTaskArgs{JobName: jobName, File: mapFiles[idx], Phase: phase, TaskNumber: idx, NumOtherPhase: n_other}
		done := call(addr, "Worker.DoTask", args, nil)
		if done {
			wg.Done()
		} else {
			timeoutCh <-idx
		}
		registerChan <-addr
	}

	for i := 0; i < ntasks; i++ {
		availWorker := <-registerChan
		go sendTaskToWorker(availWorker, i)
	}

	go func() {
		for {
			// 假如woker挂了, 就会超时
			// 假如有收到超时的任务（channel通道传输数据）， 则重试
			idx := <-timeoutCh
			availWokers := <-registerChan
			go sendTaskToWorker(availWokers, idx)
		}
	}()

	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}




