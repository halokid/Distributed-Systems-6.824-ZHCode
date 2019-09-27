package mapreduce

import (
  "debug/macho"
  "fmt"
  "net"
  "sync"
)

/**

// --------------- old source ------------------------------

import "container/list"
import "fmt"

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
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  return mr.KillWorkers()
}

*/



type Master struct {
  // master holds all the state that the master needs to keep track of
  sync.Mutex

  address               string
  doneChannel           chan bool

  newCond               *sync.Cond
  workers               []string

  jobName               string
  files                 []string
  nReduce               int

  shutdown              chan struct{}
  l                     net.Listener
  stats                 []int
}


func newMaster(master string) (mr *Master) {
  // 采用自举返回的方式， 就是返回参数这里定义了一个变量，然后函数逻辑是定义变量本身的数据
  mr = new(Master)
  mr.address = master
  mr.shutdown = make(chan struct{})
  mr.newCond = sync.NewCond(mr)
  mr.doneChannel = make(chan bool)
  return
}

func (mr *Master) run(jobName string, files []string, nreduce int,
                      schedule func(phase jobPhase),
                      finish func()) {
  mr.jobName = jobName
  mr.files = files
  mr.nReduce = nreduce

  fmt.Printf("%s: 开始 Map/Reduce 任务 %s\n", mr.address, mr.jobName)

  schedule(mapPhase)
  schedule(reducePhase)
  finish()

  mr.mer
}

func Sequential(jobName string, files []string, nreduce int,
                mapF func(string, string) []KeyValue,
                reduceF func(string, []string) string ) (mr *Master) {
  // 按顺序运行map 和 reduce 的任务， 并且等待任务完成，然后运行接下来的逻辑
  mr = newMaster("master")
  go mr.run(jobName, files, nreduce, )
}


























