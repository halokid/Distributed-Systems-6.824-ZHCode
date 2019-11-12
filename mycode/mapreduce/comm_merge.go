package main

import (
  "fmt"
  "net"
  "net/rpc"
  "sync"
)

type Master struct {
  sync.Mutex

  address               string
  registerChannel       chan string
  doneChannel           chan bool
  workers               []string        // 受mutex保护


  // 每个任务的信息
  jobName       string        // 当前执行的任务名称
  files         []string      // 输入的文件
  nReduce       int           // reduce区块的数字， 就是代表正在执行第几个reduce区块

  shutdown      chan struct{}       // 用空的 struct， 省空间
  l             net.Listener
  stats         []int
}


// 表明当前的调度任务是 map 还是 reduce
type jobPhase string

const (
  mapPhase      jobPhase = "Map"
  reducePhase   jobPhase = "Reduce"
)


type DoTaskArgs struct {
  JobName         string
  File            string
  Phase           jobPhase
  TaskNumber      int

  NumOtherPhase   int
}

func Call(srv string, rpcname string, args interface{}, reply interface{}) bool {
  c, errx :=  rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()

  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }
  fmt.Println(err)
  return false
}






