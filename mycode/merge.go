package main

import "fmt"

func (mr *Master) Merge(phase jobPhase) {
  // merge 其实就是一个 schedule 过程， 所以这个函数也可以叫 schedule
  var ntasks int
  var nios int      // number of inputs(for reduce) or output (for map)

  switch phase {
  case mapPhase:
    ntasks = len(mr.files)
    nios = mr.nReduce         // map的阶段输出的数据是 reduce的数值 个 切割好的文件
  case reducePhase:
    ntasks = mr.nReduce
    nios = len(mr.files)      // reduce的阶段输出的数据是 归并好了的(分类，计数等规则） map数值个文件（也就是 mr.files)
  }

  fmt.Printf("调度: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)


  done := make(chan bool)
  for i := 0; i < ntasks; i++ {
    go func(number int) {
      args := DoTaskArgs{mr.jobName, mr.files[number], phase, number, nios}
      var worker string
      reply := new(struct{})
      ok := false
      for ok != true {
        worker = <-mr.registerChannel
        // todo: 注意这种一直检查 ok 的值的写法
        ok = Call(worker, "Worker.DoTask", args, reply)
      }
      // 直到 ok 为 true 的时候， 跳出for, 完成任务
      done <-true
      mr.registerChannel <-worker
    }(i)
  }

  // 等待所有的任务完成
  for i := 0; i < ntasks; i++ {
    // 直到done有数据写入， channel才会执行输出， 不然会一直堵塞
    // 监听堵塞 ntasks 个 done channel， 也就是说一定要  ntasks 个done channel 都有东西写入才不会阻塞
    <-done
  }
  fmt.Printf("调度: %v 阶段完成", phase)
}