package main

import "testing"


/**
func TestDoMap(t *testing.T) {
  //return
  files := MakeInput(2)
  for i, f := range files {
    //DoMap("testjob", i, f,1, MapFunc)
    DoMap("testjob", i, f,4, MapFunc)
  }
}

在上面的 map test里面生成了 2 个 map文件， 所以下面的 nMap = 2
nReduce 是 4， 所以下面 循环 小于4，  nReduce代表要起几个 reduce任务
 */

func TestDoRecduce(t *testing.T) {
  for i := 0; i < 4; i++ {
    DoRecduce("testjob", i, 2, ReduceFunc)
  }
}
