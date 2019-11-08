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
    /**
    当 i 为 0 时：  整合 两个map文件的  第一个区块
    当 i 为 1 时：  整合 两个map文件的  第二个区块
    如此类推
     */
    DoRecduce("testjob", i, 2, ReduceFunc)
  }
}
