package main

import (
  "fmt"
  "github.com/spf13/cast"
  "testing"
)

func TestMapFunc(t *testing.T) {
  value := "a b c d e f a c f"
  res := MapFunc("my-test.txt", value)
  fmt.Println(res)
}

func TestReduceName(t *testing.T) {
  r := ReduceName("testjob", 2, 3)
  fmt.Println(r)
}

func TestIhash(t *testing.T) {
  h := Ihash("hello")
  fmt.Println(h)
}

func TestMakeInput(t *testing.T) {
  files := MakeInput(2)
  fmt.Println(files)
}

func TestDoMap(t *testing.T) {
  //return
  files := MakeInput(2)
  for i, f := range files {
    //DoMap("testjob", i, f,1, MapFunc)
    DoMap("testjob", i, f,4, MapFunc)
  }
}

func TestReduceFile(t *testing.T) {
  /**
  测试一堆数字（或者其他字符也可以），均匀分配到几个区块（几个文件， 几个列表等都可以, 这里的实例是文件）的算法
  */
  nReduce := 4        // 4个区块
  // 把100个数字按照 4 个区块来划分
  for i := 0; i < 100; i++ {
    hasKey := int(Ihash(cast.ToString(i))) % nReduce
    fmt.Println("hasKey :", i, "---", hasKey)
  }
}










