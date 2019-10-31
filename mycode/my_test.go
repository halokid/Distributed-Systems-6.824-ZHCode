package main

import (
  "fmt"
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
  return
  files := MakeInput(2)
  for i, f := range files {
    //DoMap("testjob", i, f,1, MapFunc)
    DoMap("testjob", i, f,2, MapFunc)
  }
}

func TestReduceFile(t *testing.T) {
  /**
  测试一堆数字（或者其他字符也可以），均匀分配到几个区块（几个文件， 几个列表等都可以, 这里的实例是文件）的算法
  */
  nReduce := 4        // 4个区块
  for i := 0; i < 1000; i++ {
    hasKey := int(Ihash(i)) % nReduce
  }
}










