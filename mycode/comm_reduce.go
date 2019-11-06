package main

import (
  "fmt"
  "strconv"
)

func MergeName(jobName string, reduceTask int) string {
  // 计算reduce任务生成文件的名称
  return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}


func ReduceFunc(key string, values []string) string {
  // 返回key
  for _, e := range values {
    fmt.Println("Reduce 输出", key, e)
  }
  return ""
}

