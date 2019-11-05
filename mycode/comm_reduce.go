package main

import "strconv"

func MergeName(jobName string, reduceTask int) string {
  // 计算reduce任务生成文件的名称
  return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}
