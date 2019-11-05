package main

import (
  "bufio"
  "configcenter/src/framework/core/log"
  "fmt"
  "hash/fnv"
  "os"
  "strconv"
  "strings"
)

type KeyValue struct {
  Key     string
  Value   string
}

func MapFunc(file string, value string) (res []KeyValue) {
  words := strings.Fields(value)
  for _, w := range words {
    kv := KeyValue{Key: w, Value: ""}
    res = append(res, kv)
  }
  return
}

func ReduceName(jobname string, mapTask int, reduceTask int) string {
  return "mrtmp." + jobname + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

func Ihash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

func MakeInput(num int) []string {
  var names []string
  var i = 0
  for f := 0; f < num; f++ {
    names = append(names, fmt.Sprintf("824-mrinput-%d.txt", f))
    file, err := os.Create(names[f])
    if err != nil {
      log.Fatal("生成文件失败: ", err)
    }
    w := bufio.NewWriter(file)
    for i < (f+1)*(10000/num) {
      // fixme: 生成num个文件， 把 10000 数字平均写进 num个文件的算法
      fmt.Fprintf(w, "%d\n", i)
      i++
    }
    w.Flush()
    file.Close()
  }
  return names
}











