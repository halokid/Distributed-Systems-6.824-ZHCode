package mapreduce

import (
  "bufio"
  "encoding/json"
  "fmt"
  "log"
  "os"
  "sort"
)

func (mr *Master) merge() {
  // 将许多reduce的结果合并成一个输出文件
  debug("Merge阶段开始...")
  kvs:= make(map[string]string)
  for i := 0; i < mr.nReduce; i++ {
    p := mergeName(mr.jobName, i)
    fmt.Printf("Merge阶段: 读取%s\n", p)
    file, err := os.Open(p)
    if err != nil {
      log.Fatal("Merge阶段: ", err)
    }
    dec := json.NewDecoder(file)
    for {
      var kv KeyValue
      err = dec.Decode(&kv)
      if err != nil {
        break
      }
      kvs[kv.Key] = kv.Value
    }
    file.Close()
  }

  var keys []string
  for k := range kvs {
    keys = append(keys, k)
  }
  sort.Strings(keys)

  file, err := os.Create("mrtmp." + mr.jobName)
  if err != nil {
    log.Fatal("Merge阶段: 创建 ", err)
  }
  w := bufio.NewWriter(file)
  for _, k := range keys {
    fmt.Fprintf(w, "%s: %s\n", k, kvs[k])
  }
  w.Flush()
  file.Close()
}









