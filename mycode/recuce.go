package main

import (
  "encoding/json"
  "log"
  "os"
  "sort"
)

func DoRecduce(
  jobName           string,
  reduceTaskNumber  int,
  nMap              int,
  reduceF           func(key string, values []string) string,
  ) {

  // step 1： 读取map生成（切割）出的文件， 整合成kvs的数据格式， 同样的key合并到map里面去， map的数据格式是 map[string][]string
  kvs := make(map[string][]string)

  for i := 0; i < nMap; i++ {
    fileName := ReduceName(jobName, i, reduceTaskNumber)
    file, err := os.Open(fileName)
    if err != nil {
      log.Fatal("doReduce 1: 打开map切割的文件失败: ", err)
    }

    dec := json.NewDecoder(file)

    // 每一个切割好的文件， 都编码成 kvs 的数据类型
    for {
      var kv KeyValue
      err = dec.Decode(&kv)
      if err != nil {
        break
      }

      _, ok := kvs[kv.Key]
      if !ok {
        kvs[kv.Key] = []string{}
      }
      kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
    }
    file.Close()
  }

  var keys []string
  for k := range kvs {
    keys = append(keys, k)
  }

  // step 2: 按照keys的值来排序
  sort.Strings(keys)

  // step 3: 生成结果文件
  p := MergeName(jobName, reduceTaskNumber)
  file, err := os.Create(p)
  if err != nil {
    log.Fatal("doReduce 2: 生成文件失败", err)
  }
  enc := json.NewEncoder(file)
}







