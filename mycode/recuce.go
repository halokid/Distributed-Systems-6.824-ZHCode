package main

import (
  "encoding/json"
  "fmt"
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
    /**
    聚合同一个 nMap 区块号码的文件数据， 比如 nMap 是1， 则聚合  mrtmp.testjob-0-0, mrtmp.testjob-1-0， 两个
    文件, reduce是针对不同的map区块来进行合并的

    注意两个概念：
    nMap        是有多少个数据文件
    nReduce     是把每个数据文件分成多少区块
     */
    fileName := ReduceName(jobName, i, reduceTaskNumber)
    fmt.Println("reduce 文件 fileName ----------- ", fileName)
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

  // step 4: 为每个key调用用户指定的reduce
  for _, k := range keys {
    // 只是输出一下Reduce传值
    res := reduceF(k, kvs[k])
    enc.Encode(KeyValue{k, res})
  }

  // close file
  file.Close()
}










