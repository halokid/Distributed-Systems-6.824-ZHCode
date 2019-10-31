package main

import (
  "encoding/json"
  "fmt"
  "io/ioutil"
  "log"
  "os"
)


func DoMap(
  jobName string,     // the name of the  MapReduce job
  mapTaskName int,    // which map task this is
  inFile string,
  nReduce int,        // the number of reduce task that will be run
  mapF  func(file string, contents string)  []KeyValue,
  ) {
  // 1
  contents, err := ioutil.ReadFile(inFile)
  if err != nil {
   log.Fatal("do map error for inFile", err)
  }

  // 2
  kvResult := mapF(inFile, string(contents))

  // 3
  tmpFiles := make([] *os.File, nReduce)
  encoders := make([] *json.Encoder, nReduce)

  fmt.Println("nReduce --------------", nReduce)
  for i := 0; i < nReduce; i++ {
    fmt.Println("loop i --------------", i)
    tmpFileName := ReduceName(jobName, mapTaskName, i)
    fmt.Println("tmpFileName ------------", tmpFileName)
    tmpFiles[i], err = os.Create(tmpFileName)
    if err != nil {
      log.Fatal(err)
    }

    defer tmpFiles[i].Close()
    encoders[i] = json.NewEncoder(tmpFiles[i])
    if err != nil {
      log.Fatal(err)
    }
  }
  fmt.Println("--------------------------------------")

  for _, kv := range kvResult {
    /**
    把数字作为key， 根据nReduce的值算出每个key应该分布的 分割区块键值， 就是按照每个key的值均匀分到分割的哪个
    文件去的算法
     */
    hasKey := int(Ihash(kv.Key)) % nReduce
    fmt.Println("hasKey ---------------", hasKey)
    fmt.Println("kv.Key ---------------", kv.Key)
    err := encoders[hasKey].Encode(&kv)
    if err != nil {
      log.Fatal("把map的内容编码系列化成json失败", err)
    }
  }
}
















