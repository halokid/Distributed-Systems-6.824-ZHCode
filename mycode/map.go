package main

import (
  "encoding/json"
  "io/ioutil"
  "log"
  "os"
)


func doMap(
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

  for i := 0; i < nReduce; i++ {
    tmpFileName := ReduceName(jobName, mapTaskName, i)
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

  for _, kv := range kvResult {
    hasKey := int()
  }
}
















