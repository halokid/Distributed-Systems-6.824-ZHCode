package main

import (
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
