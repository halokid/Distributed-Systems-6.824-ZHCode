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
