package raft

import "testing"

func TestC1(t *testing.T) {
  //m := make(map[int]int, 0)
  m := make(map[int]int, 4)
  m[1] = 1
  m[2] = 2
  m[3] = 3
  t.Log(len(m))


  s := make([]int, 0, 3)
  s = append(s, 1, 2, 3, 4)
  s = append(s, 1, 2, 3, 4)
  t.Log(len(s))
  t.Log(cap(s))
}