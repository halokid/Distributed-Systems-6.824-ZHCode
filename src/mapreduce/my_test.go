package mapreduce

import (
  "fmt"
  "hash/fnv"
  "testing"
)

func TestIhash(t *testing.T) {
  s := "hello"
  h := fnv.New32a()
	h.Write([]byte(s))
	x := int(h.Sum32() & 0x7fffffff)
  fmt.Println(x)

	y := int(1 & 0x7fffffff)
  fmt.Println(y)

	z := int(10 & 19)
  fmt.Println(z)
}