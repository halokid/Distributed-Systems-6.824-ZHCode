package mapreduce

func (mr *Master) merge() {
  // 将许多reduce的结果合并成一个输出文件
  debug("Merge阶段")
  kvs:= make(map[string]string)
  for i := 0; i < mr.nReduce; i++ {
    p := mergeName()
  }
}
