package mapreduce

import (
	"fmt"
	"log"
	"encoding/json"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//


	// 1. 打开所有的临时文件， 把临时文件的内容读入内存
	keyValues := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		file, err := os.Open(reduceName(jobName, i, reduceTask))
		if err != nil {
			log.Fatal("打开临时文件失败...", reduceName(jobName, i, reduceTask))
		}

		var kv KeyValue
		dec := json.NewDecoder(file)
		err = dec.Decode(&kv)
		fmt.Println("&kv -----------", kv)
		fmt.Println("len(&kv) -----------", len(kv))
		fmt.Println("reduce开始合并每个map里面统计好的key值value合集， keyValues[key] 得到的是一个slice")
		for err == nil {
			keyValues[kv.Key] = append(keyValues[kv.Key], kv.Value)
			err = dec.Decode(&kv)
		}
		file.Close()
	}

	// 2. 按key排序， 就是把key归类
	fmt.Println("这按照map的个数归类好的排序汇总起来，再来一次总的排序, 输出到临时的文件是", outFile)
	var keys []string
	for k := range keyValues {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// 3. 循环key， 用key去调用 reduceF 函数， 写入输出的内容
	out, err := os.Create(outFile)
	if err != nil {
		log.Fatal("创建输出文件失败", outFile)
	}

	enc := json.NewEncoder(out)
	for _, k := range keys {
		v := reduceF(k, keyValues[k])
		//debugx("keyValues[", k ,"] 的长度-----", len(keyValues[k]))
		fmt.Println("keyValues[", k ,"] 的长度-----", len(keyValues[k]))
		err = enc.Encode(KeyValue{Key: k, Value: v})
		if err != nil {
			log.Fatal("编码失败", KeyValue{Key: k, Value: v})
		}
	}

	out.Close()
}









