package main

import (
  "fmt"
  "sync"
)

type Fetcher interface {
  Fetch(url string) (urls []string, err error)
}

func Serial(url string, fetcher Fetcher, fetched map[string]bool) {
  // 串行处理
  if fetched[url] {
    return
  }
  fetched[url] = true
  urls, err := fetcher.Fetch(url)
  if err != nil {
    return
  }
  for _, u := range urls {
    fmt.Println("u ---------------", u)
    Serial(u, fetcher, fetched)
  }
  return
}



// 并行处理， 用共享内存放状态， 竞争锁安全操作的方式
type fetchState struct {
  mu sync.Mutex
  fetched map[string]bool
}

func ConcurrentMutex(url string, fetcher Fetcher, f *fetchState)  {
  f.mu.Lock()
  if f.fetched[url] {
    f.mu.Unlock()
    return
  }
  f.fetched[url] = true
  f.mu.Unlock()

  urls, err := fetcher.Fetch(url)
  if err != nil {
    return
  }

  var done sync.WaitGroup
  for _, u := range urls {
    done.Add(1)
    go func(u string) {
      defer done.Done()
      ConcurrentMutex(u, fetcher, f)
    }(u)
  }
  done.Wait()
  return
}

func makeState() *fetchState {
  f := &fetchState{}
  f.fetched = make(map[string]bool)
  return f
}


/**
// 并行处理， 用channel的方式
func worker(url string, ch chan []string, fetcher Fetcher) {
  urls, err := fetcher.Fetch(url)
  if err != nil {
    ch <-[]string{}
  } else {
    ch <-urls         // fixme: 持续写入ch
  }
}

func master(ch chan []string, fetcher Fetcher) {
  n := 1
  fetched := make(map[string]bool)
  for urls := range ch {          // fixme: 持续读取ch
    for _, u := range urls {
      if fetched[u] == false {
        fetched[u] = true
        n += 1
        go worker(u, ch, fetcher)
      }
    }

    n -= 1
    if n == 0 {
      break
    }
  }
}
**/


/**
fixme:
golang channel 的 master worker 并行处理模型
master  读取channel
worker  写入channel
 */

func worker(url string, ch chan []string, fetcher Fetcher) {
  urls, err := fetcher.Fetch(url)
  if err != nil {
    ch <-[]string{}       // 如果错误，返回空slice
  } else {
    ch <-urls             // 如果命中第一层key， 则返回第一层key所包含的url
  }
}

func master(ch chan []string, fetcher Fetcher) {
  n := 1
  fetched := make(map[string]bool)
  for urls := range ch {      // 持续读取ch
    for _, u := range urls {
      if fetched[u] == false {
        fetched[u] = true
        n += 1
        go worker(u, ch, fetcher)     // 写入 ch， 有多少个写入就有多少个读取
      }
    }
    n -= 1        // 每一次读取ch， 都减少1
    if n == 0 {   // 则 n == 0 为全部ch 里面的全部数据处理完成的成立条件
      break
    }
  }
}



func ConcurrentChannel(url string, fetcher Fetcher) {
  ch := make(chan []string)
  go func() {
    ch <-[]string{url}
  }()
  master(ch, fetcher)
}


// 返回可以爬取的url
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
  body string
  urls []string
}

func (f fakeFetcher) Fetch(url string) ([]string, error) {
  if res, ok := f[url]; ok {
    fmt.Printf("匹配到url: %s\n", url)
    return res.urls, nil
  }

  fmt.Printf("错误匹配的url: %s\n", url)
  return nil, fmt.Errorf("错过匹配的url: %s\n", url)
}

// fetcher is a populated fakeFetcher.
/**
假如某个key下面包含的url， 命中了第一层的key， 证明是在要爬取的目标网站中，那么就在寻找key下面的value， 如果循环
 */
var fetcher = fakeFetcher{
  "http://www.douban.com/": &fakeResult{
    "douban",
    []string{
      "https://www.douban.com/about/",
      "https://www.douban.com/about/contactus",
      "http://zhihu.com/",
    },
  },
  "http://zhihu.com/": &fakeResult{
    "zhihu",
    []string{
      "http://zhihu.com/app/",
      "http://zhihu.com/terms",
      "http://www.douban.com/",
    },
  },
}

// main 
func main() {
  fmt.Printf("======== 串行 =======\n")
  Serial("http://www.douban.com/", fetcher, make(map[string]bool))

  fmt.Printf("======== 并行共享内存 =======\n")
  ConcurrentMutex("http://www.douban.com/", fetcher, makeState())

  fmt.Printf("======== 并行channel =======\n")
  ConcurrentChannel("http://www.douban.com/", fetcher)
}



