package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"labgob"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"


type ApplyMsg struct {
	CommandValid 			bool
	Command 					interface{}
	CommandIndex 			int
	SnapShot 					[]byte
}

type State int

// 定义三种角色
const (
	Follower 				State = iota		// value ---> 0
	Candidate												// value ---> 1
	Leader													// value ---> 2
)

const NULL int = -1

type Log struct {
	Term 			int						"节点收到leader传过来某一期条目"
	Command 	interface{}		"状态机的指令"
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state 				State
	currentTerm		int			"服务器传过来的最后一个条目， 初始化为0， 单次递增"
	votedFor 			int			"候选人在当前这一期收到的投票， 如果没有收到其他peer的投票就是null"
	log 					[]Log

	lastIncludedIndex		int
	lastIncludeTerm 		int

	commitIndex 				int
	lastApplied 				int

	nextIndex 					[]int
	matchIndex 					[]int

	applyCh 						chan ApplyMsg
	killCh 							chan bool

	voteCh 							chan bool
	appendLogCh 				chan bool
}

// return currentTerm and whether this server
// believes it is the leader.

/**
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}
*/

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}





//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeRaftState())
}

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludeTerm)
	return w.Bytes()
}


//
// restore previously persisted state.
//
/**
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}
*/

func (rf *Raft) readPersist(data []byte) {
	// 从状态机持久化的存储的数据中恢复之前的状态
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var clog []Log
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&clog) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		log.Fatalf("从服务器读取持久化储存错误, %v", rf.me)
	} else {
		rf.mu.Lock()
		rf.currentTerm, rf.votedFor, rf.log = currentTerm, voteFor, clog
		rf.lastIncludeTerm, rf.lastIncludedIndex = lastIncludedTerm, lastIncludedIndex
		rf.commitIndex, rf.lastApplied = rf.lastIncludedIndex, rf.lastIncludedIndex
		rf.mu.Unlock()
	}

}





//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 							int "candidate's term"
	CandidateId 			int "candidate requesting vote"
	LastLogIndex 			int "index of candidate last log entry"
	LastLogTerm 			int "term of candidate last log entry"
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 						int  "currentTerm, for candidate to update itself"
	VoteGranted			bool "true means candidate received vote"
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

/**
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
*/

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.state = Follower				//  一开始服务启动的时候，都是 Follower
	rf.currentTerm = 0
	rf.votedFor = NULL
	rf.log = make([]Log, 1)			// 一开始的索引值为1

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	rf.voteCh = make(chan bool, 1)			// 为了避免阻塞该 ch， 设置的 1 buff
	rf.appendLogCh = make(chan bool, 1)
	rf.killCh = make(chan bool, 1)

	rf.readPersist(persister.ReadRaftState())			// fixme: fuck

	// 领导者每秒发送不超过10次心跳， 这个是发送心跳的间隔时间
	heartbeatTime := time.Duration(100) * time.Millisecond

	// 需要定期的做一些动作
	go func() {
		for {
			// 监听退出的ch
			select {
			case <-rf.killCh:
				return
			default:

			}
			electionTime := time.Duration(rand.Intn(200) + 300) * time.Millisecond

			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()

			// 监听状态的ch
			switch state {
			case Follower, Candidate:
				select {
				case <-rf.voteCh:
				case <-rf.appendLogCh:
				case <-time.After(electionTime):			// 如果超过了选举时间
					rf.mu.Lock()
					rf.be
				}
			}
		}
	}()
}

func (rf *Raft) beCandidate() {
	// 成为一个候选人
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	// 持久化目前候选人身份的数据信息
	rf.persist()
	// 因为如果选举超时已经过去了，而没有给候选人投票，那么醒醒吧
	go rf.startElection()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:  rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex:  rf.getLastLogIdx(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()
	var votes int32 = 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			reply := &RequestVoteReply{}
			ret := rf.sendRequestVote(idx, &args, reply)

			if ret {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.beFollower(reply.Term)
					return
				}
				if rf.state != Candidate || rf.currentTerm != args.Term {
					// fixme: ?? 这里不理解
					return
				}
				if reply.VoteGranted {
					atomic.AddInt32(&votes, 1)
				}
				// 如果获得超过一半的服务器的投票， 则变为领导者
				if atomic.LoadInt32(&votes) > int32(len(rf.peers) / 2) {
					rf.beLeader()
					rf.startAppendLog()
				}
			}
		}()
	}
}


func send(ch chan bool) {
	select {
	case <-ch:
	default:

	}
	ch <-true
}

func (rf *Raft) LogLen() int {
	return len(rf.log) + rf.lastIncludedIndex
}

func (rf *Raft) getLastLogIdx() int {
	return rf.LogLen() - 1
}

func (rf *Raft) getLastLogTerm() int {
	idx := rf.getLastLogIdx()
	if idx < rf.lastIncludedIndex {
		return -1
	}
	return rf.getLog(idx).Term
}

func (rf *Raft) getLog(i int) Log {
	return rf.log[i - rf.lastIncludedIndex]
}

// follower的逻辑部分
func (rf * Raft) beFollower(term int) {
	rf.state = Follower
	rf.votedFor = NULL
	rf.currentTerm = term
	rf.persist()
}

func (rf *Raft) beLeader() {
	if rf.state != Candidate {
		return
	}
	rf.state = Leader
	// 初始化leader的数据
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		// 循环初始化每一个peer的 "下一个日志条目的索引值" 为领导者的  "lastIncludedIndex" 也就是 “日志条目索引值”
		rf.nextIndex[i] = rf.getLastLogIdx() + 1
	}
}

// 领导者的逻辑部分
func (rf *Raft) startAppendLog() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			for {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				if rf.nextIndex[idx] - rf.lastIncludedIndex < 1 {
					rf.
				}
			}
		}()
	}
}

func (rf *Raft) sendSnapshot(server int) {

}







