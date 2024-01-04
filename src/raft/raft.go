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
	"fmt"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const HeartBeatTime = 120 * time.Millisecond

type LogEntry struct {
	Term    int
	Command interface{}
}

type State int

const (
	Leader    State = 1
	Follower  State = 2
	Candidate State = 3
)

const (
	APPNormal AppendEntriesState = iota
	APPOutOfDate
	APPKilled

	APPInconsistent
	APPRpcDelay
)

type VoteState int

const (
	Expire VoteState = iota
	Normal
	Voted
	Killed
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //候选人的任期号
	CandidateId  int //候选人的Id
	LastLogIndex int //候选人的最后日志条目的索引值
	LastLogTerm  int //候选人最后日志的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool //候选人赢得了此张选票时为真
	VoteState   VoteState
}

type AppendEntriesState int

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//所有服务器上持久存在的
	CurrentTerm int        //服务器最后一次知道的任期号
	VotedFor    int        //在当前获得选票的候选人的 Id  用于选举超时时，判断自己的票是否要投给自己
	Log         []LogEntry //日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号

	//所有服务器上经常变的
	CommitIndex int //已知的最大的已提交的日志条目的索引值————已提交
	LastApplied int //已经应用的最大的日志索引值————已落盘

	//在领导人里经常改变的 （选举后重新初始化）
	NextIndex  []int //对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
	MatchIndex []int //对于每一个服务器，已经复制给他的日志的最高索引值

	//自己定义的
	TimeOut time.Duration //选举过期时间： 跟随者在这一段时间（选举超时时间）没有收到任何的RPC消息
	State   State         //节点的状态
	Timer   *time.Timer   //定时器
	//LastHeartBeat time.Time     //上一次心跳时间

	ApplyChan chan ApplyMsg
}

// GetState return currentTerm and whether this server
// believes it is the leader.
// 返回当前的任期号和判断是否自己是Leader
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.CurrentTerm

	if rf.State == Leader {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//同一轮选票的任期号才会相同
	//每一个服务器最多会对一个任期号投出一张选票，按照先来先服务的原则

	rf.mu.Lock()
	defer func() {
		//fmt.Printf("投票结果：")
		//rf.String()
	}()
	defer rf.mu.Unlock()

	if rf.killed() {
		reply.VoteState = Killed
		reply.Term = -1
		reply.VoteGranted = false
		return
	}
	//任期号大的说的算
	//如果此次 RPC 中的任期号比自己小，那么候选人就会拒绝这次的 RPC 并且继续保持候选人状态。
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		reply.VoteState = Expire
		return
	} else if args.Term > rf.CurrentTerm {
		//如果接收到的 RPC 请求中，任期号T > currentTerm，那么就令 currentTerm 等于 T，并切换状态为跟随者（5.1 节）
		rf.CurrentTerm = args.Term
		rf.State = Follower
		//新一轮的选举来了  重置手中的选票
		rf.VotedFor = -1

	}

	if rf.VotedFor == -1 {
		//如果 votedFor 为空或者就是 candidateId，
		//并且候选人的日志也跟自己一样新，那么就投票给他（5.2 节，5.4 节）
		currentLogIndex := len(rf.Log) - 1
		currentLogTerm := 0
		// 如果currentLogIndex下标不是-1就把term赋值过来
		if currentLogIndex >= 0 {
			currentLogTerm = rf.Log[currentLogIndex].Term
		}
		//第二个条件————候选人的日志也和当前节点一样新
		// 这里是一个重点，卡这里导致 lab2b rejoin of partitioned leader/ backs up没pass
		// 1、args.LastLogTerm < lastLogTerm是因为选举时应该首先看term，只要term大的，才代表存活在raft中越久
		// 2、判断日志的最后一个index是否是最新的前提应该是要在这两个节点任期是否是相同的情况下，判断哪个数据更完整。
		if args.LastLogTerm < currentLogTerm || (len(rf.Log) > 0 && args.LastLogTerm == rf.Log[len(rf.Log)-1].Term && args.LastLogIndex < currentLogIndex) {
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = false
			reply.VoteState = Expire
			return
		}
		//日志这里尽量追求一样新的：因为有些候选人可能落后，将票留给更对自己“友好”的候选人身上
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		reply.VoteState = Normal
		//给票
		rf.VotedFor = args.CandidateId

		rf.Timer.Reset(rf.TimeOut)

	} else {
		//手里的票给出去了
		reply.VoteState = Voted
		reply.VoteGranted = false

		if rf.VotedFor != args.CandidateId {
			// 告诉reply票已经没了返回false
			return
		} else { // 2. 当前的节点票已经给了同一个人了，但是由于sleep等网络原因，又发送了一次请求
			// 重置自身状态
			rf.State = Follower
		}
		rf.Timer.Reset(rf.TimeOut)

	}

	return
}

type AppendEntriesArgs struct {
	Term         int        //领导人的任期号
	LeaderId     int        //领导人的ID———
	PrevLogIndex int        //上一个日志的索引号
	PrevLogTerm  int        //prevLogIndex条目的任期号
	Entries      []LogEntry //还不知道
	LeaderCommit int        //领导人已经提交日志最大索引
}

type AppendEntriesReply struct {
	Term                  int                //当前的任期号，用于领导人去更新自己
	Success               bool               //跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真
	AppState              AppendEntriesState // 追加状态
	ConflictTerm          int                //冲突的任期号
	ConflictEarliestIndex int                //冲突最小的索引
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	/*
		如果 term < currentTerm 就返回 false （5.1 节）
		如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false （5.3 节）
		如果已经存在的日志条目和新的产生冲突（相同偏移量但是任期号不同），删除这一条和之后所有的 （5.3 节）
		附加任何在已有的日志中不存在的条目
		如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
	*/
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 节点crash
	if rf.killed() {
		reply.AppState = APPKilled
		reply.Term = -1
		reply.Success = false
		return
	}

	if args.Term < rf.CurrentTerm {
		//大的任期号优先
		reply.Term = rf.CurrentTerm
		reply.AppState = APPOutOfDate
		reply.Success = false
		return
	} else if args.Term > rf.CurrentTerm {
		//如果接收到的 RPC 请求中，任期号T > currentTerm，那么就令 currentTerm 等于 T，并切换状态为跟随者（5.1 节）
		rf.CurrentTerm = args.Term
	}
	//走到这步 说明本节点肯定时Follower节点
	rf.State = Follower
	rf.VotedFor = args.LeaderId

	reply.Success = true
	reply.Term = rf.CurrentTerm
	reply.AppState = APPNormal

	//每次接受到心跳消息或候选人消息  重置定时器
	//rf.Timer.Reset(rf.TimeOut)
	rf.Timer.Reset(rf.TimeOut)

	//一致性检查
	//fmt.Printf("aeRPC为：%v\n", args)
	//跟随者的日志少很多
	if len(rf.Log)-1 < args.PrevLogIndex {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		reply.AppState = APPInconsistent
		if len(rf.Log) > 0 {
			reply.ConflictTerm = rf.Log[len(rf.Log)-1].Term
		} else {
			reply.ConflictTerm = rf.CurrentTerm
		}
		//reply.ConflictEarliestIndex=len(rf.Log)-1  不用减一
		//reply.ConflictEarliestIndex = len(rf.Log)
		reply.ConflictEarliestIndex = rf.LastApplied + 1
		//fmt.Printf("出现冲突（节点日志长度不够）节点%v，返回值为%v,该节点长度为%v,增量请求的下一个日志索引为%v \n", rf.me, reply, len(rf.Log), reply.ConflictEarliestIndex)
		return
	} else {
		//(1)如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配
		if args.PrevLogIndex >= 0 && rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm && args.PrevLogIndex < len(rf.Log) {
			reply.Term = rf.CurrentTerm
			reply.Success = false
			reply.AppState = APPInconsistent
			reply.ConflictTerm = rf.Log[args.PrevLogIndex].Term
			//reply.ConflictEarliestIndex = rf.GetEarliestIndexByTerm(rf.Log[args.PrevLogIndex].Term)
			//reply.ConflictEarliestIndex = rf.LastApplied + 1
			reply.ConflictEarliestIndex = rf.LastApplied + 1
			//fmt.Printf("出现冲突（上一个日志冲突）节点%v，返回值为%v", rf.me, reply)
			return
		}

		//用于在Leader刚刚初始化的NextIndex直接初始化为下一个的；日志重复提交
		if rf.LastApplied > args.PrevLogIndex {
			reply.AppState = APPInconsistent
			reply.Term = rf.CurrentTerm
			reply.Success = false
			reply.ConflictEarliestIndex = rf.LastApplied + 1
			return
		}

		//if len(rf.Log)-2 <= args.PrevLogIndex {
		//	//(2)如果已经已经存在的日志条目和新的产生冲突（相同偏移量但是任期号不同），
		//	//   删除这一条和之后所有的 （5.3 节） 对应与（d）情节
		//	if rf.Log[args.PrevLogIndex+1].Term != args.Entries {
		//		rf.Log = rf.Log[:args.PrevLogIndex]
		//	}
		//
		//	//感觉不用上面的判断  增量的日志RPC可能会触发多次，实现幂等性
		//	rf.Log = rf.Log[:args.PrevLogIndex]
		//	//到这前面的判断都是满足的  不用return了  直接加上添加的日志即可
		//}

	}

	//添加附加的日志
	if args.Entries != nil {
		//前面的检查全部做完了
		//因为leader前面的发送的rpc到达的时间比后面发送的rpc更加晚
		//leader发过来的日志在节点的日志里面都有，导致不能直接使用rf.Log = rf.Log[:args.PrevLogIndex+1]
		//只应该删除那些不匹配的日志
		if len(args.Entries)+args.PrevLogIndex < len(rf.Log) {
			var flag bool = false
			for i := 0; i < len(args.Entries); i++ {
				if rf.Log[args.PrevLogIndex+1+i].Term != args.Entries[i].Term {
					flag = true
				}
			}

			if !flag {
				reply.AppState = APPRpcDelay
				reply.Term = rf.CurrentTerm
				reply.Success = true
				return
			}
		}
		rf.Log = rf.Log[:args.PrevLogIndex+1]
		rf.Log = append(rf.Log, args.Entries...)
	}

	//设置该提交的索引
	if args.LeaderCommit > rf.CommitIndex {
		//设置为对等点能最大提交的地方
		rf.CommitIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	}

	//leader传过来自己已经提交的日志
	for rf.LastApplied < rf.CommitIndex {
		rf.LastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.LastApplied,
			Command:      rf.Log[rf.LastApplied].Command,
		}
		rf.ApplyChan <- applyMsg
	}

	return
}

func Min(a, b int) int {
	if a > b {
		return b
	}
	return a
}
func (rf *Raft) GetEarliestIndexByTerm(term int) int {
	for i, entry := range rf.Log {
		if entry.Term == term {
			return i
		}
	}
	//当日志为空时，也用不上这个吧
	return 1 //

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, voteNum *int) bool {
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {

		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch reply.VoteState {
	case Expire:
		{
			rf.State = Follower
			rf.Timer.Reset(rf.TimeOut)
			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.VotedFor = -1
			}
		}
	case Normal, Voted:
		{
			//重大BUG：防止之前选票的出现   扰乱市场
			if reply.VoteGranted && reply.Term == rf.CurrentTerm {
				*voteNum += 1
			}
			//fmt.Println("节点" + strconv.Itoa(rf.me) + "计数的管道长度：" + strconv.Itoa(len(successChan)))
			peerNum := len(rf.peers)
			//过半——成为领导人
			if *voteNum > peerNum/2 {
				//过半的请求只用触发一次即可
				//清空计数的管道
				//fmt.Println("清空计数的管道")
				//之前清空写法写错了
				*voteNum = 0
				fmt.Printf("节点%v 在任期%v 变成Leader,日志为%v. \n", rf.me, rf.CurrentTerm, rf.Log)
				rf.State = Leader
				rf.Timer.Stop() //关闭定时器
				rf.NextIndex = make([]int, len(rf.peers))
				for i, _ := range rf.NextIndex {
					rf.NextIndex[i] = len(rf.Log)
				}
			}
		}
	case Killed:
		return false
	}

	return ok
}

// 自己实现的
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply, appendNum *int) bool {

	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	for !ok {

		if rf.killed() {
			return false
		}

		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 对reply的返回状态进行分支
	switch reply.AppState {

	// 目标节点crash
	case APPKilled:
		{
			return false
		}

	// 目标节点正常返回
	case APPNormal:
		{

			//fmt.Printf("发给节点%v的上一个索引为%v的日志发送成功\n", server, args.PrevLogIndex)
			// 2A的test目的是让Leader能不能连续任期，所以2A只需要对节点初始化然后返回就好
			// 2B要统计appendNum的个数，判断是否可以运用于状态机中
			if reply.Success && reply.Term == rf.CurrentTerm {
				*appendNum += 1
			}

			// 说明返回的值已经大过了自身数组
			//if rf.NextIndex[server] > len(rf.Log)+1 {
			//	return false
			//}

			//改变下一个日志索引的目录
			rf.NextIndex[server] += len(args.Entries)
			//普通的心跳检测不应该进入里面  如果有增量rpc才进入
			if *appendNum > len(rf.peers)/2 && len(args.Entries) > 0 {
				*appendNum = 0

				if len(rf.Log) == 0 || rf.Log[len(rf.Log)-1].Term != rf.CurrentTerm {
					return false
				}

				//改变复制日志为已提交
				appendIndex := len(rf.Log) - 1
				for rf.LastApplied < appendIndex {
					rf.LastApplied++
					applyMsg := ApplyMsg{
						CommandIndex: rf.LastApplied,
						CommandValid: true,
						Command:      rf.Log[rf.LastApplied].Command,
					}
					//塞进去先
					rf.ApplyChan <- applyMsg
					rf.CommitIndex = rf.LastApplied
				}
			}
			return true
		}

	//If AppendEntries RPC received from new leader: convert to follower(paper - 5.2)
	//reason: 出现网络分区，该Leader已经OutOfDate(过时）
	//说明Leader已经过时了
	case APPOutOfDate:

		// 该节点变成追随者,并重置rf状态
		rf.State = Follower
		rf.VotedFor = -1
		//领导人不需要选举时间，这里变为跟随者需要
		if rf.killed() == false {
			rf.Timer.Reset(rf.TimeOut)
		}
		rf.CurrentTerm = reply.Term

	case APPInconsistent:
		//越过那些任期冲突的日志条目
		rf.NextIndex[server] = reply.ConflictEarliestIndex
	////进行重试  减少nextIndex
	//args.Entries = rf.Log[rf.NextIndex[server]:]
	//args.PrevLogIndex = reply.ConflictEarliestIndex - 1
	//if reply.ConflictEarliestIndex > 0 {
	//	args.PrevLogTerm = rf.Log[reply.ConflictEarliestIndex-1].Term
	//} else {
	//	args.PrevLogTerm = rf.CurrentTerm
	//}
	////死锁啦
	//rf.sendAppendEntries(server, args, reply, appendNum)

	case APPRpcDelay:
		{
			return false
		}
	}
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.killed() {
		return index, term, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.State != Leader {
		return index, term, false
	}

	//先把增量日志放到日志数组中，后面通过心跳检测出来
	entry := LogEntry{Term: rf.CurrentTerm, Command: command}
	rf.Log = append(rf.Log, entry)
	index = len(rf.Log) - 1
	term = rf.CurrentTerm

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	//这里的故障不是真正的故障  只是特定状态的改变
	rf.Timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) String() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("rf任期：%v，rf节点Id：%v，rf当前状态：%v，rf当前投票给%v \n", rf.CurrentTerm, rf.me, rf.State, rf.VotedFor)
}

//func (rf *Raft) DoWork() {
//	for rf.killed() == false {
//		if rf.CommitIndex > rf.LastApplied {
//			rf.LastApplied++
//			//TODO 应用到状态机中去
//		}
//	}
//}

func (rf *Raft) HeartBeart() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.State == Leader {

			appendNum := 1

			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me { //重大BUG：Leader不应该给自己发送心跳，一发心跳直接将Leader状态给打破了
					continue
				}
				//发送心跳||增量日志
				args := AppendEntriesArgs{
					Term:         rf.CurrentTerm,
					LeaderId:     rf.me,          //领导人的ID
					LeaderCommit: rf.CommitIndex, //领导人已经提交日志最大索引
				}
				if len(rf.Log) > 0 {
					if rf.NextIndex[i] < len(rf.Log) {
						args.Entries = rf.Log[rf.NextIndex[i]:]
					}
				}

				// 代表已经不是初始值0
				if rf.NextIndex[i] >= 0 {
					args.PrevLogIndex = rf.NextIndex[i] - 1
				}

				if args.PrevLogIndex > 0 && args.PrevLogIndex < len(rf.Log) {
					args.PrevLogTerm = rf.Log[args.PrevLogIndex].Term
				}

				reply := AppendEntriesReply{}
				//if len(args.Entries) != 0 {
				//	//fmt.Printf("节点%v发给%v的ae日志为%v \n", rf.me, i, args)
				//	fmt.Printf("节点%v发给%v的ae日志长度为%v，上一个日志索引为：%v \n", rf.me, i, len(args.Entries), args.PrevLogIndex)
				//}
				fmt.Printf("节点%v发给%v的ae日志长度为%v，上一个日志索引为：%v \n", rf.me, i, len(args.Entries), args.PrevLogIndex)

				go rf.sendAppendEntries(i, args, &reply, &appendNum)
				//用管道存储 成功的个数
			}

			rf.mu.Unlock()
			time.Sleep(HeartBeatTime)
		} else {
			rf.mu.Unlock()
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// 开启一个新的选举
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//是否需要进行选举

		//在开始一次选举的时候会重置一个随机地选举超时时间
		//这里应该判断的是选举时间  选举超时了

		//选举超时一定怀疑的是Leader，而不是网络，因为假设前提网络是稳定可靠的
		select {
		case <-rf.Timer.C:
			//rf.String()

			rf.mu.Lock()
			if rf.State == Follower || rf.State == Candidate {
				rf.State = Candidate
				rf.CurrentTerm = rf.CurrentTerm + 1
				rf.VotedFor = rf.me
				//不是领导人  就要重置选举超时时间
				//TODO 如果是领导人 要不要重置

				//发送投票的请求

				args := RequestVoteArgs{
					Term:         rf.CurrentTerm,
					CandidateId:  rf.me,           //候选人的Id
					LastLogIndex: len(rf.Log) - 1, //候选人的最后日志条目的索引值 TODO 选举限制
				}
				if len(rf.Log) > 0 {
					args.LastLogTerm = rf.Log[len(rf.Log)-1].Term //候选人最后日志的任期号
				}

				//把票投给自己
				voteNums := 1 // 对于正确返回的节点数量
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					reply := RequestVoteReply{}
					go rf.sendRequestVote(i, &args, &reply, &voteNums)
				}

				rf.TimeOut = GetRandTimeOut()
				rf.Timer.Reset(rf.TimeOut)

			}

			rf.mu.Unlock()
		default:
			continue
		}

	}
}

func GetRandTimeOut() time.Duration {
	return (time.Duration(150 + rand.Intn(200))) * time.Millisecond
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = make([]LogEntry, 0)

	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))
	//选举超时设置随机过期时间，阻止选票起初就被瓜分
	rf.TimeOut = GetRandTimeOut()
	rf.Timer = time.NewTimer(rf.TimeOut)
	rf.State = Follower //初始化都是跟随者
	rf.ApplyChan = applyCh

	//index从1开始,用个空的占位
	entry0 := LogEntry{Term: 0, Command: 0}
	rf.Log = append(rf.Log, entry0)
	defer func() {
		//rf.ApplyChan <- ApplyMsg{CommandIndex: 0, Command: 0, CommandValid: true}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	//进行日志复制的实际行动

	//进行
	go rf.HeartBeart()

	return rf
}
