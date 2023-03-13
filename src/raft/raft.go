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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int        // candidateID that received my vote in current term, -1 if none
	log         []LogEntry // log entries

	commitIndex int // index of the highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// leader's data
	nextIndex  []int // index of the next log entry to send to each server
	matchIndex []int // index of highest log entry known to be replicated on each server

	role Role // the current role of the server

	// electionTimer  *time.Timer
	// heartbeatTimer *time.Timer
	timer *time.Timer
}

type LogEntry struct {
	Command      interface{} // command for state machine
	ReceivedTerm int         // term when entry was received by leader
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

const (
	MaxElectionTimeout = 300
	MinElectionTimeout = 150
	HeartbeatTimeout   = 120 * time.Millisecond // no more than 10 times per second
)

// generate a randomized election timeout
func RandomizedElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration((rand.Intn(MaxElectionTimeout) + MinElectionTimeout)) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = rf.currentTerm
	if rf.role == Leader {
		isleader = true
	} else {
		isleader = false
	}
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateID  int // candidate that requests vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term   int  // currentTerm, for leader to update itself
	Sucess bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	// term is smaller than mine, or I have already voted
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateID) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	// term is greater than mine, directly turn into follower
	if args.Term > rf.currentTerm {
		rf.role = Follower
		rf.currentTerm, rf.votedFor = args.Term, -1
	}

	// the candidate's log is out of date
	if args.LastLogIndex < len(rf.log)-1 || (len(rf.log) >= 1 && args.LastLogTerm < rf.log[len(rf.log)-1].ReceivedTerm) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	// vote
	rf.votedFor = args.CandidateID
	rf.timer.Reset(RandomizedElectionTimeout())
	reply.Term, reply.VoteGranted = rf.currentTerm, true
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, votes_cnt *int) bool {
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// failed to send, try again
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !reply.VoteGranted {
		rf.role = Follower
		rf.timer.Reset(RandomizedElectionTimeout())
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
		}
	}

	// increment votes_cnt if voted
	if reply.VoteGranted && reply.Term == rf.currentTerm && *votes_cnt <= len(rf.peers)/2 {
		*votes_cnt++
		// get multiple votes
		if *votes_cnt > len(rf.peers)/2 {
			// already a leader, keep it
			if rf.role == Leader {
				return true
			}
			// become the leader
			rf.role = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			for i, _ := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.log) + 1
			}
			rf.timer.Reset(HeartbeatTimeout)
		}
	}
	return true
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// return false if term<currentTerm
	if args.Term < rf.currentTerm {
		reply.Term, reply.Sucess = rf.currentTerm, false
		return
	}

	// update rf
	rf.currentTerm = args.Term
	// rf.votedFor = args.LeaderId
	rf.role = Follower
	rf.timer.Reset(RandomizedElectionTimeout())

	reply.Term, reply.Sucess = rf.currentTerm, true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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

	if !reply.Sucess {
		rf.role = Follower
		rf.votedFor = -1
		rf.timer.Reset(RandomizedElectionTimeout())
		rf.currentTerm = reply.Term
	}
	return true
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
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		// election timeout, start a new election
		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			// election timeout
			if rf.role != Leader {
				rf.role = Candidate                         // convert to candidate
				rf.currentTerm++                            // increment currenTerm
				rf.timer.Reset(RandomizedElectionTimeout()) // reset timer
				rf.votedFor = rf.me
				votes_cnt := 1 // calculate total votes
				// request votes
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						args := RequestVoteArgs{
							Term:         rf.currentTerm,
							CandidateID:  rf.me,
							LastLogIndex: len(rf.log) - 1,
							LastLogTerm:  0,
						}
						if len(rf.log) >= 1 {
							args.LastLogTerm = rf.log[len(rf.log)-1].ReceivedTerm
						}
						reply := RequestVoteReply{}
						go rf.sendRequestVote(i, &args, &reply, &votes_cnt)
					}
				}
			}
			// heartbeat timeout
			if rf.role == Leader {
				rf.timer.Reset(HeartbeatTimeout) // reset heartbeatTimer
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						args := AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: 0,
							PrevLogTerm:  0,
							Entries:      nil, // empty means this is a heartbeat
							LeaderCommit: rf.commitIndex,
						}
						reply := AppendEntriesReply{}
						go rf.sendAppendEntries(i, &args, &reply)
					}
				}
			}
			rf.mu.Unlock()
		}
	}
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

// peers: an array of network identifiers of the Raft peers
// me: index of this peer in the peers array
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.role = Follower

	rf.timer = time.NewTimer(RandomizedElectionTimeout())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
