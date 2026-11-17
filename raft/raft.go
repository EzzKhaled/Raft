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
	"math/rand"
	"sync"
	"time"

	"lab2raft/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // Reserved for a later lab; ignore in Lab 2.
	Snapshot    []byte // Reserved for a later lab; ignore in Lab 2.
}

type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	follower  = 0
	candidate = 1
	leader    = 2
)

const heartbeatInterval = 100 * time.Millisecond

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]

	// Your data here (3A, 3B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	dead        bool      // set by Kill(); stops background goroutines
	state       int       // follower, candidate or leader
	deadline    time.Time // election deadline
	currentTerm int
	votedFor    int        // -1 = none
	log         []LogEntry // log[0] is a dummy entry; real entries start at index 1

	commitIndex int
	lastApplied int

	// Volatile state on leaders (reinitialized after election)
	nextIndex  []int
	matchIndex []int
}

// random election timeout, well above the heartbeat interval
func randomTimeout() time.Duration {
	return time.Duration(300+rand.Intn(150)) * time.Millisecond
}

// move to a newer term as a follower. caller holds rf.mu.
func (rf *Raft) stepDown(term int) {
	if rf.state == leader {
		// an old leader has a stale deadline, give the new leader time to reach us
		rf.deadline = time.Now().Add(randomTimeout())
	}
	rf.currentTerm = term
	rf.state = follower
	rf.votedFor = -1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == leader
	return term, isleader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		// newer term: step down
		rf.stepDown(args.Term)
	}
	reply.Term = rf.currentTerm

	// election restriction: candidate's log must be at least as up-to-date
	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.deadline = time.Now().Add(randomTimeout())
	}
}

type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (3A, 3B).
	Term    int
	Success bool

	// used by the leader to skip back over a whole term on a mismatch
	ConflictTerm  int // term of the conflicting entry, -1 if the log is too short
	ConflictIndex int // first index of ConflictTerm, or log length if too short
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.ConflictTerm = -1
	reply.ConflictIndex = 0

	// Rule 1: Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// Recognize the valid leader and step down if this node is a candidate or older leader
	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}
	// a candidate in the same term lost the election to this leader
	rf.state = follower

	// Acknowledge the heartbeat/RPC by resetting the election timer
	rf.deadline = time.Now().Add(randomTimeout())
	reply.Term = rf.currentTerm

	// Rule 2: Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.log) {
		// Follower's log is too short
		reply.ConflictIndex = len(rf.log)
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Term mismatch at prevLogIndex: report the first index of that term
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for i > 1 && rf.log[i-1].Term == reply.ConflictTerm {
			i--
		}
		reply.ConflictIndex = i
		return
	}

	// Rule 3 & 4: Find conflicts, delete the existing entry and all that follow it, and append new ones
	// a delayed RPC with matching entries must not truncate anything
	insertIndex := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		logIndex := insertIndex + i
		if logIndex < len(rf.log) {
			if rf.log[logIndex].Term != entry.Term {
				// Conflict found: truncate the log and append the rest of the new entries
				rf.log = rf.log[:logIndex]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		} else {
			// No conflicts, just append any remaining new entries
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	// Rule 5: Update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewEntryIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewEntryIndex
		}
	}

	reply.Success = true
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// become leader and initialize tracking arrays. caller holds rf.mu.
func (rf *Raft) becomeLeader() {
	rf.state = leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for j := range rf.peers {
		rf.nextIndex[j] = len(rf.log)
		rf.matchIndex[j] = 0
	}
	rf.matchIndex[rf.me] = len(rf.log) - 1

	// announce leadership right away instead of waiting for the next tick
	rf.broadcastAppendEntries()
}

// send AppendEntries (heartbeat or new entries) to every peer. caller holds rf.mu.
func (rf *Raft) broadcastAppendEntries() {
	for i := range rf.peers {
		if i != rf.me {
			rf.sendAppendEntriesTo(i)
		}
	}
}

// send one AppendEntries to server and handle its reply in the background.
// caller holds rf.mu and is the leader.
func (rf *Raft) sendAppendEntriesTo(server int) {
	term := rf.currentTerm

	if rf.nextIndex[server] > len(rf.log) {
		rf.nextIndex[server] = len(rf.log)
	}
	if rf.nextIndex[server] < 1 {
		rf.nextIndex[server] = 1
	}
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.log[prevLogIndex].Term

	// Copy entries starting from nextIndex[server] to the end of the log
	entries := make([]LogEntry, len(rf.log[rf.nextIndex[server]:]))
	copy(entries, rf.log[rf.nextIndex[server]:])

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}

	go func() {
		reply := &AppendEntriesReply{}
		if !rf.sendAppendEntries(server, args, reply) {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.dead {
			return
		}
		if reply.Term > rf.currentTerm {
			// We encountered a higher term, step down immediately
			rf.stepDown(reply.Term)
			return
		}
		// Ensure we are still the leader in the same term
		if rf.currentTerm != term || rf.state != leader {
			return
		}

		if reply.Success {
			// Update tracking arrays on success; old replies can't move them backwards
			match := args.PrevLogIndex + len(args.Entries)
			if match > rf.matchIndex[server] {
				rf.matchIndex[server] = match
			}
			if match+1 > rf.nextIndex[server] {
				rf.nextIndex[server] = match + 1
			}
			rf.advanceCommitIndex()
			return
		}

		// ignore a rejection for a probe we already moved past
		if rf.nextIndex[server] != args.PrevLogIndex+1 {
			return
		}

		// Back up on log conflicts using the follower's hint
		next := reply.ConflictIndex
		if reply.ConflictTerm != -1 {
			// if we have entries from that term, resume right after our last one
			for j := args.PrevLogIndex; j > 0; j-- {
				if rf.log[j].Term == reply.ConflictTerm {
					next = j + 1
					break
				}
				if rf.log[j].Term < reply.ConflictTerm {
					break
				}
			}
		}
		if next < rf.matchIndex[server]+1 {
			next = rf.matchIndex[server] + 1
		}
		if next < 1 {
			next = 1
		}
		rf.nextIndex[server] = next

		// retry now so a lagging follower is repaired quickly
		rf.sendAppendEntriesTo(server)
	}()
}

// commit the highest index stored on a majority. caller holds rf.mu and is the leader.
func (rf *Raft) advanceCommitIndex() {
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		// Rule: Only commit entries from the current term by counting replicas
		if rf.log[n].Term != rf.currentTerm {
			break // older terms only get committed indirectly
		}
		matchCount := 1 // Leader always has the entry
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= n {
				matchCount++
			}
		}
		if matchCount > len(rf.peers)/2 {
			rf.commitIndex = n
			break // Found the highest safely replicated index
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.dead || rf.state != leader {
		return index, term, isLeader // returns -1, -1, false
	}

	// Node is the leader
	isLeader = true
	term = rf.currentTerm
	index = len(rf.log)

	// Append the new command to the leader's local log; the ticker replicates it
	rf.log = append(rf.log, LogEntry{Command: command, Term: term})
	rf.matchIndex[rf.me] = index

	// a single-server cluster is its own majority
	rf.advanceCommitIndex()
	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.dead = true
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	// Initialize state
	rf.state = follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0}} // dummy entry at index 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.deadline = time.Now().Add(randomTimeout())

	// 1. appendEntries ticker: broadcasts heartbeats and new log entries
	go func() {
		for {
			time.Sleep(heartbeatInterval)

			rf.mu.Lock()
			if rf.dead {
				rf.mu.Unlock()
				return
			}
			if rf.state == leader {
				rf.broadcastAppendEntries()
			}
			rf.mu.Unlock()
		}
	}()

	// 2. apply ticker: delivers committed entries to the state machine
	go func() {
		for {
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()

			if rf.dead {
				rf.mu.Unlock()
				return
			}

			// Collect every newly committed entry in order
			msgs := []ApplyMsg{}
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				msgs = append(msgs, ApplyMsg{
					Index:   rf.lastApplied,
					Command: rf.log[rf.lastApplied].Command,
				})
			}

			// Unlock *before* sending to the channel to prevent deadlocks
			rf.mu.Unlock()
			for _, msg := range msgs {
				applyCh <- msg
			}
		}
	}()

	// 3. election timer: start an election when the deadline passes
	go func() {
		for {
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
			if rf.dead {
				rf.mu.Unlock()
				return
			}
			if rf.state == leader || !time.Now().After(rf.deadline) {
				rf.mu.Unlock()
				continue
			}

			// become candidate
			rf.state = candidate
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.deadline = time.Now().Add(randomTimeout())
			term := rf.currentTerm
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			votes := 1

			// Handle single-server cluster edge case
			if votes > len(rf.peers)/2 {
				rf.becomeLeader()
			}

			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(server int) {
					reply := &RequestVoteReply{}
					if !rf.sendRequestVote(server, args, reply) {
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.dead {
						return
					}
					if reply.Term > rf.currentTerm {
						// newer term: step down
						rf.stepDown(reply.Term)
						return
					}
					if rf.currentTerm != term || rf.state != candidate {
						return // stale reply
					}
					if reply.VoteGranted {
						votes++
						if votes > len(rf.peers)/2 {
							rf.becomeLeader()
						}
					}
				}(i)
			}
			rf.mu.Unlock()
		}
	}()

	return rf
}
