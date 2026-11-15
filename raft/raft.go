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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]

	// Your data here (3A, 3B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	dead     bool          // set by Kill(); stops background goroutines
	applyCh  chan ApplyMsg // channel to the service/tester
	state    int           // 0 = follower, 1 = candidate, 2 = leader
	deadline time.Time     // election deadline

	currentTerm int
	votedFor    int        // -1 = none
	log         []LogEntry // log[0] is a dummy entry; real entries start at index 1

	commitIndex int
	lastApplied int

	nextIndex  []int         // leader only
	matchIndex []int         // leader only
	wake       chan struct{} // wakes the leader loop (new entry / new leader)
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
	isleader = rf.state == 2
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
		rf.currentTerm = args.Term
		rf.state = 0
		rf.votedFor = -1
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
		rf.deadline = time.Now().Add(time.Duration(400+rand.Intn(300)) * time.Millisecond)
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
	Term          int
	Success       bool
	ConflictIndex int // hint for fast log backup
	ConflictTerm  int // -1 if the follower's log is too short
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.ConflictIndex = 0
	reply.ConflictTerm = -1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.state = 0 // a valid leader exists for this term
	reply.Term = rf.currentTerm
	rf.deadline = time.Now().Add(time.Duration(400+rand.Intn(300)) * time.Millisecond)

	// consistency check
	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		ct := rf.log[args.PrevLogIndex].Term
		ci := args.PrevLogIndex
		for ci > 1 && rf.log[ci-1].Term == ct {
			ci--
		}
		reply.ConflictTerm = ct
		reply.ConflictIndex = ci
		return
	}

	// append new entries, deleting only on a real conflict
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rf.log) {
			if rf.log[idx].Term == e.Term {
				continue
			}
			rf.log = rf.log[:idx]
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		break
	}

	if args.LeaderCommit > rf.commitIndex {
		last := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < last {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = last
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
	if rf.dead || rf.state != 2 {
		return index, term, isLeader
	}
	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	index = len(rf.log) - 1
	term = rf.currentTerm
	isLeader = true
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	// wake the leader loop so the new entry is sent right away
	select {
	case rf.wake <- struct{}{}:
	default:
	}

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	rf.dead = true
	rf.mu.Unlock()
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

	// Your initialization code here (3A, 3B).
	rf.applyCh = applyCh
	rf.state = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0}} // dummy entry at index 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.wake = make(chan struct{}, 1)
	rf.deadline = time.Now().Add(time.Duration(400+rand.Intn(300)) * time.Millisecond)

	// ---- election timer: start an election when the deadline passes ----
	go func() {
		for {
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
			if rf.dead {
				rf.mu.Unlock()
				return
			}
			if rf.state == 2 || !time.Now().After(rf.deadline) {
				rf.mu.Unlock()
				continue
			}

			// become candidate
			rf.state = 1
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.deadline = time.Now().Add(time.Duration(400+rand.Intn(300)) * time.Millisecond)
			term := rf.currentTerm
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			votes := 1

			if votes > len(rf.peers)/2 { // single-server cluster
				rf.state = 2
				for j := range rf.peers {
					rf.nextIndex[j] = len(rf.log)
					rf.matchIndex[j] = 0
				}
				rf.matchIndex[rf.me] = len(rf.log) - 1
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
					if rf.currentTerm != term || rf.state != 1 {
						return // stale reply
					}
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = 0
						rf.votedFor = -1
						return
					}
					if reply.VoteGranted {
						votes++
						if votes > len(rf.peers)/2 {
							// become leader
							rf.state = 2
							for j := range rf.peers {
								rf.nextIndex[j] = len(rf.log)
								rf.matchIndex[j] = 0
							}
							rf.matchIndex[rf.me] = len(rf.log) - 1
							select { // send the initial heartbeat now
							case rf.wake <- struct{}{}:
							default:
							}
						}
					}
				}(i)
			}
			rf.mu.Unlock()
		}
	}()

	// ---- leader loop: every heartbeat interval (or when woken) send
	// AppendEntries to every follower, each in its own goroutine so a
	// slow/unreachable peer never delays the others ----
	go func() {
		for {
			select {
			case <-rf.wake:
			case <-time.After(120 * time.Millisecond):
			}

			rf.mu.Lock()
			if rf.dead {
				rf.mu.Unlock()
				return
			}
			if rf.state != 2 {
				rf.mu.Unlock()
				continue
			}
			term := rf.currentTerm
			rf.mu.Unlock()

			for i := range peers {
				if i == me {
					continue
				}
				go func(server int) {
					// send AppendEntries, backing up until the follower accepts
					for {
						rf.mu.Lock()
						if rf.dead || rf.state != 2 || rf.currentTerm != term {
							rf.mu.Unlock()
							break
						}
						next := rf.nextIndex[server]
						prev := next - 1
						entries := make([]LogEntry, len(rf.log)-next)
						copy(entries, rf.log[next:])
						args := &AppendEntriesArgs{
							Term:         term,
							LeaderId:     rf.me,
							PrevLogIndex: prev,
							PrevLogTerm:  rf.log[prev].Term,
							Entries:      entries,
							LeaderCommit: rf.commitIndex,
						}
						rf.mu.Unlock()

						reply := &AppendEntriesReply{}
						if !rf.sendAppendEntries(server, args, reply) {
							break // lost; the next heartbeat retries
						}

						rf.mu.Lock()
						if rf.dead || rf.state != 2 || rf.currentTerm != term {
							rf.mu.Unlock()
							break
						}
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = 0
							rf.votedFor = -1
							rf.mu.Unlock()
							break
						}
						if reply.Success {
							m := prev + len(entries)
							if m > rf.matchIndex[server] {
								rf.matchIndex[server] = m
							}
							if m+1 > rf.nextIndex[server] {
								rf.nextIndex[server] = m + 1
							}
							// advance commitIndex: majority replicated and entry from current term
							for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
								if rf.log[n].Term != rf.currentTerm {
									break
								}
								count := 0
								for j := range rf.peers {
									if rf.matchIndex[j] >= n {
										count++
									}
								}
								if count > len(rf.peers)/2 {
									rf.commitIndex = n
									break
								}
							}
							rf.mu.Unlock()
							break
						}

						// rejected because of a log inconsistency: back up
						if rf.nextIndex[server] != next { // stale reply
							rf.mu.Unlock()
							break
						}
						n := reply.ConflictIndex
						if reply.ConflictTerm != -1 {
							for j := len(rf.log) - 1; j >= 1; j-- {
								if rf.log[j].Term == reply.ConflictTerm {
									n = j + 1
									break
								}
							}
						}
						if n >= next {
							n = next - 1
						}
						if n < 1 {
							n = 1
						}
						rf.nextIndex[server] = n
						rf.mu.Unlock()
					}
				}(i)
			}
		}
	}()

	// ---- applier: deliver committed entries in order, exactly once ----
	go func() {
		for {
			rf.mu.Lock()
			if rf.dead {
				rf.mu.Unlock()
				return
			}
			if rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				msg := ApplyMsg{Index: rf.lastApplied, Command: rf.log[rf.lastApplied].Command}
				rf.mu.Unlock()
				rf.applyCh <- msg // send outside the lock
				continue
			}
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}()

	return rf
}
