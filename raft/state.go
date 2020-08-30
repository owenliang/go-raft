package raft

import (
	"bytes"
	"encoding/gob"
	"github.com/owenliang/go-raft-lib/util"
)

func (rf *Raft) persist() {
	// 不用加锁，外层逻辑会锁
	util.DPrintf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%d]", rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
	data := rf.raftStateForPersist()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) loadPersist() {
	data := rf.persister.ReadRaftState()
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	// 为了snaphost多做了2个持久化的metadata
	d.Decode(&rf.lastIncludedIndex)
	d.Decode(&rf.lastIncludedTerm)
}

func (rf *Raft) raftStateForPersist() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	return data
}
