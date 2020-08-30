package raft

import (
	"github.com/owenliang/go-raft-lib/util"
	"time"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

// 日志是否需要压缩
func (rf *Raft) ExceedLogSize(logSize int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.persister.RaftStateSize() >= logSize {
		return true
	}
	return false
}

func (rf *Raft) installSnapshotToApplication() {
	var applyMsg *ApplyMsg

	// 同步给application层的快照
	applyMsg = &ApplyMsg{
		CommandValid:      false,
		Snapshot:          rf.persister.ReadSnapshot(rf.lastIncludedIndex),
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	}
	// 快照部分就已经提交给application了，所以后续applyLoop提交日志后移
	rf.lastApplied = rf.lastIncludedIndex

	util.DPrintf("RaftNode[%d] installSnapshotToApplication, snapshotSize[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, len(applyMsg.Snapshot), applyMsg.LastIncludedIndex, applyMsg.LastIncludedTerm)
	rf.applyCh <- *applyMsg
	return
}

// 保存snapshot，截断log
func (rf *Raft) TakeSnapshot(snapshot []byte, lastIncludedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 已经有更大index的snapshot了
	if lastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// 快照的当前元信息
	util.DPrintf("RafeNode[%d] TakeSnapshot begins, IsLeader[%v] snapshotLastIndex[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, rf.leaderId == rf.me, lastIncludedIndex, rf.lastIncludedIndex, rf.lastIncludedTerm)

	// 要压缩的日志长度
	compactLogLen := lastIncludedIndex - rf.lastIncludedIndex

	// 更新快照元信息
	rf.lastIncludedTerm = rf.log[rf.index2LogPos(lastIncludedIndex)].Term
	rf.lastIncludedIndex = lastIncludedIndex

	// 压缩日志
	afterLog := make([]LogEntry, len(rf.log)-compactLogLen)
	copy(afterLog, rf.log[compactLogLen:])
	rf.log = afterLog

	// 把snapshot和raftstate持久化
	rf.persister.SaveStateAndSnapshot(rf.raftStateForPersist(), snapshot, rf.lastIncludedIndex)

	util.DPrintf("RafeNode[%d] TakeSnapshot ends, IsLeader[%v] snapshotLastIndex[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, rf.leaderId == rf.me, lastIncludedIndex, rf.lastIncludedIndex, rf.lastIncludedTerm)
}

func (rf *Raft) doInstallSnapshot(peerId int) {
	util.DPrintf("RaftNode[%d] doInstallSnapshot starts, leaderId[%d] peerId[%d]\n", rf.me, rf.leaderId, peerId)

	args := InstallSnapshotArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.lastIncludedIndex
	args.LastIncludedTerm = rf.lastIncludedTerm
	args.Offset = 0
	args.Data = rf.persister.ReadSnapshot(rf.lastIncludedIndex)
	args.Done = true

	reply := InstallSnapshotReply{}

	go func() {
		if rf.sendInstallSnapshot(peerId, &args, &reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 如果不是rpc前的leader状态了，那么啥也别做了
			if rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm { // 变成follower
				rf.role = ROLE_FOLLOWER
				rf.leaderId = -1
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				return
			}
			rf.nextIndex[peerId] = rf.lastIndex() + 1      // 重新从末尾同步log（未经优化，但够用）
			rf.matchIndex[peerId] = args.LastIncludedIndex // 已同步到的位置，即快照后的log重新apply给application层
			rf.updateCommitIndex()                         // 更新commitIndex
			util.DPrintf("RaftNode[%d] doInstallSnapshot ends, leaderId[%d] peerId[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d]\n", rf.me, rf.leaderId, peerId, rf.nextIndex[peerId],
				rf.matchIndex[peerId], rf.commitIndex)
		}
	}()
}

// 安装快照RPC Handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) (err error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	util.DPrintf("RaftNode[%d] installSnapshot starts, rf.lastIncludedIndex[%d] rf.lastIncludedTerm[%d] args.lastIncludedIndex[%d] args.lastIncludedTerm[%d] logSize[%d]",
		rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LastIncludedIndex, args.LastIncludedTerm, len(rf.log))

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.persist()
		// 继续向下走
	}

	// 认识新的leader
	rf.leaderId = args.LeaderId
	// 刷新活跃时间
	rf.lastActiveTime = time.Now()

	// leader快照不如本地长，那么忽略这个快照
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	} else { // leader快照比本地快照长
		if args.LastIncludedIndex < rf.lastIndex() { // 快照外还有日志，判断是否需要截断
			if rf.log[rf.index2LogPos(args.LastIncludedIndex)].Term != args.LastIncludedTerm {
				rf.log = make([]LogEntry, 0) // term冲突，扔掉快照外的所有日志
			} else { // term没冲突，保留后续日志
				leftLog := make([]LogEntry, rf.lastIndex()-args.LastIncludedIndex)
				copy(leftLog, rf.log[rf.index2LogPos(args.LastIncludedIndex)+1:])
				rf.log = leftLog
			}
		} else {
			rf.log = make([]LogEntry, 0) // 快照比本地日志长，日志就清空了
		}
	}
	// 更新快照位置
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	// 持久化raft state和snapshot
	rf.persister.SaveStateAndSnapshot(rf.raftStateForPersist(), args.Data, rf.lastIncludedIndex)
	// snapshot提交给应用层
	rf.installSnapshotToApplication()
	util.DPrintf("RaftNode[%d] installSnapshot ends, rf.lastIncludedIndex[%d] rf.lastIncludedTerm[%d] args.lastIncludedIndex[%d] args.lastIncludedTerm[%d] logSize[%d]",
		rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LastIncludedIndex, args.LastIncludedTerm, len(rf.log))
	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
