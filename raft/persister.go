package raft

import (
	"fmt"
	"github.com/owenliang/go-raft-lib/util"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

type Persister struct {
	mu        sync.Mutex
	// todo:目前这俩持久化内容在内存和磁盘各一份，实际应该让snapshot仅放在磁盘上，这样内存空间才能释放出来，不过目前也够用了
	raftstate []byte
	snapshot  []byte

	raftStateMemSynced bool
	snapshotInMemSynced bool

	dir string
}

func MakePersister(dir string) (ps *Persister, err error) {
	if err = os.MkdirAll(dir, 0777); err != nil {
		return
	}
	ps = &Persister{
		dir: dir,
	}
	return
}

// 将raft状态同步到磁盘
func (ps *Persister) syncRaftStateToDisk() {
	filename := path.Join(ps.dir, "rf.state")
	ps.mustWriteFile(filename, ps.raftstate)
}

// 从磁盘加载raft状态
func (ps *Persister) loadRaftStateFromDisk() {
	if ps.raftStateMemSynced {
		return
	}

	ps.raftStateMemSynced = true

	filename := path.Join(ps.dir, "rf.state")
	ps.raftstate = ps.mustReadFile(filename)
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.raftstate = state
	ps.raftStateMemSynced = true
	ps.syncRaftStateToDisk()
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.loadRaftStateFromDisk()
	return ps.raftstate
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.loadRaftStateFromDisk()
	return len(ps.raftstate)
}

// 将snapshot同步到磁盘，用lastIncludedIndex作为文件名
func (ps *Persister) syncSnapshotToDisk(lastIncludedIndex int) {
	filename := path.Join(ps.dir, fmt.Sprintf("rf.snapshot-%d", lastIncludedIndex))
	ps.mustWriteFile(filename, ps.snapshot)
}

// 清理更新index的snapshot文件
func (ps *Persister) cleanOlderSnapshot(lastIncludedIndex int ) {
	if fileList, err := ioutil.ReadDir(ps.dir); err != nil {
		log.Fatal(err)
	} else {
		for _, file := range fileList {
			if file.IsDir() {
				continue
			}
			p := file.Name()
			ext := path.Ext(p)
			if !strings.HasPrefix(ext, ".snapshot-") {
				continue
			}
			if index, err := strconv.Atoi(p[len("rf.snapshot-"):]); err != nil {
				continue
			} else {
				if index < lastIncludedIndex {
					fullpath := path.Join(ps.dir, p)
					os.Remove(fullpath)
					util.DPrintf("删除过期snapshot: %v\n", fullpath)
				}
			}
		}
	}
}

func (ps *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte, lastIncludedIndex int) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.raftstate = state
	ps.snapshot = snapshot
	ps.raftStateMemSynced = true
	ps.snapshotInMemSynced = true

	// 顺序很重要: 先同步snapshot到磁盘,再更新raft状态（元数据），最后清理旧snapshot
	ps.syncSnapshotToDisk(lastIncludedIndex)
	ps.syncRaftStateToDisk()
	ps.cleanOlderSnapshot(lastIncludedIndex)
}

// 从磁盘加载snapshot
func (ps *Persister) loadSnapshotFromDisk(lastIncludedIndex int) {
	if ps.snapshotInMemSynced {
		return
	}
	ps.snapshotInMemSynced = true

	filename := path.Join(ps.dir, fmt.Sprintf("rf.snapshot-%d", lastIncludedIndex))
	ps.snapshot = ps.mustReadFile(filename)
}

func (ps *Persister) ReadSnapshot(lastIncludedIndex int) []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.loadSnapshotFromDisk(lastIncludedIndex)
	return ps.snapshot
}

func (ps *Persister) SnapshotSize(lastIncludedIndex int) int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ps.loadSnapshotFromDisk(lastIncludedIndex)
	return len(ps.snapshot)
}

func (ps *Persister) mustReadFile(filename string) (data []byte) {
	var err error
	var file *os.File

	// 文件不存在则返回
	if _, err = os.Stat(filename); err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Fatal(err)
	}

	if file, err = os.Open(filename); err != nil {
		log.Fatal(err)
	}
	if data, err = ioutil.ReadAll(file); err != nil {
		log.Fatal(err)
	}
	file.Close()
	return
}

func (ps *Persister) mustWriteFile(filename string, data []byte) {
	var err error
	var file *os.File

	// 文件操作失败，直接宕掉即可
	tmpFilename := filename + "-tmp"
	if file, err = os.Create(tmpFilename); err != nil {
		log.Fatal(err)
	}
	if _, err = file.Write(data); err != nil {
		log.Fatal(err)
	}
	if err = file.Sync(); err != nil {
		log.Fatal(err)
	}
	file.Close()
	if err = os.Rename(tmpFilename, filename); err != nil {
		log.Fatal(err)
	}
}