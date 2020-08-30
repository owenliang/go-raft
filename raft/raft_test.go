package raft

import (
	"fmt"
	"testing"
	"time"
)

func TestRaft(t *testing.T) {
	addrs := []string{"127.0.0.1:1500", "127.0.0.1:1501", "127.0.0.1:1502"}

	for i := 0; i < len(addrs); i++ {
		go func(idx int) {
			rf, applyChan, _ := Make(addrs, idx, fmt.Sprintf("./node%d", idx))
			go func() {
				cnt := 0
				value := ""
				for {
					msg := <-applyChan
					fmt.Printf("node%d 提交日志: %v\n", idx, msg)
					if msg.CommandValid {
						value = msg.Command.(string)
						cnt ++
						if cnt % 5 == 0 {	// 每5条日志做一次snapshot，仅为演示
							rf.TakeSnapshot([]byte(value), msg.CommandIndex)
						}
					} else {
						value = string(msg.Snapshot)
					}
				}
			}()
			// 只有leader的Start有作用
			for {
				rf.Start("一条日志")
				time.Sleep(5 * time.Second)
			}
		}(i)
	}

	time.Sleep(1 * time.Hour)
}
