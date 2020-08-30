package raft

import (
	"testing"
	"time"
)

func TestRaft(t *testing.T) {
	addrs := []string{"127.0.0.1:1500", "127.0.0.1:1501", "127.0.0.1:1502"}

	for i := 0; i< len(addrs); i++ {
		go func(idx int) {
			Make(addrs, idx, MakePersister(), make(chan ApplyMsg, 1))
		}(i)
	}

	time.Sleep(1 * time.Hour)
}