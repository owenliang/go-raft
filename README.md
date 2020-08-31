# go-raft

[切换中文版本](https://github.com/owenliang/go-raft/blob/master/README-cn.md)

A reliable raft algorithm implementation which is bored from MIT6.824's Lab, all tests are passed.

Learn more about details and my experience, please take a look at：[mit-6.824](https://github.com/owenliang/mit-6.824).

## todo

* snapshot size won't be limited by memory.
* snapshot rpc with splite chunks.
* easy-to-use client to write log to the leader

## try

I build a raft cluster with 3 nodes, and node2 will join later after node0 and node1 for testing.

```
cd raft
go test
```