# go-raft

[切换中文版本](https://github.com/owenliang/go-raft/blob/master/README-cn.md)

A reliable raft algorithm implementation which is inspired by [MIT6.824](https://pdos.csail.mit.edu/6.824/) 's Lab and passed all the correctness tests.

Learn more about details and my experience, please take a look at：[mit-6.824](https://github.com/owenliang/mit-6.824).

## todo

* Memory-independent snapshot size
* InstallSnapshot RPC with split chunks.
* Simple client for talking to leader

## try

Construct a raft cluster of 3 nodes, and node2 deplayed to join.

```
cd raft
go test
```