# go-raft

[切换中文版本](https://github.com/owenliang/go-raft/blob/master/README-cn.md)

## INTRO

The reliable raft algorithm which is inspired by the mit6.824 (https://github.com/owenliang/mit-6.824) course, and has passed all the essential tests for accuracy and correctness 

For more details, please visit the project: [mit-6.824](https://github.com/owenliang/mit-6.824).

## TODO

* Optimize the snapshot size, due to current size is still limited by the memery 
* Optimize the current native client
* Optimize the snapshot Chunked transfer

## Try it!
Structured 3 raft cluster of node, in which node2 has delay join feature

```
cd raft
go test
```
