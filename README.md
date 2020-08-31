# go-raft

The reliable Raft algorithm is implemented and passed all the correctness tests of [MIT6.824](https://github.com/owenliang/mit-6.824).

## TODO

* Memory-independent snapshot size
* Transmission snapshot block
* Native client

## Quick start

Construct a raft cluster of 3 nodes, node-02 delayed joining

```
cd raft
go test
```
