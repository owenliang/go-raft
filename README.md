# go-raft

可靠的Raft算法实现，脱胎于MIT6.824课程大作业，通过全部正确性测试。

了解更多细节与心得，请跳转这个项目：[mit-6.824](https://github.com/owenliang/mit-6.824)。

## 待优化

* 过期snapshot文件清理。
* snapshot尺寸不受限于内存。
* snapshot分块传输。