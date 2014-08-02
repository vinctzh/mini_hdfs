## 2014-08-02
---
TODO:

1. NameNode创建时，根据NameNode本地的log初始化NameNode，包括，系统中保存的文件及相关信息（大小等）
2. NameNode对新文件分块完成后，在本地记录，维护两个文件log（Files.log, FilesUnderConstruction.log）;
3. 第一个DataNode 接收到client的数据块后，接着向向一个DataNode发送数据块的测试。
4. 系统中的文件列表（根据NameNode维护的两个fiels×.log）
5. 下载文件
5. 删除文件


DONE:

1. Client添加新文件。（NameNode分配DataNode，将分块信息发送给client，client向目标块发送块信息）
## 2014-07-30
---
TODO:

1. Client和NameNode之间的通信
2. Client与DataNode之间的通信
3. DataNode与NameNode之间的心跳连接
4. Client向DataNode发送Blick数据
