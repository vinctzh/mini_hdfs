##2014-08-03
----
BUGS

1. **[done]**第二次添加文件时，BlockAckThread中的异常 
2. **[done]**在多个机器上测试时，DataNode向下一个DataNode发送block会出错。**（BlockTransfer：String blkPath = Client.CLIENT_CACHE + blkID + ".cache";）**
3. 添加文件时，文件名中带空格（进行文件名validation）
4. ###创建文件时NameNode荡机的情况（返回创建文件失败，删除NameNode和Client中的相应的记录）
5.  **[done]**DataNode结束时，NameNode不能判断DataNode已断开连接;

TODO

1. **添加新文件时，目标DataNode荡机的异常处理**;
2. 测试测试，发现bug;
3. 某些共享数据加锁。
4. 保存任务列表

DONE

1. 目前独立的4个功能已经完成。

## 2014-08-02
---
TODO:

1. NameNode创建时，根据NameNode本地的log初始化NameNode，包括，系统中保存的文件及相关信息（大小等）
2. NameNode对新文件分块完成后，在本地记录，维护两个文件log（Files.log, FilesUnderConstruction.log）;
3. 第一个DataNode 接收到client的数据块后，接着向向一个DataNode发送数据块的测试。
4. 系统中的文件列表（根据NameNode维护的两个fiels×.log）**[done]**
5. 下载文件
5. 删除文件
6. **DataNode启动时，扫描存储的block信息，通知NameNode**


DONE:

1. Client添加新文件。（NameNode分配DataNode，将分块信息发送给client，client向目标块发送块信息）
## 2014-07-30
---
TODO:

1. Client和NameNode之间的通信
2. Client与DataNode之间的通信
3. DataNode与NameNode之间的心跳连接
4. Client向DataNode发送Blick数据
