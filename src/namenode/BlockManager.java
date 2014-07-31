package namenode;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

public class BlockManager {
	
	public Random rand = new Random(System.currentTimeMillis());
 
	public BlockInfo[] allocBlocksForFile(INodeFile file, long fileSize, long blockSize, short replication) {
		int blockNums = 0;
		if (fileSize <= blockSize)
			blockNums = 1;
		else if ((fileSize%blockSize) == 0) {
			blockNums = (int) (fileSize / blockSize);
		} else {
			blockNums = (int) (fileSize / blockSize) + 1;
		}
		
		long sizeOfLastBlock = fileSize - (blockNums-1)*blockSize;
		
		int activeNodesNum = NameNode.activeDatanodeID.size();
		// block 和不同副本的DataNode的对应
		BlockInfo blocks[] = new BlockInfo[blockNums];
		for (int i=0; i<blockNums; i++) {
			Set<String> targets = new HashSet<String>();
			long blkID = Math.abs(rand.nextLong());
			long blkSize;
			if (i == (blockNums-1))
				blkSize = sizeOfLastBlock;
			else
				blkSize = blockSize;
			
			// TODO: 这里BlockInfo关联的是InodeFile
			BlockInfo blkInfo = new BlockInfo(file,blkID,blkSize,System.currentTimeMillis(),replication);

			// 因为set是不允许重复的，所以如果targeInd在targets中已存在
			// 则不会合并。所以根据这个原理，来保证对同一个block，分配到的datanode
			// 不会是同一个。
			while (targets.size()<replication) {
				int targetInd = rand.nextInt(activeNodesNum);
				String targetID = NameNode.activeDatanodeID.get(targetInd);
				targets.add(targetID);
			}
			
			Iterator<String> it = targets.iterator();
			int index = 0;
			while(it.hasNext()) {
				String tmp = it.next();
				blkInfo.addDataNode(NameNode.activeDataNodes.get(tmp), index);
			}
			
			blocks[i] = blkInfo;
		}
		return blocks;
	}
}
