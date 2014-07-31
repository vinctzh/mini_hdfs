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
		// block �Ͳ�ͬ������DataNode�Ķ�Ӧ
		BlockInfo blocks[] = new BlockInfo[blockNums];
		for (int i=0; i<blockNums; i++) {
			Set<String> targets = new HashSet<String>();
			long blkID = Math.abs(rand.nextLong());
			long blkSize;
			if (i == (blockNums-1))
				blkSize = sizeOfLastBlock;
			else
				blkSize = blockSize;
			
			// TODO: ����BlockInfo��������InodeFile
			BlockInfo blkInfo = new BlockInfo(file,blkID,blkSize,System.currentTimeMillis(),replication);

			// ��Ϊset�ǲ������ظ��ģ��������targeInd��targets���Ѵ���
			// �򲻻�ϲ������Ը������ԭ������֤��ͬһ��block�����䵽��datanode
			// ������ͬһ����
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
