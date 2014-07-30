package namenode;

import java.util.Arrays;

import datanode.DataNodeDescriptor;

public class BlockInfo extends Block {
	private INodeFile file;
	private Object[] triplets;
	
	public BlockInfo() {
		
	}
	
	protected BlockInfo(short replication) {
		this.file = null;
		triplets = new Object[3*replication];
	}
	
	public BlockInfo(INodeFile file, long blkid, long numBytes, long generationTime, short replication) {
		super(blkid, numBytes, replication);
		this.file = file;
		this.triplets = new Object[3*replication];
	}

	public INodeFile getFile() {
		return file;
	}

	public void setFile(INodeFile file) {
		this.file = file;
	}

	public boolean addDataNode(DataNodeDescriptor datanode, int index) {
		// ����Ѿ��������DataNode���򷵻�false
		if (findDatanode(datanode) >=0 )
			return false;
		// ���򣬽�DataNode�ӵ�ĩβ
		int lastNode = ensureCapacity(1);	// ���һ��triplet��λ��+1
		setDatanode(lastNode, datanode);
		setNext(lastNode,null);
		setPrevious(lastNode, null);
		
		return true;
	}
	
	void setNext(int index, BlockInfo to) {
		triplets[index*3 +2] = to;
	}
	
	void setPrevious(int index, BlockInfo to) {
		triplets[index*3 +1]= to;
	}
	
	void setDatanode(int index, DataNodeDescriptor node) {
		triplets[index*3] = node;
	}
	
	// ��֤��tripletsĩβ���㹻�Ŀռ�����num����Ԫ��
	private int ensureCapacity(int num) {
		int lastNode = numNodes();
		if (triplets.length >= ((lastNode+num)*3))
				return lastNode;
		Object[] old = triplets;
		triplets = new Object[(lastNode + num)*3];
		for (int i=0; i<lastNode*3; i++)
			triplets[i] = old[i];
		
		return lastNode;
	}
	
	/**
	 * �������BlockĿǰ�����ļ���DataNode����
	 * @return
	 */
	int numNodes() {
		int capacity = getCapacity();
		for (int i = capacity-1; i>= 0; i--) {
			if (getDataNode(i) != null)
				return i+1;
		}
		return 0;
	}
	public int findDatanode(DataNodeDescriptor dn) {
		int len = getCapacity();
		for (int idx = 0; idx < len; idx++) {
			DataNodeDescriptor cur = getDataNode(idx);
			// TODO: Ҳ���жϵķ�������ֱ����==
			if (cur == dn) 
				return idx;
			if (cur == null) 
				break;
		}
		return -1;
	}
	
	DataNodeDescriptor getDataNode(int index) {
		assert this.triplets != null : "BlockInfo is not initialized";
		assert index >= 0 && index*3 < triplets.length : "Index is out of bound";
		DataNodeDescriptor node = (DataNodeDescriptor)triplets[index * 3];
		return node;
	}
	int getCapacity() {
		assert this.triplets != null : "BlockInfo is not initialized";
		assert triplets.length % 3 == 0 : "Malformed BlockInfo";
		return triplets.length / 3;
	}

	@Override
	public String toString() {
//		return "BlockInfo [file=" + file + ", triplets="
//				+ Arrays.toString(triplets) + "]";
		
		return "BlockInfo [file=" + file
				+ ", Blk_ID=" + this.getBlockId() 
				+ ", Blk_size=" + this.getNumBytes()
				+ ", Blk_genTime=" + this.getGenerationTime()
				+ Arrays.toString(triplets) + "]";
	}
}
