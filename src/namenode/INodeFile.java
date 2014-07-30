package namenode;

public class INodeFile extends INode {
	
	short replication;
	
	long blockSize;
	
	public INodeFile(){
		super(null, 0L, 0L);
		this.replication = 0;
		this.blockSize = 1024L;
	}
	
	public INodeFile(String filename, short replication, long blockSize) {
		super(filename, 0L, 0L);
		this.replication = replication;
		this.blockSize = blockSize;
	}
	
	public INodeFile(String filename, long mTime, long acTime, short replication, long blockSize){
		super(filename, mTime, acTime);
		this.replication = replication;
		this.blockSize = blockSize;
	}
	
	public boolean isUnderConstruction() {
		return false;
	}
	
	public short getReplication() {
		return replication;
	}

	public void setReplication(short replication) {
		this.replication = replication;
	}

	public long getBlockSize() {
		return blockSize;
	}

	public void setBlockSize(long blockSize) {
		this.blockSize = blockSize;
	}
}
