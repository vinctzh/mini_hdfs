package namenode;

public class INodeFileUnderConstruction extends INodeFile {

	public INodeFileUnderConstruction(String filename, long mTime, long acTime,
			short replication, long blockSize) {
		super(filename, mTime, acTime, replication, blockSize);
	}

	public boolean isUnderConstruction() {
		return false;
	}
	
}
