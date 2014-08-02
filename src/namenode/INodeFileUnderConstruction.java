package namenode;

import java.util.Arrays;

public class INodeFileUnderConstruction extends INodeFile {

	public INodeFileUnderConstruction(String filename, long mTime, long acTime,
			short replication, long fileSize) {
		super(filename, mTime, acTime, replication, fileSize);
	}

	public INodeFileUnderConstruction(String filename, 	short replication, long fileSize) {
		super(filename, replication, fileSize);
	}
	
	public boolean isUnderConstruction() {
		return false;
	}
	
	public INodeFile convertToINodeFile() {
		return new INodeFile(getLocalNameString(), getReplication(), getFileSize());
	}

	@Override
	public String toString() {
		return "INodeFileUnderConstruction [replication=" + replication
				+ ", fileSize=" + fileSize + ", name="
				+ new String(name) + ", modificationTime="
				+ modificationTime + ", accessTime=" + accessTime + "]";
	}
	
}
