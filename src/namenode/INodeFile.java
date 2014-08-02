package namenode;

import java.util.Arrays;

public class INodeFile extends INode {
	
	short replication;
	
	long fileSize;
	
	public INodeFile(){
		super(null, 0L, 0L);
		this.replication = 0;
		this.fileSize = 1024L;
	}
	
	public INodeFile(String filename, short replication, long fileSize) {
		super(filename, 0L, 0L);
		this.replication = replication;
		this.fileSize = fileSize;
	}
	
	public INodeFile(String filename, long mTime, long acTime, short replication, long fileSize){
		super(filename, mTime, acTime);
		this.replication = replication;
		this.fileSize = fileSize;
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

	
	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	@Override
	public String toString() {
		return "INodeFile [replication=" + replication + ", fileSize="
				+ fileSize + ", name=" + new String(name)
				+ ", modificationTime=" + modificationTime + ", accessTime="
				+ accessTime + "]";
	}
}
