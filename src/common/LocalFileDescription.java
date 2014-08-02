package common;

import java.io.File;

import namenode.NameNode;

public class LocalFileDescription {
	
	String name;
	String localPath;
	long length;
	int replication;
	long blockSize;
	
	
	public LocalFileDescription() {
		this.blockSize = NameNode.DEFAULT_BLOCK_SIZE;
	}
	
	
	public LocalFileDescription(String name, String localPath, long length, int replication) {
		this.name = name;
		this.localPath = localPath;
		this.length = length;
		this.replication = replication;
		this.blockSize = NameNode.DEFAULT_BLOCK_SIZE;
	}
	
	public LocalFileDescription(File file){
		this.name = file.getName();
		this.localPath = file.getPath();
		this.length = file.length();
		this.replication = 1;
		this.blockSize = NameNode.DEFAULT_BLOCK_SIZE;
	}
	
	public LocalFileDescription(File file, int replication){
		this.name = file.getName();
		this.localPath = file.getPath();
		this.length = file.length();
		this.replication = replication;
		this.blockSize = NameNode.DEFAULT_BLOCK_SIZE;
	}


	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getLocalPath() {
		return localPath;
	}
	public void setLocalPath(String localPath) {
		this.localPath = localPath;
	}
	public long getLength() {
		return length;
	}
	public void setLength(long length) {
		this.length = length;
	}


	public int getReplication() {
		return replication;
	}


	public void setReplication(int replication) {
		this.replication = replication;
	}


	public long getBlockSize() {
		return blockSize;
	}


	public void setBlockSize(long blockSize) {
		this.blockSize = blockSize;
	}
}
