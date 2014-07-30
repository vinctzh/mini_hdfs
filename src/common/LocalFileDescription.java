package common;

import java.io.File;

public class LocalFileDescription {
	
	String name;
	String localPath;
	long length;
	int replication;
	
	public LocalFileDescription() {
		// TODO Auto-generated constructor stub
	}
	
	
	public LocalFileDescription(String name, String localPath, long length, int replication) {
		this.name = name;
		this.localPath = localPath;
		this.length = length;
		this.replication = replication;
	}
	
	public LocalFileDescription(File file){
		this.name = file.getName();
		this.localPath = file.getPath();
		this.length = file.length();
		this.replication = 1;
	}
	
	public LocalFileDescription(File file, int replication){
		this.name = file.getName();
		this.localPath = file.getPath();
		this.length = file.length();
		this.replication = replication;
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
}
