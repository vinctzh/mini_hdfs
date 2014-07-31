package datanode;

import javax.xml.crypto.Data;

public class DataNodeID {
	public String name;
	public String storageID;
	
	protected String ipAddr;
	protected int blkPort;			// used to transfer block
	protected int ipcport;			// used to send block
	
	public DataNodeID(DataNodeID from) {
		this(from.getStorageID(),from.getName(), from.getIpAddr() , from.getBlkPort(), from.getIpcport());
	}
	
	public DataNodeID() {
		this.name = "none";
		this.storageID = "none";
		this.ipAddr = "127.0.0.1";
		this.blkPort = 0;
		this.ipcport = 0;
	}
	
	public DataNodeID(String storageID, String name) {
		this.name = name;
		this.storageID = storageID;
		this.ipAddr = "127.0.0.1";
		this.blkPort = 0;
		this.ipcport = 0;
	}
	
	public DataNodeID(String storageID, String name, String ipAddr) {
		this.name = name;
		this.storageID = storageID;
		this.ipAddr = ipAddr;
		this.blkPort = 0;
		this.ipcport = 0;
	}

//	public DataNodeID(String storageID, String name, int infPort, int icpPort) {
//		this.name = name;
//		this.storageID = storageID;
//		this.ipAddr = "127.0.0.1";
//		this.blkPort = infPort;
//		this.ipcport = icpPort;
//	}
	
	public DataNodeID(String storageID, String name, String ipAddr, int infPort, int icpPort) {
		this.name = name;
		this.storageID = storageID;
		this.ipAddr = ipAddr;
		this.blkPort = infPort;
		this.ipcport = icpPort;
	}
	
	
	//TODO: 添加构造函数
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getStorageID() {
		return storageID;
	}
	public void setStorageID(String storageID) {
		this.storageID = storageID;
	}
	public int getBlkPort() {
		return blkPort;
	}
	public void setBlkPort(int blkPort) {
		this.blkPort = blkPort;
	}
	public int getIpcport() {
		return ipcport;
	}
	public void setIpcport(int ipcport) {
		this.ipcport = ipcport;
	}

	public String getIpAddr() {
		return ipAddr;
	}

	public void setIpAddr(String ipAddr) {
		this.ipAddr = ipAddr;
	}

	@Override
	public String toString() {
		return "DataNodeID [name=" + name + ", storageID=" + storageID
				+ ", ipAddr=" + ipAddr
				+ ", blkPort=" + blkPort + ", ipcport=" + ipcport + "]";
	}
	
	
}
