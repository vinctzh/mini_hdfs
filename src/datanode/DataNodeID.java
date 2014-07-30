package datanode;

import javax.xml.crypto.Data;

public class DataNodeID {
	public String name;
	public String storageID;
	protected int infoPort;
	protected int ipcport;
	
	public DataNodeID(DataNodeID from) {
		this(from.getStorageID(),from.getName(), from.getInfoPort(), from.getIpcport());
	}
	
	public DataNodeID() {
		this.name = "none";
		this.storageID = "none";
		this.infoPort = 0;
		this.ipcport = 0;
	}
	
	public DataNodeID(String storageID, String name) {
		this.name = name;
		this.storageID = storageID;
		this.infoPort = 0;
		this.ipcport = 0;
	}

	public DataNodeID(String storageID, String name, int infPort, int icpPort) {
		this.name = name;
		this.storageID = storageID;
		this.infoPort = infPort;
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
	public int getInfoPort() {
		return infoPort;
	}
	public void setInfoPort(int infoPort) {
		this.infoPort = infoPort;
	}
	public int getIpcport() {
		return ipcport;
	}
	public void setIpcport(int ipcport) {
		this.ipcport = ipcport;
	}

	@Override
	public String toString() {
		return "DataNodeID [name=" + name + ", storageID=" + storageID
				+ ", infoPort=" + infoPort + ", ipcport=" + ipcport + "]";
	}
	
	
}
