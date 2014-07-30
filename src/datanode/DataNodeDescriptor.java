package datanode;

public class DataNodeDescriptor extends DataNodeInfo {
	public DataNodeDescriptor() {}
	
	public DataNodeDescriptor(DataNodeID nodeID) {
		this(nodeID, 0L, 0L);
	}
	
	public DataNodeDescriptor(String storageID, String name, long capacity,long used) {
		super(storageID, name, capacity, used);
	}
	
	public DataNodeDescriptor(DataNodeID nodeID, long capacity,long used) {
		super(nodeID,capacity,used);
	}

	@Override
	public String toString() {
//		return "DataNodeDescriptor [capacity=" + capacity + ", used=" + used
//				+ ", remaining=" + remaining + ", name=" + name
//				+ ", storageID=" + storageID + ", infoPort=" + infoPort
//				+ ", ipcport=" + ipcport + "]";
		return "DataNodeInfo [storageID="+ storageID 
				+ ", name=" + name 
				+ ", capacity=" + capacity 
				+ ", used=" + used
				+ ", remaining=" + remaining 
				+  ", infoPort=" + infoPort
				+ ", ipcport=" + ipcport + "]";
	}
	
	
}
