package datanode;

public class DataNodeInfo extends DataNodeID {
	protected long capacity;
	protected long used	;
	protected long remaining;
	
	public DataNodeInfo() {
		
	}
	
	public DataNodeInfo(String storageID, String name,String ipAddr, long capacity, long used) {
		super(storageID, name, ipAddr);
		this.capacity = capacity;
		this.used = used;
		this.remaining = capacity - used;
	}
	
	public DataNodeInfo(String storageID, String name,long capacity, long used) {
		super(storageID, name);
		this.capacity = capacity;
		this.used = used;
		this.remaining = capacity - used;
	}
	
	public DataNodeInfo(DataNodeID nodeID) {
		super(nodeID);
		this.capacity = -1;
		this.used = -1;
		this.remaining = -1;
	}
	
	public DataNodeInfo(DataNodeID nodeID,long capacity, long used) {
		super(nodeID);		
		this.capacity = capacity;
		this.used = used;
		this.remaining = capacity - used;
	}
	
	public long getCapacity() {
		return capacity;
	}
	public void setCapacity(long capacity) {
		this.capacity = capacity;
	}
	public long getUsed() {
		return used;
	}
	public void setUsed(long used) {
		this.used = used;
	}
	public long getRemaining() {
		return remaining;
	}
	public void setRemaining(long remaining) {
		this.remaining = remaining;
	}
	
}
