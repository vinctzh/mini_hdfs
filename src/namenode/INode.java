package namenode;


public class INode {
	
	protected byte[] name;
	protected long modificationTime;
	protected long accessTime;
	
	public INode() {
		
	}
	
	public INode(INode file) {
		this.name = file.getLocalName();
		this.modificationTime = file.getModificationTime();
		this.accessTime = file.getAccessTime();
	}
	
	public INode(String name, long modificationTime, long accessTime) {
		this.name = name.getBytes();
		this.modificationTime = modificationTime;
		this.accessTime = accessTime;
	}
	
	public String getLocalNameString(){
		return new String(name);
	}
	public byte[] getLocalName() {
		return name;
	}
	public void setLocalName(byte[] name) {
		this.name = name;
	}
	public void setLocalName(String name) {
		this.name = name.getBytes();
	}
	public long getModificationTime() {
		return modificationTime;
	}
	public void setModificationTime(long modificationTime) {
		this.modificationTime = modificationTime;
	}
	public long getAccessTime() {
		return accessTime;
	}
	public void setAccessTime(long accessTime) {
		this.accessTime = accessTime;
	}
}