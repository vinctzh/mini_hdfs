package namenode;

public class Block {
	private long blockId;
	private long numBytes;
	private long generationTime;
	
	public Block(){
	}
	
	public Block(long blkid, long numBytes, long generationTime) {
		setBlockId(blkid);
		setNumBytes(numBytes);
		setGenerationTime(generationTime);
	}
	
	public long getBlockId() {
		return blockId;
	}
	public void setBlockId(long blockId) {
		this.blockId = blockId;
	}
	public long getNumBytes() {
		return numBytes;
	}
	public void setNumBytes(long numBytes) {
		this.numBytes = numBytes;
	}
	public long getGenerationTime() {
		return generationTime;
	}
	public void setGenerationTime(long generationTime) {
		this.generationTime = generationTime;
	}
	
}
