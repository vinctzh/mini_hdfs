package namenode;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import common.LocalFileDescription;

import datanode.DataNodeDescriptor;

public class NameNode {
	
	public static long DEFAULT_BLOCK_SIZE = 62914560;  // 60M
	BlockManager blockManager;
	public static List<String> activeDatanodeID;
	// 当前活跃的数据节点<DatanodeID, DataNode>
	public static HashMap<String, DataNodeDescriptor> activeDataNodes;
	
	// 失去连接的书觉节点
	public static HashMap<String, DataNodeDescriptor> deadDataNodes;
	
	public NameNode() {
		blockManager = new BlockManager();
		activeDataNodes = new HashMap<String, DataNodeDescriptor>();
		activeDatanodeID = new ArrayList<String>();
		testInit();
	}
	
	public void addFile(String src) {
		INodeFile file = new INodeFile(src,(short)2,1024L);
		BlockInfo[] blocks =blockManager.allocBlocksForFile(file, 10248L, 1024, (short)2);
		for (int i=0; i<blocks.length; i++) {
			showBlockInfo(blocks[i]);
		}
	}
	
	public void addFile(File file, short replication) {
		String fileName = file.getName();
		Long fileSize = file.length();
		
		INodeFile iFile = new INodeFile(fileName,replication,fileSize);
		BlockInfo[] blocks =blockManager.allocBlocksForFile(iFile, fileSize, DEFAULT_BLOCK_SIZE, replication);
		for (int i=0; i<blocks.length; i++) {
			showBlockInfo(blocks[i]);
		}
	}
	
	public void addFile(LocalFileDescription file) {
		String fileName = file.getName();
		Long fileSize = file.getLength();
		short replication = (short) file.getReplication();
		
		INodeFile iFile = new INodeFile(fileName,replication,fileSize);
		
		BlockInfo[] blocks =blockManager.allocBlocksForFile(iFile, fileSize, DEFAULT_BLOCK_SIZE, replication);
		for (int i=0; i<blocks.length; i++) {
			showBlockInfo(blocks[i]);
		}
	}
	
	private void testInit() {
		// 初始话3个活动的DataNode
//		public DataNodeDescriptor(String storageID, String name, long capacity,long used) 
		String id_prefix = "ID20140729_";
		String name_prefix = "DSNM_";
		for (int i=0;i<3;i++) {
			String id = id_prefix+i;
			DataNodeDescriptor dnd = new DataNodeDescriptor(id, name_prefix+i, 10240, 512);
			activeDatanodeID.add(id);
			activeDataNodes.put(id, dnd);
		}
	}
	
	public void showActiveDataNodes(){
		for (int i=0; i<activeDatanodeID.size(); i++) {
			System.out.println("DataNode " +i 
					+ activeDataNodes.get(activeDatanodeID.get(i)));
		}
	}
	
	public void showBlockInfo(BlockInfo block) {
		System.out.println(block.toString());
	}
}
