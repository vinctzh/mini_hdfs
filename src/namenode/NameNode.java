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
	
	// 当前活跃的数据节点的 ID
	public static List<String> activeDatanodeID; 	// storageID
	// 当前活跃的数据节点<DatanodeID, DataNode>
	public static HashMap<String, DataNodeDescriptor> activeDataNodes;
	// 失去连接的数据节点
//	public static HashMap<String, DataNodeDescriptor> deadDataNodes;
	
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
	
	/**
	 * 根据本地文件信息，在NameNode创建文件，指派存放嗯block的DataNode
	 * 返回BlockInfo
	 * @param file
	 * @return
	 */
	public BlockInfo[] addFile(LocalFileDescription file) {
		String fileName = file.getName();
		Long fileSize = file.getLength();
		short replication = (short) file.getReplication();
		
		INodeFile iFile = new INodeFile(fileName,replication,fileSize);
		
		BlockInfo[] blocks =blockManager.allocBlocksForFile(iFile, fileSize, DEFAULT_BLOCK_SIZE, replication);
		for (int i=0; i<blocks.length; i++) {
			showBlockInfo(blocks[i]);
		}
		
		return blocks;
	}
	
	/**
	 * 当一个DataNode连接到NameNode的时候，更新NameNode中的DataNode信息
	 * @param dataNode
	 */
	public void addDataNode(DataNodeDescriptor dataNode) {
		String nodeId = dataNode.getStorageID();
		// DataNode已经处于在线状态
		// 进行更新
		if (activeDatanodeID.contains(nodeId) && activeDataNodes.containsKey(nodeId)) {
			activeDataNodes.put(nodeId, dataNode);
			return ;
		}
		// 活跃列表中不存在，检查已掉线的列表
		// 如果存在，添加到活跃列表，删除掉线列表
//		if (deadDataNodes.containsKey(nodeId)) {
//			activeDataNodes.put(nodeId, dataNode);
//			activeDatanodeID.add(nodeId);
//			deadDataNodes.remove(nodeId);
//		} 
		else {
			activeDatanodeID.add(nodeId);
			activeDataNodes.put(nodeId, dataNode);
		}
		return;		
	}
	
	public boolean deleteNode(String nodeID) {
		if (activeDatanodeID.contains(nodeID) && activeDataNodes.containsKey(nodeID)) {
			activeDataNodes.remove(nodeID);
			return activeDatanodeID.remove(nodeID);
		}
		return false;
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
