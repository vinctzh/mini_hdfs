package namenode;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import common.FileHelper;
import common.LocalFileDescription;
import datanode.DataNodeDescriptor;

public class NameNode {
	
	private final String NAMENODE_ROOT = "/home/jianyuan/namenode/";
	public static long DEFAULT_BLOCK_SIZE = 6291456;//0;  // 60M
	BlockManager blockManager;
	
	// ��ǰ��Ծ�����ݽڵ�� ID
	public static List<String> activeDatanodeID; 	// storageID
	// ��ǰ��Ծ�����ݽڵ�<DatanodeID, DataNode>
	public static HashMap<String, DataNodeDescriptor> activeDataNodes;
	// ʧȥ���ӵ����ݽڵ�
//	public static HashMap<String, DataNodeDescriptor> deadDataNodes;
	
	public static HashMap<String, INodeFile> files;
	public static HashMap<String, INodeFileUnderConstruction> filesUnderConstruction;
	
	// DataNode��Ҫ���Ĳ���log
	public static HashMap<String, JSONArray> optTODOLogs;
	
	public NameNode() {
		blockManager = new BlockManager();
		activeDataNodes = new HashMap<String, DataNodeDescriptor>();
		activeDatanodeID = new ArrayList<String>();
		optTODOLogs = new HashMap<String, JSONArray>();
		initialNN();
	}
	
	public boolean initialNN() {
		loadLocalLogs();
		showFiles();
		showUCFiles();
		return true;
	}
	
	
	/**
	 * ���ݱ����ļ���Ϣ����NameNode�����ļ���ָ�ɴ����block��DataNode
	 * ����BlockInfo
	 * @param file
	 * @return
	 */
	public BlockInfo[] addFile(LocalFileDescription file) {
		
		// û�л��DataNode
		if (activeDatanodeID.isEmpty())
			return null;
		
		if (activeDatanodeID.size() < file.getReplication())
			return null;
		String fileName = file.getName();
		
		if (existFile(fileName)) {
			return null;
		}
		Long fileSize = file.getLength();
		short replication = (short) file.getReplication();
		
		INodeFileUnderConstruction iFileUC = new INodeFileUnderConstruction(fileName,replication,fileSize);
		
		BlockInfo[] blocks =blockManager.allocBlocksForFile(iFileUC, fileSize, DEFAULT_BLOCK_SIZE, replication);
		for (int i=0; i<blocks.length; i++) {
			showBlockInfo(blocks[i]);
		}
		JSONObject fileBlkInfos = NameNodeService.blkInfoToJsonData(blocks, replication);
		String fileInfoPath = this.NAMENODE_ROOT + "/details/" + fileName + ".detail";
		FileHelper.saveStringIntoFile(fileInfoPath, fileBlkInfos.toString());
		filesUnderConstruction.put(fileName, iFileUC);
		showFiles();
		showUCFiles();
		updateNNLog();
		return blocks;
	}
	
	public JSONObject copyFile(String fileName) {
		//�ļ�����
		JSONObject locatedFile = new JSONObject();
		
		if (files.containsKey(fileName)) {
			JSONObject fileDetail = getFileDetail(fileName);
			int blockNums = fileDetail.getInt("blockNum");
			String fName = fileDetail.getString("filename");
			int replication = fileDetail.getInt("replication");
			
			JSONArray blocks = fileDetail.getJSONArray("blocks");
			JSONArray newBlocks = new JSONArray();
			if (blocks.size() != blockNums) {
				return new JSONObject();
			}
			int blkCount =  blocks.size();
			for (int i=0; i < blkCount ; i++) {
				JSONObject block = blocks.getJSONObject(i);
				JSONArray targets = block.getJSONArray("targets");
				JSONArray activeTargets = new JSONArray();
				JSONArray deadTargets = new JSONArray();
				for (int j=0; j < targets.size(); j++) {
					JSONObject target = targets.getJSONObject(j);
					if (activeDatanodeID.contains(target.getString("storageId"))) {
						activeTargets.add(target);
					} else {
						deadTargets.add(target);
					}
					block.remove("targets");
					block.put("activeTargets", activeTargets);
					block.put("deadTargets", deadTargets);
				}
				newBlocks.add(block);
			}
			locatedFile.put("filename", fName);
			locatedFile.put("blockNum", blockNums);
			locatedFile.put("replication", replication);
			locatedFile.put("blocks", newBlocks);
		} else {
			return new JSONObject();
		}
		
		return locatedFile;
	}
	
	private JSONObject getFileDetail(String fileName) {
		JSONObject fileDetail = new JSONObject();
		String detailPath = NAMENODE_ROOT + "/details/" + fileName + ".detail";
		try {
			String fileDetailStr = FileHelper.loadFileIntoString(detailPath, "UTF-8");
			fileDetail = JSONObject.fromObject(fileDetailStr);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fileDetail;
	}
	private boolean existFile(String fileName) {
		if (files.containsKey(fileName))
			return true;
		if (filesUnderConstruction.containsKey(fileName))
			return true;
		return false;
	}
	
	// ��֧�ֶԴ����е��ļ�����ɾ��
	public boolean removeFile(String filename) {
		if (files.containsKey(filename)) {
			return removeFileInternal(filename);
		} 
		return false;
	}
	
	private boolean removeFileInternal(String filename) {
		if (files.containsKey(filename)) {
			JSONObject fileDetail = getFileDetail(filename);
			JSONArray blocks = fileDetail.getJSONArray("blocks");
			
			if (blocks.size() <= 0) {
				return false;
			}
			
			for (int i=0;i < blocks.size();i++) {
				JSONObject block = blocks.getJSONObject(i);
				long blockId = block.getLong("blockId");
				JSONArray targets = block.getJSONArray("targets");
				for (int j=0; j<targets.size(); j++) {
					JSONObject target = targets.getJSONObject(j);
					String storageID = target.getString("storageId");
					if (optTODOLogs.containsKey(storageID)) {
						JSONArray opts = optTODOLogs.get(storageID);
						JSONObject opt = new JSONObject();
						opt.put("opt", "rmblock");
						opt.put("object", blockId);
						opts.add(opt);
						optTODOLogs.put(storageID, opts);
					} else {
						JSONArray opts = new JSONArray();
						JSONObject opt = new JSONObject();
						opt.put("opt", "rmblock");
						opt.put("object", blockId);
						opts.add(opt);
						optTODOLogs.put(storageID, opts);
					}
				}
			}
			files.remove(filename);
			removeDetailFile(filename);
			showOptTODOLogs() ;
			updateNNLog();
			return true;
		} 
		return false;
	}
	
	private void removeDetailFile(String filename) {
		String detailPath = NAMENODE_ROOT + "/details/" + filename + ".detail";
		File detailFile = new File(detailPath);
		if (detailFile.exists() && detailFile.isFile()) {
			detailFile.delete();
		}
	}
	
	public JSONArray getOptsTODO(String storageID) {
		JSONArray optsTODO = new JSONArray();
		if (optTODOLogs.containsKey(storageID)) {
			optsTODO = optTODOLogs.get(storageID);
		}
		return optsTODO;	
	}
	
	public void setOptsTODO(String storageID, JSONArray optsTODO) {
		optTODOLogs.put(storageID, optsTODO);
	}
	public void removeOptsTODO(String storageID, JSONArray opts) {
		JSONArray optsTODO = getOptsTODO(storageID);
		for (int i=0; i< opts.size(); i++) {
			optsTODO.remove(opts.get(i));
		}
		setOptsTODO(storageID, optsTODO);
	}
	

	public boolean commitFileConstruction(String filename) {
		// ���ļ��ڴ����б���
		if (filesUnderConstruction.containsKey(filename)) {
			INodeFile iNodeFile = filesUnderConstruction.get(filename).convertToINodeFile();
			files.put(filename, iNodeFile);
			filesUnderConstruction.remove(filename);
			
			showFiles();
			showUCFiles();
			updateNNLog();
			
			return true;
		}
		return false;
	}
	
	public void updateNNLog() {
		// ���ݵ�ǰ���ļ�
		long curTime = System.currentTimeMillis();
		// ���ݵ�ǰ��log
		String fileBakName = "files" + curTime + ".log";
		String filesUcBakName = "filesUC" + curTime + ".log";
		File filesLog = new File(NAMENODE_ROOT + "files.log");
		File filesUCLog = new File(NAMENODE_ROOT + "filesUC.log");
		if (filesLog.isFile() && filesLog.exists()) {
			filesLog.renameTo(new File(NAMENODE_ROOT +"/logBackup/" + fileBakName));
		}
		if (filesUCLog.isFile() && filesUCLog.exists()) {
			filesUCLog.renameTo(new File(NAMENODE_ROOT  + "/logBackup/" + filesUcBakName));
		}
		
		// ���ڴ��е�logд������
		JSONObject filesinfo = convertFilesToJSONObject();
		JSONObject filesUCInfo = convertUCFilesToJSONObject();
		FileHelper.saveStringIntoFile(NAMENODE_ROOT + "files.log", filesinfo.toString());
		FileHelper.saveStringIntoFile(NAMENODE_ROOT + "filesUC.log", filesUCInfo.toString());
	}
	
	public JSONObject convertFilesToJSONObject() {
		JSONObject filesJson = new JSONObject();
		if (files.isEmpty())
			return filesJson;
		
//		Map<String, INodeFile>.Entry<String, INodeFile>
		
		Iterator<Entry<String, INodeFile>> fileIter = files.entrySet().iterator();
		JSONArray filesArray = new JSONArray();
		while (fileIter.hasNext()) {
			Map.Entry<String, INodeFile> entry = (Map.Entry<String, INodeFile>)fileIter.next();
			String key = entry.getKey();
			INodeFile file = entry.getValue();
			JSONObject fileJSON = new JSONObject();
			fileJSON.put("filename", file.getLocalNameString());
			fileJSON.put("replication", file.getReplication());
			fileJSON.put("fileSize", file.getFileSize());
			filesArray.add(fileJSON);
		}
		filesJson.put("files", filesArray);
		return filesJson;
	}
	
	public void loadLocalLogs() {
		try {
			String filesLog = FileHelper.loadFileIntoString(NAMENODE_ROOT + "files.log", "UTF-8");
			String filesUCLog = FileHelper.loadFileIntoString(NAMENODE_ROOT + "filesUC.log", "UTF-8");

			JSONObject filesLogJSON = JSONObject.fromObject(filesLog) ;
			JSONObject filesUCLogJSON = JSONObject.fromObject(filesUCLog);
			
			if (filesLogJSON.isEmpty()) {
				files = new HashMap<String, INodeFile>();
			} else {
				files = new HashMap<String, INodeFile>();
				JSONArray filesArray = filesLogJSON.getJSONArray("files");
				int filesCount = filesArray.size();
				for (int i=0; i < filesCount; i++) {
					String fileName = filesArray.getJSONObject(i).getString("filename");
					int replication = filesArray.getJSONObject(i).getInt("replication");
					long fileSize = filesArray.getJSONObject(i).getLong("fileSize");
					files.put(fileName, new INodeFile(fileName, (short)replication, fileSize));
				}
			}
			
			if (filesUCLogJSON.isEmpty()) {
				filesUnderConstruction = new HashMap<String, INodeFileUnderConstruction>();
			} else {
				filesUnderConstruction = new HashMap<String, INodeFileUnderConstruction>();
				JSONArray filesUCArray = filesUCLogJSON.getJSONArray("filesUC");
				int filesCount = filesUCArray.size();
				for (int i=0; i < filesCount; i++) {
					String fileName = filesUCArray.getJSONObject(i).getString("filename");
					int replication = filesUCArray.getJSONObject(i).getInt("replication");
					long fileSize = filesUCArray.getJSONObject(i).getLong("fileSize");
					filesUnderConstruction.put(fileName, new INodeFileUnderConstruction(fileName, (short)replication, fileSize));
				}
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public JSONObject convertUCFilesToJSONObject() {
		JSONObject filesUCJson = new JSONObject();
		if (filesUnderConstruction.isEmpty())
			return filesUCJson;
		
		Iterator<Entry<String, INodeFileUnderConstruction>> fileIter = filesUnderConstruction.entrySet().iterator();
		
		JSONArray filesArray = new JSONArray();
		while (fileIter.hasNext()) {
			Map.Entry<String, INodeFileUnderConstruction> entry = (Map.Entry<String, INodeFileUnderConstruction>)fileIter.next();
			String key = entry.getKey();
			INodeFile file = entry.getValue();
			JSONObject fileJSON = new JSONObject();
//			fileName,replication,fileSize
			fileJSON.put("filename", file.getLocalNameString());
			fileJSON.put("replication", file.getReplication());
			fileJSON.put("fileSize", file.getFileSize());
			filesArray.add(fileJSON);
		}
		filesUCJson.put("filesUC", filesArray);
		return filesUCJson;
	}
	
	public JSONObject getStoredFiles() {
		JSONObject storedFiles = new JSONObject();
		
		JSONObject files = convertFilesToJSONObject();
		JSONObject filesUnderConstruction = convertUCFilesToJSONObject();
		storedFiles.put("files", files);
		storedFiles.put("filesUC", filesUnderConstruction);
		
		return storedFiles;
	}
	/**
	 * ��һ��DataNode���ӵ�NameNode��ʱ�򣬸���NameNode�е�DataNode��Ϣ
	 * @param dataNode
	 */
	public void addDataNode(DataNodeDescriptor dataNode) {
		String nodeId = dataNode.getStorageID();
		// DataNode�Ѿ���������״̬
		// ���и���
		if (activeDatanodeID.contains(nodeId) && activeDataNodes.containsKey(nodeId)) {
			activeDataNodes.put(nodeId, dataNode);
			return ;
		}
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
		// ��ʼ��3�����DataNode
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
	
	public void showFiles() {
		if (files.isEmpty()) {
			System.err.println("û���ļ�");
		} else {
			System.err.println("�����ļ���Ϣ��");
			System.err.println("--> " +files.toString());
		}
	}
	public void showUCFiles() {
		if (filesUnderConstruction.isEmpty()) {
			System.err.println("û��UC�ļ�");
		} else {
			System.err.println("����UC�ļ���Ϣ��");
			System.err.println("--> " +filesUnderConstruction.toString());
		}
	}
	
	public void showOptTODOLogs() {
		int sz = optTODOLogs.size();
		if (optTODOLogs.isEmpty() || sz <= 0) {
			System.out.println("û��TODO��¼");
		}
		Iterator<Entry<String, JSONArray>> todoIter = optTODOLogs.entrySet().iterator();
		
		while(todoIter.hasNext()) {
			Map.Entry<String, JSONArray> entry = todoIter.next();
			String key = entry.getKey();
			JSONArray todos = entry.getValue();
			System.out.println(key+"Ҫ���еĲ�����");
			
			for (int i=0; i<todos.size(); i++) {
				JSONObject todo = todos.getJSONObject(i);
				System.out.println("-->"+todo.getString("opt")+"\t"+todo.getString("object"));
			}
			
		}
	}
}
