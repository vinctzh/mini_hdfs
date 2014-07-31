package datanode;

import common.FileHelper;

import net.sf.json.JSONObject;

public class DataNodeClient {

	public static void main(String[] args) {
		
//		DataNodeInfo data = new DataNodeInfo("ST001","Centvin", 1024000, 51200);
//		JSONObject json = new JSONObject();
//		json.put("storageID", data.getStorageID());
//		json.put("name", data.getName());
//		json.put("capacity", data.getCapacity());
//		json.put("used", data.getUsed());
//		FileHelper.saveStringIntoFile("storageInfo.txt", json.toString());
		
		DataNodeDameon dnd = new DataNodeDameon("storageInfo.txt");
		dnd.start();
	}
}
