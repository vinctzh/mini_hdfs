package common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.io.ObjectInputStream.GetField;

import client.Client;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class LocalFileSeperator {

	public static boolean seperateFile(String filepath, JSONObject blksInfo) {
		
		int blockNums = blksInfo.getInt("blockNum");
		
		long writtenSize = 0;
		JSONArray blocks = blksInfo.getJSONArray("blocks");
		for (int i=0; i < blockNums; i++) {
			JSONObject curBlock = blocks.getJSONObject(i);
			long blkID = curBlock.getLong("blockId");
			long blkSize = curBlock.getLong("blockSize");
			String blkCacheName = Client.CLIENT_CACHE + blkID + ".cache";
			if (writeFile(filepath,blkCacheName, blkSize, writtenSize)) {
				System.out.println("Block "+ blkID +"blkSize " + blkSize + " is created and saved in cache!");
				writtenSize = writtenSize + blkSize;
				System.out.println("written size: " + writtenSize);
			} else 
				return false ;
			
		}
		
		File locaFile = new File(filepath);
		if (locaFile.length() == writtenSize) 
			return true;
		
		return false;
	}
	
	
	private static boolean writeFile(String fileAndPath,String fileSeparateName,long blockSize,long beginPos)
	{
		RandomAccessFile randACFile = null;
		FileOutputStream oStream = null;
		byte[] bts = new byte[1024];
		long writeByte = 0;
		int len = 0;
		
		try {
			randACFile = new RandomAccessFile(fileAndPath, "r");
			randACFile.seek(beginPos);
			oStream = new FileOutputStream(fileSeparateName);
			
			while ((len = randACFile.read(bts)) > 0) {
				if (writeByte<blockSize){
					writeByte += len;
					if (writeByte <= blockSize) {
						oStream.write(bts,0,len);
					} else {
						len = len - (int)(writeByte - blockSize);
						oStream.write(bts,0,len);
					}
				}
			}
			
			oStream.close();
			randACFile.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			try {
				if (oStream != null) oStream.close();
				if (randACFile != null) randACFile.close();
				
			} catch(Exception e2) {
				return false;
			}
			return false;
		} 
		
		return true;
	}
	
}
