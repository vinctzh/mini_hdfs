package common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import client.Client;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class LocalBlocksCombine {
	
	public static boolean combineBlocks(String filename,String dstDir, JSONArray blocks, String cacheDir) {
		if (blocks == null || blocks.isEmpty())
			return false;
		
		dstDir = dstDir == null ? Client.CLIENT_DOWNLOAD : dstDir;
		cacheDir = cacheDir == null ? Client.CLIENT_CACHE : cacheDir;
		String dstFilePath = dstDir + filename;
		
		int blockNums = blocks.size();
		
		RandomAccessFile raf = null;
	    long alreadyWrite=0;		
	    FileInputStream fis=null;
	    
	    int len=0;
	    byte[] bt=new byte[1024];
	    
	    try {
			raf = new RandomAccessFile(dstFilePath, "rw");
			for (int i=0; i<blockNums; i++) {
				JSONObject block = blocks.getJSONObject(i);
				String blockPath = cacheDir + block.getString("blockId") + ".cache";
				long blkSize = block.getLong("blockSize");
				fis=new FileInputStream(blockPath);
				while ((len=fis.read(bt))>0) {
					raf.write(bt,0,len);
				}
				fis.close();
				alreadyWrite=alreadyWrite+blkSize;
			}
			raf.close();   
			
			// «Â¿Ìcache
			
			
		} catch (IOException e) {
			try {
				if(raf!=null)
				raf.close();
				 if(fis!=null)
			          fis.close();
			} catch (IOException e1) {
				e1.printStackTrace();
				return false;
			}
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
}
