package namenode;

import java.io.IOException;

import common.FileHelper;
import net.sf.json.JSONObject;

public class NameNodeRunnable {
	
	public static void main(String[] args) {
		
		if (args.length != 1) {
			System.err.println("Usage:\tNameNodeRunnable configfilepath\n\t eg:\tNameNodeRunnable   namenodesetting.config");
			return;
		}
		
		try {
			String configStr = FileHelper.loadFileIntoString(args[0], "UTF-8");
			
			JSONObject configJSON = JSONObject.fromObject(configStr);
			String nnroot = configJSON.getString("nnroot");
			long blkSizeDefault = configJSON.getLong("blkSizeDefault");
			NameNode.DEFAULT_BLOCK_SIZE = blkSizeDefault;
			NameNode.NAMENODE_ROOT = nnroot;
			NameNodeService nnService = new NameNodeService();
			nnService.start();
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		
	}
}
