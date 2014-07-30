package namenode;

import java.io.IOException;
import java.util.HashMap;

public class NameNodeDirectory {
	
	HashMap<String, INodeFile> storedfiles;
	HashMap<String, INodeFileUnderConstruction> storedFilesUC;	// file list to store those files under construction 

	// 添加文件
	public void addFile(INodeFileUnderConstruction file) throws IOException {
		// 如果文件存在，抛出文件已经存在的错误
		if (storedfiles.containsKey(file.getLocalNameString())) {
			throw new IOException("File already exists!");
		}
		storedFilesUC.put(file.getLocalNameString(), file);
	}
	
	
}
