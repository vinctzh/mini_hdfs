package namenode;

import java.io.IOException;
import java.util.HashMap;

public class NameNodeDirectory {
	
	HashMap<String, INodeFile> storedfiles;
	HashMap<String, INodeFileUnderConstruction> storedFilesUC;	// file list to store those files under construction 

	// ����ļ�
	public void addFile(INodeFileUnderConstruction file) throws IOException {
		// ����ļ����ڣ��׳��ļ��Ѿ����ڵĴ���
		if (storedfiles.containsKey(file.getLocalNameString())) {
			throw new IOException("File already exists!");
		}
		storedFilesUC.put(file.getLocalNameString(), file);
	}
	
	
}
