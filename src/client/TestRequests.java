package client;

import java.io.File;

import namenode.NameNode;
import namenode.NameNodeService;

public class TestRequests {
	
	public static void main(String[] args) {
		NameNode nameNode = new NameNode();
		nameNode.showActiveDataNodes();
		
//		String filepath = "/home/jianyuan/Workspace/MiniHDFS/xsh0242_02_03.MP4";
//		File file = new File(filepath);
//		if (file == null || !file.exists())
//			System.out.println("文件不存在，或无法打开！");
//		else{
//			System.out.println(file.toString() + file.length());
//			nameNode.addFile(file,(short)2);
//		}
		
		NameNodeService nnService = new NameNodeService();
		
	}
}
