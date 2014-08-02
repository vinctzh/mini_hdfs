package client;

import java.util.Scanner;

import namenode.NameNodeService.ClientConnection;
import common.MiniHDFSConstants;

public class Client {
	
	public static final String CLIENT_ROOT = "/home/jianyuan/";
	public static final String CLIENT_CACHE = "/home/jianyuan/cache/";

	public static void main(String[] args) {
		ClientOperations operations = new ClientOperations();
		boolean isConnected = false; 
		Scanner sc = new Scanner(System.in);
		
		while (true) {
			System.out.println("请出入你要执行的操作：");
			String opt = sc.next();
			
			if (opt.equals(MiniHDFSConstants.ADDFILE)) {
				System.out.println("请出入你要添加的文件路径：");
				String localFilePath = sc.next();
				operations.addFile(localFilePath);
				System.out.println("继续操作");
			} else if(opt.equals(MiniHDFSConstants.LSFILES))  {
				System.out.println("列出MiniHDFS上的所有文件：");
				operations.listFile();
			}
			
		}
	}

}
