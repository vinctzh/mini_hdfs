package client;

import java.util.Scanner;

import namenode.NameNodeService.ClientConnection;
import common.MiniHDFSConstants;

public class Client {
	
	

	public static void main(String[] args) {
		ClientOperations operations = new ClientOperations();
		boolean isConnected = false; 
		Scanner sc = new Scanner(System.in);
		System.out.println("请出入你要执行的操作：");
		while (true) {
			
			String opt = sc.next();
			
			if (opt.equals(MiniHDFSConstants.ADDFILE)) {
				System.out.println("请出入你要添加的文件路径：");
				String localFilePath = sc.next();
				operations.addFile(localFilePath);
			}
			
		}
	}

}
