package client;

import java.util.Scanner;

import namenode.NameNodeService.ClientConnection;
import common.MiniHDFSConstants;

public class Client {
	
	

	public static void main(String[] args) {
		ClientOperations operations = new ClientOperations();
		boolean isConnected = false; 
		Scanner sc = new Scanner(System.in);
		System.out.println("�������Ҫִ�еĲ�����");
		while (true) {
			
			String opt = sc.next();
			
			if (opt.equals(MiniHDFSConstants.ADDFILE)) {
				System.out.println("�������Ҫ��ӵ��ļ�·����");
				String localFilePath = sc.next();
				operations.addFile(localFilePath);
			}
			
		}
	}

}
