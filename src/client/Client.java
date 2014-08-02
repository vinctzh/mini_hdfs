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
			System.out.println("�������Ҫִ�еĲ�����");
			String opt = sc.next();
			
			if (opt.equals(MiniHDFSConstants.ADDFILE)) {
				System.out.println("�������Ҫ��ӵ��ļ�·����");
				String localFilePath = sc.next();
				operations.addFile(localFilePath);
				System.out.println("��������");
			} else if(opt.equals(MiniHDFSConstants.LSFILES))  {
				System.out.println("�г�MiniHDFS�ϵ������ļ���");
				operations.listFile();
			} else if (opt.equals(MiniHDFSConstants.RMFILE)) {
				System.out.println("������Ҫɾ�����ļ�����");
				String filename  = sc.next();
				System.out.println("����Ϊ��ɾ��HDFS�ϵ��ļ�"+filename);
			} else if (opt.equals(MiniHDFSConstants.COPYFILE)) {
				System.out.println("������Ҫ���������ص��ļ�����");
				String filename  = sc.next();
				System.out.println("����Ϊ������HDFS�ϵ��ļ�"+filename);
			} else {
				System.out.println(opt + "���ǺϷ�����");
			}
			
			
		}
	}

}
