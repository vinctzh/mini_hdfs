package client;

import java.util.Scanner;

import namenode.NameNodeService.ClientConnection;
import common.FileHelper;
import common.MiniHDFSConstants;

public class Client {
	
	public static final String CLIENT_ROOT = "/home/jianyuan/";
	public static final String CLIENT_CACHE = "/home/jianyuan/cache/";
	public static final String CLIENT_DOWNLOAD = "/home/jianyuan/fromHDFS/";

	
	public static boolean add_file_locked = false;
	
	public static void main(String[] args) {
		ClientOperations operations = new ClientOperations();
		boolean isConnected = false; 
		Scanner sc = new Scanner(System.in);
		
		while (true) {
			System.out.println("�������Ҫִ�еĲ�����");
			String opt = sc.next();
			
			if (opt.equals(MiniHDFSConstants.ADDFILE)) {
				if (add_file_locked) {
					System.err.println("��ǰ���ļ����ڱ��ϴ������Ժ����ϴ�^-^");
					continue;
				}
				System.out.println("�������Ҫ��ӵ��ļ�·����");
				String localFilePath = sc.next();
				String filePath = Client.CLIENT_ROOT + localFilePath;
				if (FileHelper.fileExists(filePath)) {
					operations.addFile(localFilePath);
				} else {
					System.err.println("�ļ������ڡ���");
				}
				
				System.out.println("��������");
			} else if(opt.equals(MiniHDFSConstants.LSFILES))  {
				System.out.println("�г�MiniHDFS�ϵ������ļ���");
				operations.listFile();
			} else if (opt.equals(MiniHDFSConstants.RMFILE)) {
				System.out.println("������Ҫɾ�����ļ�����");
				String filename  = sc.next();
				System.out.println("����Ϊ��ɾ��HDFS�ϵ��ļ�"+filename);
				if (operations.removeFile(filename)) {
					System.out.println("ɾ���ļ��ɹ�����");
				} else {
					System.err.println("ɾ���ļ�ʧ�ܣ���");
				}
			} else if (opt.equals(MiniHDFSConstants.COPYFILE)) {
				System.out.println("������Ҫ���������ص��ļ�����");
				String filename  = sc.next();
				System.out.println("����Ϊ������HDFS�ϵ��ļ�"+filename);
				if (operations.copyFile(filename))
					System.out.println("�Ѿ���ɿ���"+filename);
				else
					System.out.println("����"+filename+"ʧ��");
			} else {
				System.out.println(opt + "���ǺϷ�����");
			}
			
			
		}
	}

}
