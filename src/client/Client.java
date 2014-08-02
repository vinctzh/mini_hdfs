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
			} else if (opt.equals(MiniHDFSConstants.RMFILE)) {
				System.out.println("请输入要删除的文件名：");
				String filename  = sc.next();
				System.out.println("正在为您删除HDFS上的文件"+filename);
			} else if (opt.equals(MiniHDFSConstants.COPYFILE)) {
				System.out.println("请输入要拷贝到本地的文件名：");
				String filename  = sc.next();
				System.out.println("正在为您拷贝HDFS上的文件"+filename);
			} else {
				System.out.println(opt + "不是合法命令");
			}
			
			
		}
	}

}
