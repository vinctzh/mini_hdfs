package client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

import namenode.NameNodeService.ClientConnection;
import net.sf.json.JSONObject;
import common.FileHelper;
import common.MiniHDFSConstants;

public class Client {
	
	public static String CLIENT_ROOT = "/home/jianyuan/";
	public static String CLIENT_CACHE = "/home/jianyuan/cache/";
	public static String CLIENT_DOWNLOAD = "/home/jianyuan/fromHDFS/";

	
	public static boolean add_file_locked = false;
	
	public static void main(String[] args) {
		
		if (args.length != 1) {
			System.err.println("Uage: Client [local config file]\n\t eg: Client clientconfig.txt");
			return;
		}
		try {
			String configStr = FileHelper.loadFileIntoString(args[0], "UTF-8");
			JSONObject configJSON = JSONObject.fromObject(configStr);
			CLIENT_ROOT = configJSON.getString("root");
			CLIENT_CACHE = configJSON.getString("cache");
			CLIENT_DOWNLOAD = configJSON.getString("download");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		} 
		
		
		ClientOperations operations = new ClientOperations();
		Scanner sc = new Scanner(System.in);
		
		while (true) {
			System.out.println("请出入你要执行的操作：");
			String opt = sc.next();
			
			if (opt.equals(MiniHDFSConstants.ADDFILE)) {
				if (add_file_locked) {
					System.err.println("当前有文件正在被上传，请稍后再上传^-^");
					continue;
				}
				System.out.println("请出入你要添加的文件路径：");
				String localFilePath = sc.next();
				String filePath = Client.CLIENT_ROOT + localFilePath;
				if (FileHelper.fileExists(filePath)) {
					operations.addFile(localFilePath);
				} else {
					System.err.println("文件不存在～～");
				}
				
				System.out.println("继续操作");
			} else if(opt.equals(MiniHDFSConstants.LSFILES))  {
				System.out.println("列出MiniHDFS上的所有文件：");
				operations.listFile();
			} else if (opt.equals(MiniHDFSConstants.RMFILE)) {
				System.out.println("请输入要删除的文件名：");
				String filename  = sc.next();
				System.out.println("正在为您删除HDFS上的文件"+filename);
				if (operations.removeFile(filename)) {
					System.out.println("删除文件成功！！");
				} else {
					System.err.println("删除文件失败！！");
				}
			} else if (opt.equals(MiniHDFSConstants.COPYFILE)) {
				System.out.println("请输入要拷贝到本地的文件名：");
				String filename  = sc.next();
				System.out.println("正在为您拷贝HDFS上的文件"+filename);
				if (operations.copyFile(filename))
					System.out.println("已经完成拷贝"+filename);
				else
					System.out.println("拷贝"+filename+"失败");
			} else {
				System.out.println(opt + "不是合法命令");
			}
		}
	}

}
