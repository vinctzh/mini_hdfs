package common;

import java.net.Socket;

public class MiniHDFSConstants {
	// 是否调试
	public static final boolean doDebug = true;
	// Cient 和 NameNode通信的协议格式
	public static final String ADDFILE = "add";
	public static final String LSFILES = "ls";
	
	// NameNode Server IP地址
	public static final String SERVER = "127.0.0.1";
	
	public static final int SERVER_PORT4CLIENT = 1314;
	public static final int SERVER_PORT4DATANODE = 5200;
	
	// DataNode 
	public static final int DN_BLK_RECEIVER_PORT = 8807;
	public static final int DN_BLK_SENDER_PORT = 8808;
	public static final String DEFAULT_DN_DIR = "/home/jianyuan/storage/";
	
	// Client
	public static final int CLIENT_BLK_ACK_PORT = 9222;
	
}
