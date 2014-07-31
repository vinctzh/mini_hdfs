package common;

import java.net.Socket;

public class MiniHDFSConstants {
	// 是否调试
	public static final boolean doDebug = true;
	// Cient 和 NameNode通信的协议格式
	public static final String ADDFILE = "add";

	// NameNode Server IP地址
	public static final String SERVER = "127.0.0.1";
	
	public static final int SERVER_PORT4CLIENT = 1314;
	public static final int SERVER_PORT4DATANODE = 5200;
	
	// DataNode 
	public static final int DN_BLK_RECEIVER_PORT = 8807;
	public static final int DN_BLK_SENDER_PORT = 8808;
	
}
