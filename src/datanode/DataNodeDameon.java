package datanode;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import common.FileHelper;
import common.MiniHDFSConstants;

public class DataNodeDameon {

	private DataNodeInfo dataNode;
	private String dataNodeJSONString;
	
	public DataNodeDameon(String localConfigFile) {
		try {
			String config = FileHelper.loadFileIntoString(localConfigFile, "UTF-8");
			dataNodeJSONString = config;
			JSONObject json = JSONObject.fromObject(config);
			DataNodeInfo dataNode = new DataNodeInfo(json.getString("storageID"),json.getString("name"),
					json.getLong("capacity"),json.getLong("used"));	
			this.dataNode = dataNode;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void start() {
		new NameNodeConnectionThread().start();
	}
	
	/**
	 * 这个线程的目的是用来接受其他DataNode发送过来的Block数据
	 * @author jianyuan
	 *
	 */
	public class BlockOperatorThread extends Thread {
		
		ServerSocket serverSocket;
		public BlockOperatorThread() {
			try {
				serverSocket = new ServerSocket(MiniHDFSConstants.DN_BLK_RECEIVER_PORT);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public JSONObject getLocatedTargets(String jsonStr) {
			return JSONObject.fromObject(jsonStr);
		}
		 
		public void run() {
			if (serverSocket != null) {
				try {
					Socket socket = serverSocket.accept();
					byte[] buffer = new byte[1024];
					InputStream inStream = socket.getInputStream();
					
					int len = inStream.read(buffer);
					
					if (len > 0 ) {
						System.out.println("Datanode recevied msg: "+new String(buffer, 0,len ) + " from NameNode.");
						String recv = new String(buffer,0, len);
						
						if (recv.startsWith("putBlock")) {
							String subStr = recv.substring("putBlock".length()).trim();
							
							JSONObject blkInfo = getLocatedTargets(subStr);
							new DoReceiveThread(socket, blkInfo).start();
						}
					}
					
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				System.err.println("-->BlockReceiverThread中serverSocket未初始化！");
			}
		}
	}
	
	public class DoReceiveThread extends Thread {
		Socket receiveSocket;
		JSONObject blkInfo;
		public DoReceiveThread(Socket socket, JSONObject blkInfo) {
			this.receiveSocket = socket;
			this.blkInfo = blkInfo;
		}
		
		public void run() {
			long blkId = blkInfo.optLong("blockId");
			int replication = blkInfo.optInt("replication");
			int curIndex = blkInfo.optInt("curIndex");
			
			JSONArray targets = blkInfo.getJSONArray("target");
			JSONObject curStorage =  targets.getJSONObject(curIndex);
			
			InputStream inputStream;
			try {
				inputStream = receiveSocket.getInputStream();
				//TODO: 检测该block是否已经存在于该DataNode上，以及存在时的操作
				// 不存在，创建并保存
				String blkFileName = "BLK_"+blkId + ".blk";
				FileOutputStream fos = new FileOutputStream(blkFileName);
				int data;
				while ( -1 != (data =inputStream.read()))
				{
					fos.write( data );
				}
				fos.close();
				System.out.println("\nFile has been recerved successfully.");
				inputStream.close();
				
				
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
		}
	}
	/**
	 * 线程用来和NameNode进行通讯的线程
	 * @author jianyuan
	 *
	 */
	public class NameNodeConnectionThread extends Thread {

		public void run() {
			byte buffer[] = new byte[1024];
			
			try {
				Socket socket = connect();
				InputStream inStream = socket.getInputStream();
				OutputStream outStream = socket.getOutputStream();
			
				// 第一次建立连接，首先接收到Server的确认消息;
				// 然后将本地的存储信息发送给NameNode
				int len = inStream.read(buffer);
				System.out.println("Datanode recevied msg: "+new String(buffer, 0,len ) + " from NameNode.");
				outStream.write(("regist " + dataNodeJSONString).getBytes());
				
				while (true) {
					try {
						System.out.println("Heartbeats");
						sleep(5000);
						outStream.write("This is a heart beat information!".getBytes());
					} catch (InterruptedException e) {
						e.printStackTrace();
						System.out.println("exception caught while sleep");
					}
					
				}
				
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		
		Socket connect() throws IOException {
			Socket client = new Socket();
			client.connect(new InetSocketAddress(MiniHDFSConstants.SERVER,MiniHDFSConstants.SERVER_PORT4DATANODE));
			
			return client;
		}
	}
}
