package datanode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import javax.print.attribute.standard.Finishings;
import javax.xml.ws.handler.MessageContext.Scope;

import client.BlockTransfer;
import client.Client;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import common.FileHelper;
import common.MiniHDFSConstants;

public class DataNodeDameon {

	private DataNodeInfo dataNode;
	private String dataNodeJSONString;
	
	NameNodeConnectionThread nnComConnectionThread;
	
	private void doOperation(String opt, String obj) {
		if (opt.equals("rmblock")) {
			// 删除本地块
			String blkPath = dataNode.getStorageDir() + obj + ".meta";
			File file = new File(blkPath);
			if (file.isFile() && file.exists()) 
				file.delete();
		}
	}
	
	public DataNodeDameon(String localConfigFile) {
		
		nnComConnectionThread = new NameNodeConnectionThread();
		try {
			String config = FileHelper.loadFileIntoString(localConfigFile, "UTF-8");
			dataNodeJSONString = config;
			JSONObject json = JSONObject.fromObject(config);
			DataNodeInfo dataNode = new DataNodeInfo(json.getString("storageID"),
					json.getString("name"),
					json.getLong("capacity"),
					json.getLong("used"));	
			String dir = json.optString("storageDir");
			if (dir != null)
				dataNode.setStorageDir(dir);
			
			String storageDir = dataNode.getStorageDir();
			
			if (!validAndMkdirs(storageDir))
				System.err.println("Fail to initialize storage directory! Check and try again!");
			int blkPort = json.optInt("blkPort");
			if (blkPort != 0) 
				dataNode.setBlkPort(blkPort);
			
			this.dataNode = dataNode;
		} catch (IOException e) {
			System.err.println("Fail to initialize storage directory! Check and try again!");
			e.printStackTrace();
		}
	}
	public boolean validAndMkdirs(String storageDir) {
		File st_dir = new File(storageDir);
		if (!st_dir.exists())
			return st_dir.mkdirs();
		else 
			return true;
	}
	public void start() {
		//new NameNodeConnectionThread().start();
		nnComConnectionThread.start();
		new BlockOperatorThread().start();
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
				serverSocket = new ServerSocket(dataNode.getBlkPort());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		 
		public void run() {
			if (serverSocket != null) {
				while (true) {
					try {
						
						Socket socket = serverSocket.accept();
						new DoReceiveThread(socket).start();
						
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} else {
				System.err.println("-->BlockReceiverThread中serverSocket未初始化！");
			}
		}
	}
	
	public class DoReceiveThread extends Thread {
		Socket receiveSocket;
		JSONObject blkInfo;
		public DoReceiveThread(Socket socket) {
			this.receiveSocket = socket;
		}
		
		public JSONObject getLocatedTargets(String jsonStr) {
			return JSONObject.fromObject(jsonStr);
		}
		
		public void run() {
			
			InputStream inputStream;
			OutputStream outputStream;
			try {
				inputStream = receiveSocket.getInputStream();
				outputStream = receiveSocket.getOutputStream();
				outputStream.write("hello".getBytes());
				byte[] buffer = new byte[1024];
				int len;
				while (true) {
					len = inputStream.read(buffer);
					if (len <= 0) {
//						System.err.println("连接已经断开");
						inputStream.close();
						outputStream.close();
						receiveSocket.close();
						break;
					}
					
					String recvStr = new String(buffer, 0, len);
					if (MiniHDFSConstants.doDebug)
						System.out.println("get string from client  "+recvStr);
					if (recvStr.startsWith("putBlock")) {
						
						String subStr = recvStr.substring("putBlock".length()).trim();
						JSONObject blkInfo = getLocatedTargets(subStr);
						outputStream.write("blknow".getBytes()); // 通知Client发送block实际数据
						
						long blkId = blkInfo.optLong("blockId");
						
						String blkPath = dataNode.getStorageDir() + blkId + ".meta";
						FileOutputStream fos = new FileOutputStream(blkPath);
						int data;
						int count = 0;
						while ( -1 != (data =inputStream.read()))
						{
							fos.write( data );
							count++;
						}
						if (MiniHDFSConstants.doDebug)
							System.out.println("\nFile has been received successfully." + count);

						fos.close();
						outputStream.write("done".getBytes());
						JSONObject msg = new JSONObject();
						msg.put("operation", "addBlkReplica");
						JSONObject msgData = new JSONObject();
						msgData.put("blockId", blkId);
						msgData.put("storageId", dataNode.getStorageID());
						
						msg.put("data", msgData);
						
						nnComConnectionThread.addMsg(msg);
						
						int curIndex = blkInfo.getInt("curIndex");
						int replication = blkInfo.getInt("replication");
						// 这个节点不是最后一个节点
						if (curIndex < (replication-1)) {	// curIndex从0开始计数
							blkInfo.put("curIndex", curIndex+1);
							if (MiniHDFSConstants.doDebug) {
								System.out.println("向" + (curIndex+1) + "的数据节点发包");
								System.out.println(""+blkInfo.getInt("curIndex"));
							}
							BlockTransfer blkTransfer = new BlockTransfer(blkInfo, dataNode.getStorageDir(), "meta");
							blkTransfer.sendBlock();
						} else {
							if (MiniHDFSConstants.doDebug) 
								System.out.println("最后一个replication了：" + curIndex);
							// 最后一个节点，向Client 发送ack数据
							String client = blkInfo.getString("clientAddr");
							int ackPort = blkInfo.getInt("blkAckPort");
							int curBlkIndex = blkInfo.getInt("blkIndex");
							Socket socket = new Socket();
							socket.connect(new InetSocketAddress(client, ackPort));
							
							OutputStream oStream = socket.getOutputStream();
							JSONObject json = new JSONObject();
							json.put("ackBlockNum", curBlkIndex);
							oStream.write(json.toString().getBytes());
							
							oStream.flush();
							oStream.close();
							socket.close();
						}
						
					}
					
					if (recvStr.startsWith("pullBlock")) {
						String blockID = recvStr.substring("pullBlock".length()).trim();
						String blkPath = dataNode.getStorageDir() + blockID + ".meta";
						
						FileInputStream fins = new FileInputStream(blkPath);
						
						int data;
						int count = 0;
						while (-1 != (data = fins.read()))
						{
							count ++;
							outputStream.write(data);
						}
						System.out.println("==send done==" + count);
						fins.close();
						inputStream.close();
						outputStream.close();
						break;
					}
				}
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

		private List<JSONObject> msgBox = new ArrayList<JSONObject>();
		
		//其他线程，通过这个函数向msgBox中添加一个msg，在发送心跳包是，如果msgBox不是空，就从这里拿一个出来发送
		public void addMsg(JSONObject msg) {
			msgBox.add(msg);
		}
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
				len = inStream.read(buffer);
				if (new String(buffer,0,len).equals("OK"))
					System.out.println("Connection established!");
				else
					return;
				while (true) {
					try {
						sleep(5000);
						outStream.write("This is a heart beat information!".getBytes());
						len = inStream.read(buffer);
						String recv = new String(buffer, 0, len);
						if (recv.startsWith("optstodo")) {
							System.err.println("Namenode发来TODO任务" + recv);
							String optstodo = recv.substring("optstodo".length()).trim();
							JSONArray optsArray = JSONArray.fromObject(optstodo);
							if (!optsArray.isEmpty()) {
								for (int i=0; i< optsArray.size(); i++) {
									JSONObject opt = optsArray.getJSONObject(i);
									String operation = opt.getString("opt");
									String obj = opt.getString("object");
									doOperation(operation, obj);
								}
								outStream.write(("optsdone "+optsArray.toString()).getBytes());
								len = inStream.read(buffer);
								String rc = new String(buffer,0, len);
								if ("OK".equals(rc))
									continue;
							}
						}
						
						if ("OK".equals(recv))
							continue;
					} catch (InterruptedException e) {
						e.printStackTrace();
						System.out.println("exception caught while sleep");
					}
				}
				
			} catch (IOException e) {
				e.printStackTrace();
				
				System.out.println("NameNode没有启动，稍后再试>-<");
			}
			
		}
		
		Socket connect() throws IOException {
			Socket client = new Socket();
			client.connect(new InetSocketAddress(MiniHDFSConstants.SERVER,MiniHDFSConstants.SERVER_PORT4DATANODE));
			
			return client;
		}
	}
}
