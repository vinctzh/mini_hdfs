package datanode;

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
			int blkPort = json.optInt("blkPort");
			if (blkPort != 0) 
				dataNode.setBlkPort(blkPort);
			
			System.err.println("test:" + json.optInt("dafd") + " " + json.optString("das"));
			this.dataNode = dataNode;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void start() {
		//new NameNodeConnectionThread().start();
		nnComConnectionThread.start();
		new BlockOperatorThread().start();
	}
	
	/**
	 * ����̵߳�Ŀ����������������DataNode���͹�����Block����
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
				System.err.println("-->BlockReceiverThread��serverSocketδ��ʼ����");
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
						System.err.println("�����Ѿ��Ͽ�");
						inputStream.close();
						outputStream.close();
						receiveSocket.close();
						break;
					}
					
					String recvStr = new String(buffer, 0, len);
					System.out.println("get string from client  "+recvStr);
					if (recvStr.startsWith("putBlock")) {
						
						String subStr = recvStr.substring("putBlock".length()).trim();
						JSONObject blkInfo = getLocatedTargets(subStr);
						outputStream.write("blknow".getBytes()); // ֪ͨClient����blockʵ������
						
						long blkId = blkInfo.optLong("blockId");
						
						String blkPath = dataNode.getStorageDir() + blkId + ".meta";
						FileOutputStream fos = new FileOutputStream(blkPath);
						int data;
						while ( -1 != (data =inputStream.read()))
						{
							fos.write( data );
						}
						System.out.println("\nFile has been recerved successfully.");

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
						// ����ڵ㲻�����һ���ڵ�
						if (curIndex < (replication-1)) {	// curIndex��0��ʼ����
							blkInfo.put("curIndex", curIndex++);
							System.out.println("��" + (curIndex+1) + "�����ݽڵ㷢��");
							BlockTransfer blkTransfer = new BlockTransfer(blkInfo);
							blkTransfer.sendBlock();
						}
						System.out.println("���һ��replication�ˣ�" + curIndex);
						// ���һ���ڵ㣬��ǰһ����ack����
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
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			

		}
	}
	/**
	 * �߳�������NameNode����ͨѶ���߳�
	 * @author jianyuan
	 *
	 */
	public class NameNodeConnectionThread extends Thread {

		private List<JSONObject> msgBox = new ArrayList<JSONObject>();
		
		//�����̣߳�ͨ�����������msgBox�����һ��msg���ڷ����������ǣ����msgBox���ǿգ��ʹ�������һ����������
		public void addMsg(JSONObject msg) {
			msgBox.add(msg);
		}
		public void run() {
			byte buffer[] = new byte[1024];
			
			try {
				Socket socket = connect();
				InputStream inStream = socket.getInputStream();
				OutputStream outStream = socket.getOutputStream();
			
				// ��һ�ν������ӣ����Ƚ��յ�Server��ȷ����Ϣ;
				// Ȼ�󽫱��صĴ洢��Ϣ���͸�NameNode
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
