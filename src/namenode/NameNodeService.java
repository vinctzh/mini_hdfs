package namenode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import common.LocalFileDescription;
import common.MiniHDFSConstants;
import datanode.DataNodeDescriptor;

public class NameNodeService {
	
	public int client_count;
	
	NameNode nameNode = new NameNode();
	
	public NameNodeService() {
		ServiceForClientThread serviceForClientThread = new ServiceForClientThread();
		serviceForClientThread.start();
		
		new ServiceForDataNode().start();
	}
	
	public class ServiceForDataNode extends Thread {
		private ServerSocket serverSocket;
		
		public ServiceForDataNode() {
			try {
				serverSocket = new ServerSocket(MiniHDFSConstants.SERVER_PORT4DATANODE);
				
				if (MiniHDFSConstants.doDebug) {
					System.out.println("Server Socket for DataNode is created: serverSocket: " + serverSocket.toString());
					System.out.println("Current active NameNode: "+NameNode.activeDataNodes.toString());
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public void run() {
			if (MiniHDFSConstants.doDebug) {
				System.out.println("Server for DataNode is now running ... ... ");
			}
			
			if (serverSocket != null) {
				while (true) {
					try {
						Socket socket = serverSocket.accept();
						
						new DNConnectionHandler(socket).start();
						if (MiniHDFSConstants.doDebug)
							System.out.println("A new connection with DataNode is established:" + socket.toString());
						
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				
				
			}
		}
	}
	
	public class DNConnectionHandler extends Thread {
		
		private Socket socket;
		private String dataNodeId;
	
		public DNConnectionHandler(Socket socket) {
			this.socket = socket;
		}
		
		public void run() {
			try {
				InputStream inStream = socket.getInputStream();	
				OutputStream outStream = socket.getOutputStream();
				
				String ip = new String(socket.getInetAddress().getAddress());
				int port = socket.getPort();
				outStream.write("Hello DataNode!".getBytes());
				
				byte[] buffer = new byte[1024];
				while (true) {
					// 连接之后读取客户端的传入
					int len = inStream.read(buffer);
					if (len <= 0) {
						System.err.println("Connection lost with Datanode : " + dataNodeId);
						nameNode.deleteNode(dataNodeId);
						nameNode.showActiveDataNodes();
						inStream.close();
						outStream.close();
						socket.close();
						break;
					}
					String recvMsg = new String(buffer, 0, len);
					if (recvMsg.startsWith("regist")) {
						String data = recvMsg.substring("regist".length()).trim();
						
						JSONObject dataNodeInfo = JSONObject.fromObject(data);
						dataNodeId = dataNodeInfo.getString("storageID");
						DataNodeDescriptor dnDescriptor = new DataNodeDescriptor(dataNodeInfo.getString("storageID"),
								dataNodeInfo.getString("name"),
								dataNodeInfo.getLong("capacity"), 
								dataNodeInfo.getLong("used"));
						dnDescriptor.setIpAddr(ip);
						dnDescriptor.setIpcport(port);
						
						nameNode.addDataNode(dnDescriptor);
						nameNode.showActiveDataNodes();
					}
					if (MiniHDFSConstants.doDebug)
						System.out.println("Received msg: "+recvMsg);
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
	}
	
	public class ServiceForClientThread extends Thread {
		private ServerSocket serverSocket;
		
		public ServiceForClientThread(){
			try {
				serverSocket = new ServerSocket(MiniHDFSConstants.SERVER_PORT4CLIENT);
				client_count = 0;
				
				if (MiniHDFSConstants.doDebug)
					System.out.println("Server Socket created: serverSocket: " + serverSocket.toString());
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		public void run() {
			client_count = 0;
			if (MiniHDFSConstants.doDebug) {
				System.out.println("Server is now running ... ... ");
			}
			
			if (serverSocket != null) {
				try {
					
					while (true) {
						if (MiniHDFSConstants.doDebug)
							System.out.println("Waiting for connecting ... ...");
						
						Socket newSocket = serverSocket.accept();
						
						if (MiniHDFSConstants.doDebug)
							System.out.println("Connection established with socket:" + newSocket.toString());
						
						new ClientConnection(newSocket).start();
						client_count++;
						System.out.println("A new client is connected!");
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		}
	}
	
	public class ClientConnection extends Thread {
		private Socket socket;
		
		public ClientConnection(Socket socket) {
			this.socket = socket;
		}

		public void run() {
			
			try {
				InputStream inStream = socket.getInputStream();
				OutputStream outStream = socket.getOutputStream();
				outStream.write("Welcome !".getBytes());
				byte[] buffer = new byte[1024];
				while (true) {
					int len = inStream.read(buffer);
					if (len <= 0) {
						System.out.println("-->和client的连接已经断开！！");
						outStream.close();
						inStream.close();
						socket.close();
						return ;
					}
					String  recvMsg = new String(buffer, 0, len);
					if (MiniHDFSConstants.doDebug)
						System.out.println("Received msg: "+recvMsg);
					if (recvMsg.startsWith(MiniHDFSConstants.ADDFILE)) {
						LocalFileDescription locaFile = getLocalFileData(recvMsg.substring(3).trim());
						BlockInfo[] blocks = nameNode.addFile(locaFile);
						JSONObject jsonInfo = blkInfoToJsonData(blocks, locaFile.getReplication());
						outStream.write(jsonInfo.toString().getBytes());
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public JSONObject blkInfoToJsonData(BlockInfo[] blocks, int replication) {
		int blkNums = blocks.length;
		
		JSONObject blocksJson = new JSONObject();
		blocksJson.put("filename", blocks[0].getFile().getLocalNameString());
		blocksJson.put("blockNum", blkNums);
		blocksJson.put("replication", replication);
		
		JSONArray blocksArray = new JSONArray();
		
		for (int i=0; i< blkNums; i++) {
			JSONObject curBlock = new JSONObject();
			curBlock.put("blockId", blocks[i].getBlockId());
			curBlock.put("replication", replication);
			curBlock.put("blockSize", blocks[i].getNumBytes());
			curBlock.put("curIndex", 0);
			
//			JSONArray targets = blkInfo.getJSONArray("target");
			JSONArray targets = new JSONArray();
			for (int j=0; j<replication; j++) {
				JSONObject target = new JSONObject();
				target.put("storageId", blocks[i].getDataNode(i).getStorageID());
				target.put("ipaddr", blocks[i].getDataNode(i).getIpAddr());
				target.put("blkPort", blocks[i].getDataNode(i).getBlkPort());
				targets.add(target);
			}
			curBlock.put("targets", targets);
			blocksArray.add(curBlock);
		}
		blocksJson.put("blocks", blocksArray);
		
		return blocksJson;
		
	}
	public LocalFileDescription getLocalFileData(String recvMsg) {
		JSONObject json = JSONObject.fromObject(recvMsg);
		LocalFileDescription localFileDescription = new LocalFileDescription(json.getString("name"), json.getString("localpath"), json.getLong("length"),json.getInt("replication"));
		return localFileDescription;
	}
}
