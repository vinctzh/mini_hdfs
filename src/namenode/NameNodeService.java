package namenode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import net.sf.json.JSONObject;

import common.LocalFileDescription;
import common.MiniHDFSConstants;

public class NameNodeService {
	
	public final static int PORT_4_CLIENT = 1314;
	public int client_count;
	
	NameNode nameNode = new NameNode();
	
	public NameNodeService() {
		ServiceForClientThread serviceForClientThread = new ServiceForClientThread();
		serviceForClientThread.start();
	}
	
	public class ServiceForClientThread extends Thread {
		private ServerSocket serverSocket;
		
		public ServiceForClientThread(){
			try {
				serverSocket = new ServerSocket(PORT_4_CLIENT);
				client_count = 0;
				
				if (MiniHDFSConstants.doDebug)
					System.out.println("Server Socket created: serverSocket: " + serverSocket.toString());
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
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
					String recvMsg = new String(buffer, 0, len);
					if (MiniHDFSConstants.doDebug)
						System.out.println("Received msg: "+recvMsg);
					if (recvMsg.startsWith(MiniHDFSConstants.ADDFILE)) {
						LocalFileDescription locaFile = getLocalFileData(recvMsg);
						nameNode.addFile(locaFile);
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public LocalFileDescription getLocalFileData(String recvMsg) {
		String subStr = recvMsg.substring(3).trim();
		JSONObject json = JSONObject.fromObject(subStr);
		LocalFileDescription localFileDescription = new LocalFileDescription(json.getString("name"), json.getString("localpath"), json.getLong("length"),json.getInt("replication"));
		return localFileDescription;
	}
}
