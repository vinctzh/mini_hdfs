package client;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ObjectOutputStream.PutField;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import net.sf.json.JSONObject;
import common.LocalFileDescription;
import common.MiniHDFSConstants;

public class ClientOperations {
	
	public void addFile(String localFilePath) {
		new AddNewFile(localFilePath).start();
	}
	
	public class AddNewFile extends Thread {
		
		private String localFilePath;
		public AddNewFile(String filePath) {
			this.localFilePath = filePath;
		}

		public void run() {
			byte buffer[] = new byte[1024];
			try {
				Socket socket = connect();
				InputStream inStream = socket.getInputStream();
				OutputStream outStream = socket.getOutputStream();
				
				int len = inStream.read(buffer);
				System.out.println(new String(buffer, 0,len ));
				File file = new File(localFilePath);
				LocalFileDescription localFile = new LocalFileDescription(file,2);
				JSONObject locaFileJSON = new JSONObject();
				locaFileJSON.put("name", localFile.getName());
				locaFileJSON.put("replication",localFile.getReplication());
				locaFileJSON.put("localpath",localFile.getLocalPath());
				locaFileJSON.put("length", localFile.getLength());
				String jsonStr = locaFileJSON.toString();
				
				outStream.write((MiniHDFSConstants.ADDFILE + " "+ jsonStr).getBytes());
				
				// 等待NameNode分块完成，并指定好各个分块的存储器
				while (true) {
					len = inStream.read(buffer);
					if (len >0) {
						String recv = new String(buffer,0,len);
						System.out.println(recv);
						
						if (recv.equals("done")) {
							System.out.println("断开连接");
							inStream.close();
							socket.close();
						}
					}
				}
				
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
			
		}
		
		Socket connect() throws IOException {
			Socket client = new Socket(); 
			client.connect(new InetSocketAddress(MiniHDFSConstants.SERVER, MiniHDFSConstants.SERVER_PORT4CLIENT));
			return client;
		}
		
		
	}
}
