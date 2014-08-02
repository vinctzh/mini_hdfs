package client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import common.FileHelper;
import common.LocalFileDescription;
import common.LocalFileSeperator;
import common.MiniHDFSConstants;

public class ClientOperations {
	
	public JSONObject listFile() {
		
		JSONObject storedFiles = new JSONObject();
		try {
			Socket client = new Socket(); 
			client.connect(new InetSocketAddress(MiniHDFSConstants.SERVER, MiniHDFSConstants.SERVER_PORT4CLIENT));
			OutputStream outStream = client.getOutputStream();
			InputStream inputStream = client.getInputStream();
			
			
			int len;
			while (true) {
				byte buffer[] = new byte[1024];
				len = inputStream.read(buffer);
				System.out.println(new String(buffer,0,len));

				if (len <= 0) {
					inputStream.close();
					outStream.close();
					client.close();
					System.out.println("结束");
					break;
				} else  {
					String recv = new String(buffer, 0, len);
					if (recv.equals("Welcome !")) {
						outStream.write(MiniHDFSConstants.LSFILES.getBytes());	
					}
					// 返回的文件信息
					if (recv.startsWith("StoredFiles")) {
						String storedFilesJSONString = recv.substring("StoredFiles".length()).trim();
						storedFiles = JSONObject.fromObject(storedFilesJSONString);
						outStream.write("received".getBytes());
					}
					
					if (recv.equals("done")) {
						System.out.println("结束");
						outStream.close();
						inputStream.close();
						client.close();
						break;
					}
				}
			
			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		showFilesOnHDFS(storedFiles);
		return storedFiles;
	}

	private void showFilesOnHDFS(JSONObject storedFiles) {
		JSONObject files = storedFiles.getJSONObject("files");
//		filesJson.put("files", filesArray)
		
		System.out.println("创建完成的文件信息：");
		if (files.isEmpty()) {
			System.out.println("->没有创建完成的文件！！");
		} else {
			JSONArray filesArray = files.getJSONArray("files");
			for (int i=0; i < filesArray.size(); i++) {
				JSONObject file = filesArray.getJSONObject(i);
				String fileName = file.getString("filename");
				long fSize = file.getLong("fileSize");
				String fileSize = String.valueOf(fSize);
				System.out.printf("->%50s\t%16s\n", fileName,fileSize );
			}
		}
		System.out.println("------------------------------------");
		System.out.println("正在创建的文件信息：");
		JSONObject filesUC = storedFiles.getJSONObject("filesUC");
		
		
		if (filesUC.isEmpty()) {
			System.out.println("->没有正在创建的文件！！");
		} else {
			JSONArray filesUCArray = filesUC.getJSONArray("filesUC");
			for (int i=0; i < filesUCArray.size(); i++) {
				JSONObject file = filesUCArray.getJSONObject(i);
				String fileName = file.getString("filename");
				long fSize = file.getLong("fileSize");
				String fileSize = String.valueOf(fSize);
				System.out.printf("->%20s\t%16s", fileName,fileSize );
			}
		}
	}
	
	public void copyFile(String filename) {
		JSONObject locatedFiles = new JSONObject();
		try {
			Socket client = new Socket(); 
			client.connect(new InetSocketAddress(MiniHDFSConstants.SERVER, MiniHDFSConstants.SERVER_PORT4CLIENT));
			OutputStream outStream = client.getOutputStream();
			InputStream inputStream = client.getInputStream();
			
			
			int len;
			while (true) {
				byte buffer[] = new byte[1024];
				len = inputStream.read(buffer);
				System.out.println(new String(buffer,0,len));

				if (len <= 0) {
					inputStream.close();
					outStream.close();
					client.close();
					System.out.println("结束");
					break;
				} else  {
					String recv = new String(buffer, 0, len);
					if (recv.equals("Welcome !")) {
						outStream.write((MiniHDFSConstants.COPYFILE + " " + filename).getBytes());	
					}
					// 返回的文件信息
					if (recv.startsWith("StoredFiles")) {
						String storedFilesJSONString = recv.substring("StoredFiles".length()).trim();
						locatedFiles = JSONObject.fromObject(storedFilesJSONString);
						outStream.write("received".getBytes());
					}
					
					if (recv.equals("done")) {
						System.out.println("结束");
						outStream.close();
						inputStream.close();
						client.close();
						break;
					}
				}
			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void addFile(String localFilePath) {
		new AddNewFile(localFilePath).start();
		return;
	}
	
	public void commitFile(final String fileName) {
		new CommitFileThread(fileName).start();
	}
	
	public void sendBlocks(JSONObject blksInfo, int blkIndex) {
		System.out.println("->>发送第"+blkIndex+"个数据块");		
		int blkNums = blksInfo.getInt("blockNum");
		JSONArray blocks = blksInfo.getJSONArray("blocks");
		
		if (blkIndex >= blkNums)
			return;
		
		JSONObject block = blocks.getJSONObject(blkIndex);
	
		block.put("clientAddr", blksInfo.getString("clientAddr"));
		block.put("blkAckPort", blksInfo.getInt("blkAckPort"));
		block.put("blkIndex", blksInfo.getInt("blkIndex"));
		
//		String filepath = json.getString("blkInfosFilePath");
//		int ackBlkNum = json.getInt("ackBlockNum");
		BlockTransfer blockTransfer = new BlockTransfer(block);
		blockTransfer.sendBlock();
	}
	
	public class DistributeFileThread extends Thread {
		String blkInfosFilePath;
		int sentBlockNum;
		String localHost;
		
		public DistributeFileThread(String blkInfosFilePath) {
			this.blkInfosFilePath = blkInfosFilePath;
			this.sentBlockNum = 0;
			try {
				localHost = InetAddress.getLocalHost().getHostAddress();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public DistributeFileThread(String blkInfosFilePath, int sentBlockNum) {
			this.blkInfosFilePath = blkInfosFilePath;
			this.sentBlockNum = sentBlockNum;
			try {
				localHost = InetAddress.getLocalHost().getHostAddress();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public void setSentBlockNum(int sentBlockNum){
			System.out.println("set sentblockNum from" + this.sentBlockNum +" to " + sentBlockNum);
			this.sentBlockNum = sentBlockNum;
		}
		
		public void distributeFile() {
			System.out.println("sentbloknum  " + sentBlockNum);
			distributeFile(sentBlockNum);
		}
		public void distributeFile(int sentBlockNum) {
			
			String blksinfoStr;
			try {
				blksinfoStr = FileHelper.loadFileIntoString(blkInfosFilePath, "UTF-8");
				JSONObject blksinfo = JSONObject.fromObject(blksinfoStr);
				
				int blkNums = blksinfo.getInt("blockNum");
				
				
				if (sentBlockNum >= blkNums) {
					// 清空本地缓存
					System.out.println("所有包都发送完成，现在清空本地缓存！");
					commitFile(blksinfo.getString("filename"));
					File blkInfosLocal = new File(blkInfosFilePath);
					if (blkInfosLocal.isFile() && blkInfosLocal.exists())
						blkInfosLocal.delete();
					JSONArray blocks = blksinfo.getJSONArray("blocks");
					for (int i=0;i<blkNums;i++) {
						String blkCachePath = Client.CLIENT_CACHE + blocks.getJSONObject(i).getString("blockId") + ".cache";
						File tmp = new File(blkCachePath);
						if (tmp.isFile() && tmp.exists())
							tmp.delete();
					}
					System.out.println("清空本地缓存完成！");
					
					return;
				}
					
				blksinfo.put("clientAddr", localHost);
				blksinfo.put("blkAckPort", MiniHDFSConstants.CLIENT_BLK_ACK_PORT);
				blksinfo.put("blkIndex", sentBlockNum);
				
				System.out.println("located Blockinfo " + blksinfo.toString());
				sendBlocks(blksinfo, sentBlockNum);
				
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
	}
	
	public class BlockACKThread extends Thread {
		ServerSocket serverSocket;
		DistributeFileThread distributeFileThread;
		public BlockACKThread(DistributeFileThread distributeFileThread) {
			System.out.println("testas");
			this.distributeFileThread = distributeFileThread;
			try {
				serverSocket = new ServerSocket(MiniHDFSConstants.CLIENT_BLK_ACK_PORT);
				if (MiniHDFSConstants.doDebug) {
					System.out.println("Serversocket for block ack: serverSocket: " + serverSocket.toString());
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public void run() {
			
			if (serverSocket != null) {
				byte buffer[] = new byte[1024];
				int len;
				while (true) {
					try {
						Socket socket = serverSocket.accept();
						InputStream inputStream = socket.getInputStream();
						len = inputStream.read(buffer);
						if (len <= 0) {
							continue;
						} else {
							String recv = new String(buffer);
							System.out.println("-->ack message received: " + recv);
							JSONObject json = JSONObject.fromObject(recv);
							//String filepath = json.getString("blkInfosFilePath");
							
							int ackBlkNum = json.getInt("ackBlockNum");
							distributeFileThread.setSentBlockNum(ackBlkNum+1);
							distributeFileThread.distributeFile();
							inputStream.close();
							socket.close();
						}
						
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	
	
	public class AddNewFile extends Thread {
		
		private String localFilePath;
		public AddNewFile(String filePath) {
			this.localFilePath = Client.CLIENT_ROOT + filePath;
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
				// TODO 这里存入了replication数量
				LocalFileDescription localFile = new LocalFileDescription(file,2);
				System.out.println("localFile: "+localFile.toString());
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

						if (recv.equals("done")) {
							System.out.println("断开连接");
							inStream.close();
							socket.close();
							break;
						} else if (recv.startsWith("locatedBlks")){
							System.out.println(recv);
							String recvData = recv.substring("locatedBlks".length()).trim();
							JSONObject blksInfo = JSONObject.fromObject(recvData);
							int blkNums = blksInfo.getInt("blockNum");
							// 文件创建失败
							if (blkNums <1) {
								System.err.println("-->文件创建失败！！");
								break;
							} else {
								// 将文件分成多个分块
								if ( LocalFileSeperator.seperateFile(localFilePath, blksInfo) ) {
									FileHelper.saveStringIntoFile(Client.CLIENT_CACHE + localFile.getName() + ".blksinfo", recvData);
								} else {
									System.err.println("文件分块失败！");
								}
								outStream.write("received".getBytes());
								DistributeFileThread distributeFileThread = new DistributeFileThread(Client.CLIENT_CACHE + localFile.getName() + ".blksinfo");
								distributeFileThread.distributeFile();
								new BlockACKThread(distributeFileThread).start();
							}
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
	public class CommitFileThread extends Thread {
		String fileName;
		public CommitFileThread(String fileName) {
			this.fileName = fileName;
		}
		public void run() {
			// TODO Auto-generated method stub
			try {
				Socket socket = connect();
				OutputStream outStream = socket.getOutputStream();
				InputStream inputStream = socket.getInputStream();
				byte buffer[] = new byte[1024];
				int len;
				while (true) {
					len = inputStream.read(buffer);
					if (len <= 0) {
						System.out.println("结束");
						break;
					} else  {
						String recv = new String(buffer, 0, len);
						if (recv.equals("Welcome !")) {
							outStream.write(("commitFile "+ fileName).getBytes());	
						}
						if (recv.equals("done")) {
							System.out.println("结束");
							outStream.close();
							inputStream.close();
							socket.close();
							break;
						}
					}
				
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		Socket connect() throws IOException {
			Socket client = new Socket(); 
			client.connect(new InetSocketAddress(MiniHDFSConstants.SERVER, MiniHDFSConstants.SERVER_PORT4CLIENT));
			return client;
		}
		
	};
	
}
