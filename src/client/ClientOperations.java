package client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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
import common.LocalBlocksCombine;
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
					System.out.println("����");
					break;
				} else  {
					String recv = new String(buffer, 0, len);
					if (recv.equals("Welcome !")) {
						outStream.write(MiniHDFSConstants.LSFILES.getBytes());	
					}
					// ���ص��ļ���Ϣ
					if (recv.startsWith("StoredFiles")) {
						String storedFilesJSONString = recv.substring("StoredFiles".length()).trim();
						storedFiles = JSONObject.fromObject(storedFilesJSONString);
						outStream.write("received".getBytes());
					}
					
					if (recv.equals("done")) {
						System.out.println("����");
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
		
		System.out.println("������ɵ��ļ���Ϣ��");
		if (files.isEmpty()) {
			System.out.println("->û�д�����ɵ��ļ�����");
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
		System.out.println("���ڴ������ļ���Ϣ��");
		JSONObject filesUC = storedFiles.getJSONObject("filesUC");
		
		
		if (filesUC.isEmpty()) {
			System.out.println("->û�����ڴ������ļ�����");
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
	
	private boolean canCopy(JSONObject locatedFiles) {
		if (locatedFiles == null) 
			return false;
		
		JSONArray blocks = locatedFiles.getJSONArray("blocks");
		if (blocks.size() < 1) 
			return false;
		
		for (int i=0;i<blocks.size();i++) {
			JSONObject block = blocks.getJSONObject(i);
			JSONArray activeTargets = block.getJSONArray("activeTargets");
			if (activeTargets.isEmpty())
				return false;
		}
		
		return true;
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
					System.out.println("����");
					break;
				} else  {
					String recv = new String(buffer, 0, len);
					if (recv.equals("Welcome !")) {
						outStream.write((MiniHDFSConstants.COPYFILE + " " + filename).getBytes());	
					}
					// ���ص��ļ���Ϣ
					if (recv.startsWith("locatedFiles")) {
						String storedFilesJSONString = recv.substring("locatedFiles".length()).trim();
						locatedFiles = JSONObject.fromObject(storedFilesJSONString);
						System.out.println(storedFilesJSONString);
						outStream.write("received".getBytes());
					}
					
					if (recv.equals("done")) {
						System.out.println("����");
						outStream.close();
						inputStream.close();
						client.close();
						break;
					}
				}
			}
			if (canCopy(locatedFiles)) {
				PullBlocksFromDataNodes pullBlocksFromDataNodes = new PullBlocksFromDataNodes(locatedFiles);
				pullBlocksFromDataNodes.pullBlock();
			} else {
				System.err.println("��HDFS�����ļ�"+filename+"ʧ�ܣ�");
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public boolean removeFile(String filename) {
		Socket client = new Socket(); 
		boolean result =false;
		try {
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
					System.out.println("����");
					break;
				} else  {
					String recv = new String(buffer, 0, len);
					if (recv.equals("Welcome !")) {
						outStream.write((MiniHDFSConstants.RMFILE + " " + filename).getBytes());	
					}
					// ���ص��ļ���Ϣ
					if (recv.startsWith("removeResult")) {
						String msg = recv.substring("removeResult".length()).trim();
						System.err.println("ɾ���ļ�"+filename+":" + msg);
						outStream.write("received".getBytes());
						if (msg.startsWith("OK")) {
							result = true;
						}
							
					}
					
					if (recv.equals("done")) {
						System.out.println("����");
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
			return false;
		}
		return result;
	}
	public void addFile(String localFilePath) {
		new AddNewFile(localFilePath).start();
		return;
	}
	
	public void commitFile(final String fileName) {
		new CommitFileThread(fileName).start();
	}
	
	public void sendBlocks(JSONObject blksInfo, int blkIndex) {
		System.out.println("->>���͵�"+blkIndex+"�����ݿ�");		
		int blkNums = blksInfo.getInt("blockNum");
		JSONArray blocks = blksInfo.getJSONArray("blocks");
		
		if (blkIndex >= blkNums)
			return;
		
		JSONObject block = blocks.getJSONObject(blkIndex);
	
		block.put("clientAddr", blksInfo.getString("clientAddr"));
		block.put("blkAckPort", blksInfo.getInt("blkAckPort"));
		block.put("blkIndex", blksInfo.getInt("blkIndex"));
		
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
					// ��ձ��ػ���
					System.out.println("���а���������ɣ�������ձ��ػ��棡");
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
					System.out.println("��ձ��ػ�����ɣ�");
					
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
				// TODO ���������replication����
				LocalFileDescription localFile = new LocalFileDescription(file,2);
				System.out.println("localFile: "+localFile.toString());
				JSONObject locaFileJSON = new JSONObject();
				locaFileJSON.put("name", localFile.getName());
				locaFileJSON.put("replication",localFile.getReplication());
				locaFileJSON.put("localpath",localFile.getLocalPath());
				locaFileJSON.put("length", localFile.getLength());
				String jsonStr = locaFileJSON.toString();
				
				outStream.write((MiniHDFSConstants.ADDFILE + " "+ jsonStr).getBytes());
				
				// �ȴ�NameNode�ֿ���ɣ���ָ���ø����ֿ�Ĵ洢��
				while (true) {
					len = inStream.read(buffer);
					if (len >0) {
						String recv = new String(buffer,0,len);

						if (recv.equals("done")) {
							System.out.println("�Ͽ�����");
							inStream.close();
							socket.close();
							break;
						} else if (recv.startsWith("locatedBlks")){
							System.out.println(recv);
							String recvData = recv.substring("locatedBlks".length()).trim();
							JSONObject blksInfo = JSONObject.fromObject(recvData);
							int blkNums = blksInfo.getInt("blockNum");
							// �ļ�����ʧ��
							if (blkNums <1) {
								System.err.println("-->�ļ�����ʧ�ܣ���");
								break;
							} else {
								// ���ļ��ֳɶ���ֿ�
								if ( LocalFileSeperator.seperateFile(localFilePath, blksInfo) ) {
									FileHelper.saveStringIntoFile(Client.CLIENT_CACHE + localFile.getName() + ".blksinfo", recvData);
								} else {
									System.err.println("�ļ��ֿ�ʧ�ܣ�");
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
						System.out.println("����");
						break;
					} else  {
						String recv = new String(buffer, 0, len);
						if (recv.equals("Welcome !")) {
							outStream.write(("commitFile "+ fileName).getBytes());	
						}
						if (recv.equals("done")) {
							System.out.println("����");
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
	
	public class PullBlocksFromDataNodes extends Thread {
		JSONObject locatedBlocks;
		int pulledBlockNum;
		String localHost ;
		
		public PullBlocksFromDataNodes(JSONObject locatedBlocks) {
			this.locatedBlocks = locatedBlocks;
			this.pulledBlockNum = 0;
			
			try {
				localHost = InetAddress.getLocalHost().getHostAddress();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public PullBlocksFromDataNodes(JSONObject locatedBlocks, int pulledBlockNum) {
			this.locatedBlocks = locatedBlocks;
			this.pulledBlockNum = pulledBlockNum;
			
			try {
				localHost = InetAddress.getLocalHost().getHostAddress();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		public int getPulledBlockNum() {
			return pulledBlockNum;
		}

		public void setPulledBlockNum(int pulledBlockNum) {
			this.pulledBlockNum = pulledBlockNum;
		}
		
		public void pullBlock() {
			pullBlock(pulledBlockNum);
		}
		
		public void pullBlock(int pulledNum) {
			int blkNums = locatedBlocks.getInt("blockNum");
			String filename = locatedBlocks.getString("filename");
			if (pulledBlockNum >= blkNums) {
				System.out.println("pulledBlockNum:"+pulledBlockNum);
				System.out.println("����block�����浽�����ˣ�׼���ϲ�");
			} else {
				JSONArray blocks = locatedBlocks.getJSONArray("blocks");
				int curPulledNum = pulledNum;
				for (int i=curPulledNum; i <blkNums; i++ ) {
					JSONObject curBlock = blocks.getJSONObject(i);
					long blockId = curBlock.getLong("blockId");
					JSONArray activeTargets = curBlock.getJSONArray("activeTargets");
					for (int j=0; j< activeTargets.size(); j++) {
						JSONObject target = activeTargets.getJSONObject(j);
						String ipaddr = target.getString("ipaddr");
						int port = target.getInt("blkPort");
						Socket client = new Socket();
						try {
							client.connect(new InetSocketAddress(ipaddr,port));
							OutputStream outStream = client.getOutputStream();
							InputStream inputStream = client.getInputStream();
							outStream.write(("pullBlock" + " " + curBlock.getLong("blockId")).getBytes());	

							String blkPath = Client.CLIENT_CACHE + blockId + ".cache";
							FileOutputStream fos = new FileOutputStream(blkPath);
							int data;
							int count = 0;
							while ( -1 != (data =inputStream.read()))
							{
								fos.write( data );
								count++;
							}
							System.out.println("\nFile has been received successfully." + count);
							fos.close();
							outStream.write("done".getBytes());
							inputStream.close();
							outStream.close();
							client.close();
							System.out.println("pulledBlockNum:"+pulledNum);
							pulledNum ++;
							System.out.println("pulledBlockNum:"+pulledNum);
							break;
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				
				//ȫ��block��pull��cache���ˣ�ִ�кϲ�
				if (pulledNum == blkNums) {
					System.out.println("����block��pull�������ˣ�׼���ϲ�");
					
					if (LocalBlocksCombine.combineBlocks(filename, null, blocks, null)) {
						System.out.println("����block�ϲ����");
					}
					
					for (int i=0;i<blkNums;i++) {
						String blkCachePath = Client.CLIENT_CACHE + blocks.getJSONObject(i).getString("blockId") + ".cache";
						File tmp = new File(blkCachePath);
						if (tmp.isFile() && tmp.exists())
							tmp.delete();
					}
				}
			}
		}
	}
}
