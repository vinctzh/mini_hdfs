package client;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class BlockTransfer {
	JSONObject locatedBlock;
	String directory;
	String format;
	

	public BlockTransfer(JSONObject locatedBlock) {
		System.out.println(locatedBlock.toString());
		this.locatedBlock = locatedBlock;
	}
	
	public BlockTransfer(JSONObject locatedBlock, String directory, String format) {
		System.out.println(locatedBlock.toString());
		this.locatedBlock = locatedBlock;
		this.directory = directory;
		this.format = format;
	}
	
	public void sendBlock() {
		if (locatedBlock == null)
			return ;
		if (validLocatedBlock(locatedBlock)) {
			System.out.println("开始发送块线程");
			new SendBlockThread(locatedBlock).start();
		}
	}
	
	private boolean validLocatedBlock(JSONObject locatedBlock) {
		
		JSONArray targets = locatedBlock.getJSONArray("targets");
		int replication = locatedBlock.getInt("replication");
		if (replication < 1) {
			return false;
		}
		if (replication != targets.size())
			return false;
		// TODO: 其他可能的情况
		return true;
	}
	
	public class SendBlockThread extends Thread {
		String blkID ;
		JSONArray targets;
		JSONObject targetDN;
		
		String client;
		int blkACKPort;
		
		public SendBlockThread(JSONObject locatedBlock) {
			blkID = locatedBlock.getString("blockId");
			targets = locatedBlock.getJSONArray("targets");
			int curIndex = locatedBlock.getInt("curIndex");
			System.err.println("当前包"+blkID+"第"+curIndex+"个副本");
			targetDN = targets.getJSONObject(curIndex);
			
			client = locatedBlock.getString("clientAddr");
			blkACKPort = locatedBlock.getInt("blkAckPort");
		}
		
		public void run() {
			System.out.println("SendBlockThread 开始run " );
			byte buffer[] = new byte[1024];
			System.err.println("target datanode: " + targetDN.toString()); 
			Socket socket = connect(targetDN);
			try {
				InputStream inStream = socket.getInputStream();
				OutputStream outStream = socket.getOutputStream();
				System.out.println("SendBlockThread： " + socket.toString());
				
				int len;
				while (true) {
					len = inStream.read(buffer);
					if (len > 0) {
						String recv = new String(buffer,0,len);
						System.out.println("接收到DataNode的消息： "+recv);
						// 连接已经建立
						// 发送包和datanode的json数据
						if (recv.equals("hello")) {
							outStream.write(("putBlock" + locatedBlock.toString()).getBytes());
						}
						// blkinfo已经接受到，开始传实际的blk数据
						if (recv.equals("blknow")) {
//							String blkPath = Client.CLIENT_CACHE + blkID + ".cache";
							
							String blkPath = directory + blkID + "." + format;;

							FileInputStream fins = new FileInputStream(blkPath);
							
							int data;
							int count = 0;
							while (-1 != (data = fins.read()))
							{
								count ++;
								outStream.write(data);
							}
							System.out.println("==send done==" + count);
							fins.close();
							inStream.close();
							outStream.close();
							socket.close();
							break;
						}
						
						if (recv.equals("done")){
							System.out.println("断开连接");
							inStream.close();
							outStream.close();
							socket.close();
							break;
						}
					}
				}
			} catch (IOException e) {
				// TODO 异常，说明DataNode荡了，做处理，通知Client和NameNode
//				e.printStackTrace();
				
				try {
					Socket sc = new Socket();
					sc.connect(new InetSocketAddress(client, blkACKPort));
					OutputStream oStream = sc.getOutputStream();
					JSONObject json = new JSONObject();
					json.put("ackBlockNum", -1);
					oStream.write(json.toString().getBytes());
					oStream.flush();
					oStream.close();
					sc.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			
			
		}



		private Socket connect(JSONObject node) {
			Socket socket = new Socket(); 
			try {
				socket.connect(new InetSocketAddress(node.getString("ipaddr"), node.getInt("blkPort")));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
			return socket;
		}
	}
}
