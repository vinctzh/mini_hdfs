package client;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import common.MiniHDFSConstants;
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
			System.out.println("��ʼ���Ϳ��߳�");
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
		// TODO: �������ܵ����
		return true;
	}
	
	public class SendBlockThread extends Thread {
		String blkID ;
		JSONArray targets;
		JSONObject targetDN;
		
		public SendBlockThread(JSONObject locatedBlock) {
			blkID = locatedBlock.getString("blockId");
			targets = locatedBlock.getJSONArray("targets");
			int curIndex = locatedBlock.getInt("curIndex");
			System.err.println("��ǰ��"+blkID+"��"+curIndex+"������");
			targetDN = targets.getJSONObject(curIndex);
		}
		
		public void run() {
			System.out.println("SendBlockThread ��ʼrun " );
			byte buffer[] = new byte[1024];
			
			Socket socket = connect(targetDN);
			try {
				InputStream inStream = socket.getInputStream();
				OutputStream outStream = socket.getOutputStream();
				System.out.println("SendBlockThread�� " + socket.toString());
				
				int len;
				while (true) {
					len = inStream.read(buffer);
					if (len > 0) {
						String recv = new String(buffer,0,len);
						System.out.println("���յ�DataNode����Ϣ�� "+recv);
						// �����Ѿ�����
						// ���Ͱ���datanode��json����
						if (recv.equals("hello")) {
							outStream.write(("putBlock" + locatedBlock.toString()).getBytes());
						}
						// blkinfo�Ѿ����ܵ�����ʼ��ʵ�ʵ�blk����
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
							System.out.println("�Ͽ�����");
							inStream.close();
							outStream.close();
							socket.close();
							break;
						}
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
