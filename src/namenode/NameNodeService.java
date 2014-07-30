package namenode;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class NameNodeService {
	
	public final static int PORT_4_CLIENT = 1314;
	
	public class ServiceForClientThread extends Thread {
		private ServerSocket serverSocket;
		public ServiceForClientThread(){
			try {
				serverSocket = new ServerSocket(PORT_4_CLIENT);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		public void run() {
			while 
		}
	}
}
