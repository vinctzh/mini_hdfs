package datanode;

public class DataNodeClient_01 {

	public static void main(String[] args) {
		if (args.length != 1 ) {
			System.err.println("Uage: DataNodeClient [local config file]\n\t eg: DataNodeClient storageInfo.txt");
		} else {
			DataNodeDameon dnd = new DataNodeDameon("storageInfo01.txt");
			dnd.start();
		}
		DataNodeDameon dnd = new DataNodeDameon("storageInfo01.txt");
		dnd.start();
		
	}
}
