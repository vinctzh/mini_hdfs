package datanode;

public class DataNodeClient {

	public static void main(String[] args) {
		if (args.length != 1 ) {
			System.err.println("Uage: DataNodeClient [local config file]\n\t eg: DataNodeClient storageInfo.txt");
		} else {
			DataNodeDameon dnd = new DataNodeDameon(args[0]);
			dnd.start();
		}
	}
}
