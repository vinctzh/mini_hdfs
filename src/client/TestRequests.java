package client;

import java.io.File;

import namenode.NameNode;
import namenode.NameNodeService;

public class TestRequests {
	
	public static void main(String[] args) {
		NameNode nameNode = new NameNode();
		nameNode.showActiveDataNodes();
		
		NameNodeService nnService = new NameNodeService();
		
	}
}
