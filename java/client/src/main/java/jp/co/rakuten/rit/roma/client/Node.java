package jp.co.rakuten.rit.roma.client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 
 */
public class Node {

    public static List<Node> create(List<String> IDs) {
	List<Node> result = new ArrayList<Node>(IDs.size());
	Iterator<String> iter = IDs.iterator();
	while (iter.hasNext()) {
	    result.add(create(iter.next()));
	}
	return result;
    }

    public static Node create(String ID) {
	int index = ID.indexOf('_');
	String host = ID.substring(0, index);
	try {
	    int port = Integer.parseInt(ID.substring(index + 1, ID.length()));
	    return new Node(host, port);
	} catch (NumberFormatException e) {
	    throw e;
	    // return null;
	}
    }

    String host;

    int port;

    String ID;

    Node(String host, int port) {
	this.host = host;
	this.port = port;
	this.ID = this.host + "_" + this.port;
    }
    
    public String getHost() {
	return host;
    }
    
    public int getPort() {
	return port;
    }

    @Override
    public int hashCode() {
	return this.ID.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
	if (!(obj instanceof Node)) {
	    return false;
	}

	Node n = (Node) obj;
	return n.host.equals(this.host) && n.port == this.port;
    }

    @Override
    public String toString() {
	StringBuilder sb = new StringBuilder();
	sb.append(host).append("_").append(port);
	return sb.toString();
    }
}
