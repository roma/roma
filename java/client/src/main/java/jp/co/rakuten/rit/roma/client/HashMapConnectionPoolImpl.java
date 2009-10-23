package jp.co.rakuten.rit.roma.client;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * 
 */
public class HashMapConnectionPoolImpl implements ConnectionPool {

    protected int size;

    HashMap<Node, List<Connection>> pool = new HashMap<Node, List<Connection>>();

    public HashMapConnectionPoolImpl(final int size) {
	this.size = size;
    }

    public synchronized Connection get(Node node) throws IOException {
	List<Connection> conns = pool.get(node);
	if (conns == null) {
	    conns = new ArrayList<Connection>();
	    pool.put(node, conns);
	}
	if (conns.size() == 0) {
	    conns.add(new Connection(new Socket(node.host, node.port)));
	}
	return conns.remove(0);
    }

    public synchronized void put(Node node, Connection conn) throws IOException {
	try {
	    pool.get(node).add(conn);
	} catch (Exception e) {
	    // do nothing
	}
    }

    public synchronized void delete(Node node) {
	pool.remove(node);
    }

    public synchronized void closeAll() {
	pool.clear();
    }
}
