package jp.co.rakuten.rit.roma.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.util.NoSuchElementException;

import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

public class SocketPool implements Closeable {

    private ObjectPool pool;

    public SocketPool(final String host, final int port, int max) {
	pool = new GenericObjectPool(new PoolableObjectFactory() {

	    public void destroyObject(Object obj) throws Exception {
		if (obj instanceof Socket) {
		    ((Socket) obj).close();
		}
	    }

	    public boolean validateObject(Object obj) {
		if (obj instanceof Socket) {
		    return ((Socket) obj).isConnected();
		}
		return false;
	    }

	    public Object makeObject() throws Exception {
		return new Socket(host, port);
	    }

	    public void activateObject(Object obj) throws Exception {
		// do nothing
	    }

	    public void passivateObject(Object obj) throws Exception {
		// do nothing
	    }

	}, max);
    }

    public Socket get() throws Exception, NoSuchElementException,
	    IllegalStateException {
	return (Socket) pool.borrowObject();
    }

    public void put(Socket socket) throws Exception {
	pool.returnObject(socket);
    }

    public void close() throws IOException {
	try {
	    pool.clear();
	} catch (Exception e) {
	    throw new IOException(e);
	}
    }
}
