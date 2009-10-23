package jp.co.rakuten.rit.roma.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * A connection to a ROMA process.  
 * 
 */
public class Connection {

    public Socket sock;

    public ExtInputStream in;

    public OutputStream out;

    /**
     * Construct a connection to a ROMA process.  
     * 
     * @param sock - a socket of the ROMA process to connect to 
     * @throws IOException - if creating this connection fails
     */
    public Connection(final Socket sock) throws IOException {
	this.sock = sock;
	this.in = new ExtInputStream(sock.getInputStream());
	this.out = new BufferedOutputStream(sock.getOutputStream());
    }

    /**
     * Close this connection.  
     * 
     * @throws IOException - if closing this connection fails 
     */
    public void close() throws IOException {
	if (in != null) {
	    in.close();
	}
	if (out != null) {
	    out.close();
	}
	if (sock != null) {
	    sock.close();
	}
    }

    public static long TIME = 0;

    public class ExtInputStream extends BufferedInputStream {

	private byte[] one_byte = new byte[1];

	ExtInputStream(InputStream in) {
	    super(in);
	}

	public void read(int byteLen) throws IOException {
	    for (int i = 0; i < byteLen; ++i) {
		super.read(one_byte);
	    }
	}

	public String readLine() throws IOException {
	    StringBuilder ret = new StringBuilder();

	    long t = System.currentTimeMillis();

	    while (super.read(one_byte) > 0) {
		if (one_byte[0] == 0x0d) { // \r
		    break;
		} else {
		    ret.append((char) one_byte[0]);
		}
	    }

	    TIME = TIME + (System.currentTimeMillis() - t);

	    super.read(one_byte); // \n
	    return ret.toString();
	}
    }
}
