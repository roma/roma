package jp.co.rakuten.rit.roma.client.util;

import java.io.*;
import java.util.Date;
import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.RomaClient;

/**
 * 
 * Sample code might look like:
 * 
 * <blockquote>
 * 
 * <pre>
 *     public static void main(String[] args) throws Exception {
 *          List&lt;Node&gt; initNodes = new ArrayList&lt;Node&gt;();
 *          initNodes.add(Node.create(&quot;localhost_11211&quot;));
 *          initNodes.add(Node.create(&quot;localhost_11212&quot;));
 *          
 *          RomaClientFactory factory = RomaClientFactory.getInstance();
 *          RomaClient client = factory.newRomaClient();
 *          
 *          ObjectWrapper&lt;Foo&gt; oWrapper = new ObjectWrapper&lt;Foo&gt;(client);
 *             
 *          // open connections with ROMA
 *          client.open(initNodes);
 *          // execute a get command
 *          oWrapper.put(&quot;foo&quot;, new Foo());
 *          // execute a put command
 *          Foo f = oWrapper.get(&quot;foo&quot;);
 *          // close the connection
 *          client.close();
 *     }
 *     class Foo implements Serializable { ... ... }
 * </pre>
 * 
 * </blockquote>
 * 
 * 
 */
public class ObjectWrapper<T> {

    protected RomaClient client;

    public ObjectWrapper(RomaClient client) {
	this.client = client;
    }

    public boolean delete(String key) throws ClientException {
	return client.delete(key);
    }

    @SuppressWarnings("unchecked")
    public T get(String key) throws ClientException, IOException,
	    ClassNotFoundException {
	byte[] value = client.get(key);
	if (value == null) {
	    return (T) null;
	}
	Object obj = DataSerialization.toObject(value);
	return (T) obj;
    }

    public boolean put(String key, T obj) throws ClientException, IOException {
	return put(key, obj, new Date(0));
    }

    public boolean put(String key, T obj, Date expiry) throws ClientException,
	    IOException {
	byte[] value = DataSerialization.toByteArray(obj);
	return client.put(key, value, expiry);
    }

    public static class DataSerialization {

	public static Object toObject(byte[] bytes) throws IOException,
		ClassNotFoundException {
	    if (bytes == null || bytes.length == 0) {
		return null;
	    }
	    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
	    ObjectInputStream oin = new ObjectInputStream(in);
	    Object obj = oin.readObject();
	    return obj;
	}

	public static byte[] toByteArray(Object obj) throws IOException {
	    if (obj == null) {
		return new byte[0];
	    }
	    ByteArrayOutputStream out = new ByteArrayOutputStream();
	    ObjectOutputStream oout = new ObjectOutputStream(out);
	    oout.writeObject(obj);
	    oout.flush();
	    oout.close();
	    return out.toByteArray();
	}
    }
}
