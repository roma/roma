package jp.co.rakuten.rit.roma.client.util;

import jp.co.rakuten.rit.roma.client.*;

import java.io.UnsupportedEncodingException;
import java.util.Date;

/**
 * 
 * Sample code might look like:
 * 
 * <blockquote>
 * 
 * <pre>
 * public static void main(String[] args) throws Exception {
 *     List&lt;Node&gt; initNodes = new ArrayList&lt;Node&gt;();
 *     initNodes.add(Node.create(&quot;localhost_11211&quot;));
 *     initNodes.add(Node.create(&quot;localhost_11212&quot;));
 *     // create and initialize the instance of ROMA Client
 *     RomaClientFactory fact = RomaClientFactory.getInstance();
 *     RomaClient client = fact.newRomaClient();
 * 
 *     StringWrapper sWrapper = StringWrapper(client);
 *     // open connections with ROMA
 *     client.open(initNodes);
 *     // append 
 *     sWrapper.put(&quot;muga&quot;, &quot;m&quot;);
 *     sWrapper.append(&quot;muga&quot;, &quot;u&quot;);
 *     sWrapper.append(&quot;muga&quot;, &quot;g&quot;);
 *     sWrapper.append(&quot;muga&quot;, &quot;a&quot;);
 *     // ret is &quot;muga&quot;
 *     String ret = sWrapper.get(&quot;muga&quot;);
 *     // prepend 
 *     sWrapper.put(&quot;torii&quot;, &quot;a&quot;);
 *     sWrapper.prepend(&quot;torii&quot;, &quot;g&quot;);
 *     sWrapper.prepend(&quot;torii&quot;, &quot;u&quot;);
 *     sWrapper.prepend(&quot;torii&quot;, &quot;m&quot;);
 *     ret = sWrapper.get(&quot;torii&quot;);
 *     // ret is &quot;muga
 *     // close the connection
 *     client.close();
 * }
 * </pre>
 * 
 * </blockquote>
 * 
 * 
 */
public class StringWrapper {

    protected RomaClient client;

    public StringWrapper(RomaClient client) {
	if (client == null) {
	    throw new NullPointerException();
	}
	this.client = client;
    }

    public RomaClient getRomaClient() {
	return this.client;
    }

    public boolean append(String key, String value) throws ClientException {
	return append(key, value, new Date(0));
    }

    public boolean append(String key, String value, Date expiry)
	    throws ClientException {
	try {
	    return client.append(key, value.getBytes(StringUtil.ENCODING), expiry);
	} catch (UnsupportedEncodingException e) {
	    throw new ClientException(e);
	}
    }

    public boolean delete(String key) throws ClientException {
	return client.delete(key);
    }

    public String get(String key) throws ClientException {
	if (key == null) {
	    throw new NullPointerException("key is null");
	}
	if (key.equals("")) {
	    throw new IllegalArgumentException();
	}

	byte[] b = client.get(key);
	if (b == null) {
	    return null;
	} else {
	    try {
		return new String(b, StringUtil.ENCODING);
	    } catch (UnsupportedEncodingException e) {
		throw new ClientException(e);
	    }
	}
    }

    public boolean prepend(String key, String value) throws ClientException {
	return prepend(key, value, new Date(0));
    }

    public boolean prepend(String key, String value, Date expiry)
	    throws ClientException {
	try {
	    return client.prepend(key, value.getBytes(StringUtil.ENCODING), expiry);
	} catch (UnsupportedEncodingException e) {
	    throw new ClientException(e);
	}
    }

    public boolean put(String key, String value) throws ClientException {
	return this.put(key, value, new Date(0));
    }

    public boolean put(String key, String value, Date expiry)
	    throws ClientException {
	try {
	    return client.put(key, value.getBytes(StringUtil.ENCODING), expiry);
	} catch (UnsupportedEncodingException e) {
	    throw new ClientException(e);
	}
    }
}
