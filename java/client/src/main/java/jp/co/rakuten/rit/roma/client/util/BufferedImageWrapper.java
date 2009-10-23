package jp.co.rakuten.rit.roma.client.util;

import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import javax.imageio.ImageIO;
import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.RomaClient;

/**
 * 
 * Sample code might look like:
 * 
 * <blockquote>
 * 
 * <pre>
 * public static void main(String[] args) throws Exception {
 *     String fileName = &quot;./data/foo.jpg&quot;;
 *     File inputFile = new File(fileName);
 *     BufferedImage img = ImageIO.read(inputFile);
 *     List&lt;Node&gt; initNodes = new ArrayList&lt;Node&gt;();
 *     initNodes.add(Node.create(&quot;localhost_11211&quot;));
 *     initNodes.add(Node.create(&quot;localhost_11212&quot;));
 *     RomaClientFactory factory = RomaClientFactory.getInstance();
 *     RomaClient client = factory.newRomaClient();
 *     BufferedImageWrapper imgWrapper = new BufferedImageWrapper(client);
 *     // open connections with ROMA
 *     client.open(initNodes);
 *     // set a format
 *     imgWrapper.setFormat(&quot;jpg&quot;);
 *     // execute a get command
 *     imgWrapper.put(fileName, img);
 *     // execute a put command
 *     BufferedImage newImg = imgWrapper.get(fileName);
 *     ImageIO.write(newImg, &quot;jpg&quot;, new File(&quot;./data/foo-new.jpg&quot;));
 *     // close the connection
 *     client.close();
 * }
 * </pre>
 * 
 * </blockquote>
 * 
 * 
 */
public class BufferedImageWrapper {

    protected RomaClient client;

    protected String format; // e.g. "jpeg"

    public BufferedImageWrapper(RomaClient client) {
	this.client = client;
    }

    public void setFormat(final String format) {
	this.format = format;
    }

    public boolean delete(String key) throws ClientException {
	return client.delete(key);
    }

    public BufferedImage get(String key) throws ClientException, IOException {
	byte[] value = client.get(key);
	if (value == null) {
	    return (BufferedImage) null;
	}
	BufferedImage image = DataSerialization.toBufferedImage(value);
	return image;
    }

    public boolean put(String key, BufferedImage image) throws ClientException,
	    IOException {
	return put(key, image, new Date(0));
    }

    public boolean put(String key, BufferedImage image, Date expiry)
	    throws ClientException, IOException {
	if (format == null || format.equals("")) {
	    format = "jpg";
	}
	byte[] value = DataSerialization.toByteArray(format, image);
	return client.put(key, value, expiry);
    }

    static class DataSerialization {

	public static BufferedImage toBufferedImage(byte[] bytes)
		throws IOException {
	    if (bytes == null || bytes.length == 0) {
		return null;
	    }
	    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
	    BufferedInputStream bin = new BufferedInputStream(in);
	    BufferedImage image = ImageIO.read(bin);
	    return image;
	}

	public static byte[] toByteArray(String format, BufferedImage image)
		throws IOException {
	    if (image == null) {
		return new byte[0];
	    }
	    ByteArrayOutputStream out = new ByteArrayOutputStream();
	    BufferedOutputStream bout = new BufferedOutputStream(out);
	    image.flush();
	    ImageIO.write(image, format, bout);
	    bout.flush();
	    bout.close();
	    return out.toByteArray();
	}
    }
}
