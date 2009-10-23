package jp.co.rakuten.rit.roma.client.util;

import java.awt.image.BufferedImage;
import java.io.File;
import java.util.Properties;

import javax.imageio.ImageIO;
import jp.co.rakuten.rit.roma.client.*;
import junit.framework.TestCase;

public class BufferedImageWrapperTest extends TestCase {

    private static String NODE_ID = AllTests.NODE_ID;

    private static String KEY_PREFIX =
	BufferedImageWrapperTest.class.getName();

    public BufferedImageWrapperTest() {
	super();
    }

	public void testPut01() throws Exception {
		//assertTrue(true);

		RomaClientFactory factory
			= RomaClientFactory.getInstance();
		RomaClient client = factory.newRomaClient(new Properties());
		BufferedImageWrapper appender = new BufferedImageWrapper(client);
		client.open(Node.create(NODE_ID));
		appender.setFormat("jpg");
		String filePathName = "src/test/resources/";
		String testPathName = "target/";
		for (int i = 1; i <= 35; ++i) {
			File imgFile = new File(filePathName + i + ".jpg");
			BufferedImage img = ImageIO.read(imgFile);
			assertTrue(appender.put(KEY_PREFIX + "_img_" + i, img));
			BufferedImage newImg = appender.get(KEY_PREFIX + "_img_" + i);
			//assertEquals(img.hashCode(), newImg.hashCode());
			ImageIO.write(newImg, "jpg", new File(testPathName + "new_" + i
					+ ".jpg"));
		}
		client.close();
	}
}
