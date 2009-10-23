package jp.co.rakuten.rit.roma.client.util;

import java.util.Date;
import java.util.Properties;

import jp.co.rakuten.rit.roma.client.*;
import junit.framework.TestCase;

public class ObjectWrapperTest extends TestCase {

	private static String NODE_ID = AllTests.NODE_ID;

	private static String KEY_PREFIX = ObjectWrapperTest.class.getName();
	
	private static RomaClient CLIENT = null;
	
	private static ObjectWrapper<Serial> APPENDER = null;
	
	private static String KEY = null;
	
	public ObjectWrapperTest() {
		super();
	}
	
	@Override
	public void setUp() throws Exception {
		RomaClientFactory factory = RomaClientFactory.getInstance();
		CLIENT = factory.newRomaClient(new Properties());
		APPENDER = new ObjectWrapper<Serial>(CLIENT);
		CLIENT.open(Node.create(NODE_ID));
	}
	
	@Override
	public void tearDown() throws Exception {
		APPENDER.delete(KEY);
		APPENDER = null;
		CLIENT.close();
		CLIENT = null;
		KEY = null;
	}

	static class Serial implements java.io.Serializable {

		private static final long serialVersionUID = -9047265610334678584L;
		
		String _a;
		int _b;

		Serial(String a, int b) {
			_a = a;
			_b = b;
		}

		public boolean equals(Object obj) {
			if (!(obj instanceof Serial)) {
				return false;
			}

			Serial s = (Serial) obj;
			return s._a.equals(_a) && s._b == _b;
		}
	}

	static class NonSerial {
		String _a;
		int _b;

		NonSerial(String a, int b) {
			_a = a;
			_b = b;
		}
	}

	public void testDelete01() throws Exception {
		KEY = KEY_PREFIX + "testDelete01";

		assertTrue(APPENDER.put(KEY, new Serial("muga", 20)));
		assertTrue(APPENDER.delete(KEY));
	}

	public void testDelete02() throws Exception {
		KEY = KEY_PREFIX + "testDelete02";
		assertFalse(APPENDER.delete(KEY));
	}

	public void testGet01() throws Exception {
		KEY = KEY_PREFIX + "testGet01";
		assertTrue(APPENDER.put(KEY, new Serial("muga", 20)));
		assertEquals(new Serial("muga", 20), APPENDER.get(KEY));
	}

	public void testGet02() throws Exception {
		KEY = KEY_PREFIX + "testGet02";
		assertEquals(null, APPENDER.get(KEY));
	}

	public void testPut01() throws Exception {
		KEY = KEY_PREFIX + "testPut01";
		assertTrue(APPENDER.put(KEY, new Serial("muga", 20)));
		assertEquals(new Serial("muga", 20), APPENDER.get(KEY));
	}

	public void testPut02() throws Exception {
		KEY = KEY_PREFIX + "testPut02";
		Date zero = new Date(0);
		assertTrue(APPENDER.put(KEY, new Serial("muga", 20), zero));
		assertEquals(new Serial("muga", 20), APPENDER.get(KEY));
	}

	public void testPut03() throws Exception {
		KEY = KEY_PREFIX + "testPut03";
		Date one = new Date(2000);
		assertTrue(APPENDER.put(KEY, new Serial("muga", 20), one));
		assertEquals(new Serial("muga", 20), APPENDER.get(KEY));
		Thread.sleep(3000);
		assertEquals(null, APPENDER.get(KEY));
	}

	public void testPut04() throws Exception {
		ObjectWrapper<NonSerial> appender = new ObjectWrapper<NonSerial>(CLIENT);
		KEY = KEY_PREFIX + "testPut04";
		try {
			appender.put(KEY, new NonSerial("muga", 20));
			fail("error");
		} catch (java.io.NotSerializableException e) {
			assertTrue(true);
			appender.delete(KEY);
		}
	}
}
