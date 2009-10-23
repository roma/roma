package jp.co.rakuten.rit.roma.client.util;

import java.util.Date;
import java.util.Properties;

import jp.co.rakuten.rit.roma.client.*;
import jp.co.rakuten.rit.roma.client.commands.TimeoutFilter;
import junit.framework.TestCase;

public class StringWrapperTest extends TestCase {

    private static String NODE_ID = AllTests.NODE_ID;

	private static String KEY_PREFIX = StringWrapperTest.class.getName();

	private static RomaClient CLIENT = null;

	private static StringWrapper APPENDER = null;

	private static String KEY = null;

	public StringWrapperTest() {
		super();
	}
	
	@Override
	public void setUp() throws Exception {
		RomaClientFactory factory = RomaClientFactory.getInstance();
		CLIENT = factory.newRomaClient(new Properties());
		APPENDER = new StringWrapper(CLIENT);
		CLIENT.open(Node.create(NODE_ID));
		TimeoutFilter.timeout = 100 * 1000;
	}
	
	@Override
	public void tearDown() throws Exception {
		APPENDER.delete(KEY);
		APPENDER = null;
		CLIENT.close();
		CLIENT = null;
		KEY = null;
	}
	
	public void testAppend01() throws Exception {
		KEY = KEY_PREFIX + "testAppend01";
		assertTrue(APPENDER.put(KEY, "01"));
		assertTrue(APPENDER.append(KEY, "02"));
		assertTrue(APPENDER.append(KEY, "03"));
		assertTrue(APPENDER.append(KEY, "04"));
		String value = APPENDER.get(KEY);
		assertEquals("01020304", value);
	}

	public void testAppend02() throws Exception {
		KEY = KEY_PREFIX + "testAppend02";
		assertFalse(APPENDER.append(KEY, "01"));
	}

	public void testAppend03() throws Exception {
		KEY = KEY_PREFIX + "testAppend03";
		assertTrue(APPENDER.put(KEY, "01"));
		assertTrue(APPENDER.append(KEY, "02"));
		assertTrue(APPENDER.append(KEY, "03", new Date(2000)));
		assertEquals("010203", APPENDER.get(KEY));
		Thread.sleep(3000);
		assertEquals(null, APPENDER.get(KEY));
	}

	public void testAppend04() throws Exception {
		KEY = KEY_PREFIX + "testAppend04";
		assertTrue(APPENDER.put(KEY, "01"));
		assertTrue(APPENDER.append(KEY, "02"));
		assertTrue(APPENDER.append(KEY, "03", new Date(2000)));
		assertEquals("010203", APPENDER.get(KEY));
		Thread.sleep(3000);
		assertFalse(APPENDER.append(KEY, "04"));
	}

	public void testDelete01() throws Exception {
		KEY = KEY_PREFIX + "testDelete01";
		assertTrue(APPENDER.put(KEY, "01"));
		assertTrue(APPENDER.delete(KEY));
	}

	public void testDelete02() throws Exception {
		KEY = KEY_PREFIX + "testDelete02";
		assertFalse(APPENDER.delete(KEY));
	}

	public void testGet01() throws Exception {
		KEY = KEY_PREFIX + "testGet01";
		assertTrue(APPENDER.put(KEY, "01"));
		assertEquals("01", APPENDER.get(KEY));
		assertTrue(APPENDER.append(KEY, "02"));
		assertEquals("0102", APPENDER.get(KEY));
		assertTrue(APPENDER.append(KEY, "03"));
		assertEquals("010203", APPENDER.get(KEY));
		assertTrue(APPENDER.prepend(KEY, "04"));
		assertEquals("04010203", APPENDER.get(KEY));
	}

	public void testGet02() throws Exception {
		KEY = KEY_PREFIX + "testGet02";
		assertEquals(null, APPENDER.get(KEY));
	}

	public void testPrepend01() throws Exception {
		KEY = KEY_PREFIX + "testPrepend01";
		assertTrue(APPENDER.put(KEY, "01"));
		assertTrue(APPENDER.prepend(KEY, "02"));
		assertTrue(APPENDER.prepend(KEY, "03"));
		assertTrue(APPENDER.prepend(KEY, "04"));

		String value = APPENDER.get(KEY);
		assertEquals("04030201", value);
	}

	public void testPrepend02() throws Exception {
		KEY = KEY_PREFIX + "testPrepend02";
		assertFalse(APPENDER.prepend(KEY, "01"));
	}

	public void testPrepend03() throws Exception {
		KEY = KEY_PREFIX + "testPrepend03";
		assertTrue(APPENDER.put(KEY, "01"));
		assertTrue(APPENDER.prepend(KEY, "02"));
		assertTrue(APPENDER.prepend(KEY, "03", new Date(2000)));
		assertEquals("030201", APPENDER.get(KEY));
		Thread.sleep(3000);
		assertEquals(null, APPENDER.get(KEY));
	}

	public void testPrepend04() throws Exception {
		KEY = KEY_PREFIX + "testPrepend04";
		assertTrue(APPENDER.put(KEY, "01"));
		assertTrue(APPENDER.prepend(KEY, "02"));
		assertTrue(APPENDER.prepend(KEY, "03", new Date(2000)));
		assertEquals("030201", APPENDER.get(KEY));
		Thread.sleep(3000);
		assertFalse(APPENDER.prepend(KEY, "04"));
	}

	public void testPut01() throws Exception {
		KEY = KEY_PREFIX + "testPut01";
		assertTrue(APPENDER.put(KEY, "01"));
		assertEquals("01", APPENDER.get(KEY));
	}

	public void testPut02() throws Exception {
		KEY = KEY_PREFIX + "testPut02";
		Date zero = new Date(0);
		assertTrue(APPENDER.put(KEY, "01", zero));
		assertEquals("01", APPENDER.get(KEY));
	}

	public void testPut03() throws Exception {
		KEY = KEY_PREFIX + "testPut03";
		Date one = new Date(2000);
		assertTrue(APPENDER.put(KEY, "01", one));
		assertEquals("01", APPENDER.get(KEY));
		Thread.sleep(3000);
		assertEquals(null, APPENDER.get(KEY));
	}
}
