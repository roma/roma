package jp.co.rakuten.rit.roma.client;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import jp.co.rakuten.rit.roma.client.commands.TimeoutFilter;
import junit.framework.TestCase;

public class RomaClientImplTest extends TestCase {

	private static String NODE_ID = AllTests.NODE_ID;

	private static String KEY_PREFIX = RomaClientImplTest.class.getName();

	private static RomaClient CLIENT = null;

	private static String KEY = null;

	public RomaClientImplTest() {
		super();
	}

	@Override
	public void setUp() throws Exception {
		RomaClientFactory factory = RomaClientFactory.getInstance();
		CLIENT = factory.newRomaClient(new Properties());
		CLIENT.open(Node.create(NODE_ID));
		TimeoutFilter.timeout = 100 * 1000;
	}

	@Override
	public void tearDown() throws Exception {
		CLIENT.delete(KEY);
		CLIENT.close();
		CLIENT = null;
		KEY = null;
	}

	public void testGets01() throws Exception {
		try {
			KEY = KEY_PREFIX + "testGets01";
			assertTrue(CLIENT.put(KEY + "01", "01".getBytes()));
			assertTrue(CLIENT.put(KEY + "02", "02".getBytes()));
			assertTrue(CLIENT.put(KEY + "03", "03".getBytes()));
			List<String> keys = new ArrayList<String>();
			keys.add(KEY + "01");
			keys.add(KEY + "02");
			keys.add(KEY + "03");
			Map<String, byte[]> values = CLIENT.gets(keys);
			assertEquals(3, values.size());
			assertEquals("01", new String(values.get(KEY + "01")));
			assertEquals("02", new String(values.get(KEY + "02")));
			assertEquals("03", new String(values.get(KEY + "03")));
		} finally {
			CLIENT.delete(KEY + "01");
			CLIENT.delete(KEY + "02");
			CLIENT.delete(KEY + "03");
		}
	}

	public void testGets02() throws Exception {
		try {
			KEY = KEY_PREFIX + "testGets02";
			assertTrue(CLIENT.put(KEY + "01", "01".getBytes()));
			assertTrue(CLIENT.put(KEY + "02", "02".getBytes()));
			assertTrue(CLIENT.put(KEY + "03", "03".getBytes()));
			List<String> keys = new ArrayList<String>();
			keys.add(KEY + "01");
			keys.add(KEY + "02");
			keys.add(KEY + "04");
			keys.add(KEY + "05");
			keys.add(KEY + "03");
			Map<String, byte[]> values = CLIENT.gets(keys);
			assertEquals(3, values.size());
			assertEquals("01", new String(values.get(KEY + "01")));
			assertEquals("02", new String(values.get(KEY + "02")));
			assertEquals("03", new String(values.get(KEY + "03")));
		} finally {
			CLIENT.delete(KEY + "01");
			CLIENT.delete(KEY + "02");
			CLIENT.delete(KEY + "03");
		}
	}

	public void testGets03() throws Exception {
		try {
			KEY = KEY_PREFIX + "testGets03";
			assertTrue(CLIENT.put(KEY + "01", "01".getBytes(), new Date(2000)));
			assertTrue(CLIENT.put(KEY + "02", "02".getBytes()));
			assertTrue(CLIENT.put(KEY + "03", "03".getBytes()));
			List<String> keys = new ArrayList<String>();
			keys.add(KEY + "01");
			keys.add(KEY + "02");
			keys.add(KEY + "04");
			keys.add(KEY + "05");
			keys.add(KEY + "03");
			Map<String, byte[]> values = CLIENT.gets(keys);
			assertEquals(3, values.size());
			assertEquals("01", new String(values.get(KEY + "01")));
			assertEquals("02", new String(values.get(KEY + "02")));
			assertEquals("03", new String(values.get(KEY + "03")));
			Thread.sleep(3000);
			values = CLIENT.gets(keys);
			assertEquals(2, values.size());
			assertEquals("02", new String(values.get(KEY + "02")));
			assertEquals("03", new String(values.get(KEY + "03")));
		} finally {
			CLIENT.delete(KEY + "01");
			CLIENT.delete(KEY + "02");
			CLIENT.delete(KEY + "03");
		}
	}

	public void testPut01() throws Exception {
		KEY = KEY_PREFIX + "testPut01";
		assertTrue(CLIENT.put(KEY, "01".getBytes()));
		byte[] ret = CLIENT.get(KEY);
		assertEquals("01", new String(ret));
	}

	public void testPut02() throws Exception {
		KEY = KEY_PREFIX + "testPut02";
		Date zero = new Date(0);
		assertTrue(CLIENT.put(KEY, "01".getBytes(), zero));
		byte[] ret = CLIENT.get(KEY);
		assertEquals("01", new String(ret));
	}

	public void testPut03() throws Exception {
		KEY = KEY_PREFIX + "testPut03";
		Date one = new Date(2000);
		assertTrue(CLIENT.put(KEY, "01".getBytes(), one));
		byte[] ret = CLIENT.get(KEY);
		assertEquals("01", new String(ret));
		Thread.sleep(3000);
		ret = CLIENT.get(KEY);
		assertEquals(null, ret);
	}

	public void testAppend01() throws Exception {
		KEY = KEY_PREFIX + "testAppend01";
		assertTrue(CLIENT.put(KEY, "01".getBytes()));
		assertTrue(CLIENT.append(KEY, "02".getBytes()));
		assertTrue(CLIENT.append(KEY, "03".getBytes()));
		byte[] ret = CLIENT.get(KEY);
		assertEquals("010203", new String(ret));
	}

	public void testAppend02() throws Exception {
		KEY = KEY_PREFIX + "testAppend02";
		assertFalse(CLIENT.append(KEY, "01".getBytes()));
	}

	public void testAppend03() throws Exception {
		KEY = KEY_PREFIX + "testAppend03";
		assertTrue(CLIENT.put(KEY, "01".getBytes()));
		assertTrue(CLIENT.append(KEY, "02".getBytes()));
		assertTrue(CLIENT.append(KEY, "03".getBytes(), new Date(2000)));
		byte[] ret = CLIENT.get(KEY);
		assertEquals("010203", new String(ret));
		Thread.sleep(3000);
		ret = CLIENT.get(KEY);
		assertEquals(null, ret);
	}

	public void testAppend04() throws Exception {
		KEY = KEY_PREFIX + "testAppend04";
		assertTrue(CLIENT.put(KEY, "01".getBytes()));
		assertTrue(CLIENT.append(KEY, "02".getBytes()));
		assertTrue(CLIENT.append(KEY, "03".getBytes(), new Date(2000)));
		byte[] ret = CLIENT.get(KEY);
		assertEquals("010203", new String(ret));
		Thread.sleep(3000);
		assertFalse(CLIENT.append(KEY, "04".getBytes()));
	}

	public void testPrepend01() throws Exception {
		KEY = KEY_PREFIX + "testPrepend01";
		assertTrue(CLIENT.put(KEY, "01".getBytes()));
		assertTrue(CLIENT.prepend(KEY, "02".getBytes()));
		assertTrue(CLIENT.prepend(KEY, "03".getBytes()));
		byte[] ret = CLIENT.get(KEY);
		assertEquals("030201", new String(ret));
	}

	public void testPrepend02() throws Exception {
		KEY = KEY_PREFIX + "testPrepend02";
		assertFalse(CLIENT.prepend(KEY, "01".getBytes()));
	}

	public void testPrepend03() throws Exception {
		KEY = KEY_PREFIX + "testPrepend03";
		assertTrue(CLIENT.put(KEY, "01".getBytes()));
		assertTrue(CLIENT.prepend(KEY, "02".getBytes()));
		assertTrue(CLIENT.prepend(KEY, "03".getBytes(), new Date(2000)));
		byte[] ret = CLIENT.get(KEY);
		assertEquals("030201", new String(ret));
		Thread.sleep(3000);
		ret = CLIENT.get(KEY);
		assertEquals(null, ret);
	}

	public void testPrepend04() throws Exception {
		KEY = KEY_PREFIX + "testPrepend04";
		assertTrue(CLIENT.put(KEY, "01".getBytes()));
		assertTrue(CLIENT.prepend(KEY, "02".getBytes()));
		assertTrue(CLIENT.prepend(KEY, "03".getBytes(), new Date(2000)));
		byte[] ret = CLIENT.get(KEY);
		assertEquals("030201", new String(ret));
		Thread.sleep(3000);
		assertFalse(CLIENT.prepend(KEY, "04".getBytes()));
	}

}
