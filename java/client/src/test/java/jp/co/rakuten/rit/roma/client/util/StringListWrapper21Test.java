package jp.co.rakuten.rit.roma.client.util;

import java.util.List;
import java.util.Properties;

import jp.co.rakuten.rit.roma.client.*;
import junit.framework.TestCase;

public class StringListWrapper21Test extends TestCase {

    private static String NODE_ID = AllTests.NODE_ID;

    private static String KEY_PREFIX = StringListWrapper21Test.class.getName();

    private static RomaClient CLIENT = null;

    private static StringListWrapper LISTUTIL = null;

    private static String KEY = null;

    public StringListWrapper21Test() {
	super();
    }

    @Override
    public void setUp() throws Exception {
	RomaClientFactory factory = RomaClientFactory.getInstance();
	CLIENT = factory.newRomaClient(new Properties());
	LISTUTIL = new StringListWrapper(CLIENT, true);
	CLIENT.open(Node.create(NODE_ID));
    }

    @Override
    public void tearDown() throws Exception {
	LISTUTIL.deleteList(KEY);
	LISTUTIL = null;
	CLIENT.close();
	CLIENT = null;
	KEY = null;
    }

    public void testAppend01() throws Exception {
	KEY = KEY_PREFIX + "testAppend01";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertTrue(LISTUTIL.append(KEY, "04"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(4, list.size());
	assertEquals("01", list.get(0));
	assertEquals("02", list.get(1));
	assertEquals("03", list.get(2));
	assertEquals("04", list.get(3));
    }

    public void testAppend02() throws Exception {
	KEY = KEY_PREFIX + "testAppend02";
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertFalse(LISTUTIL.append(KEY, "04"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(3, list.size());
	assertEquals("01", list.get(0));
	assertEquals("02", list.get(1));
	assertEquals("03", list.get(2));
    }

    public void testAppend03() throws Exception {
	KEY = KEY_PREFIX + "testAppend03";
	try {
	    LISTUTIL = new StringListWrapper(CLIENT, true, -1);
	    fail();
	} catch (Exception e) {
	    assertTrue(e instanceof IllegalArgumentException);
	}
    }

    public void testDeleteList01() throws Exception {
	KEY = KEY_PREFIX + "testDeleteList01";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	LISTUTIL.deleteList(KEY);
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(0, list.size());
    }

    public void testDeleteList02() throws Exception {
	KEY = KEY_PREFIX + "testDeleteList02";
	assertTrue(LISTUTIL.append(KEY, "01"));
	LISTUTIL.deleteList(KEY);
	LISTUTIL.deleteList(KEY);
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(0, list.size());
    }

    public void testDeleteList03() throws Exception {
	KEY = KEY_PREFIX + "testDeleteList03";
	LISTUTIL.deleteList(KEY);
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(0, list.size());
    }

    public void testDeleteList04() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testDeleteList04";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertFalse(LISTUTIL.append(KEY, "04"));
	LISTUTIL.deleteList(KEY);
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(0, list.size());
    }

    public void testDeleteList05() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testDeleteList05";
	LISTUTIL.deleteList(KEY);
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(0, list.size());
    }

    public void testDelete01() throws Exception {
	KEY = KEY_PREFIX + "testDelete01";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertTrue(LISTUTIL.append(KEY, "04"));
	assertTrue(LISTUTIL.delete(KEY, 2));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(3, list.size());
	assertEquals("01", list.get(0));
	assertEquals("02", list.get(1));
	assertEquals("04", list.get(2));
    }

    public void testDelete02() throws Exception {
	KEY = KEY_PREFIX + "testDelete02";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertFalse(LISTUTIL.delete(KEY, 10));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(2, list.size());
	assertEquals("01", list.get(0));
	assertEquals("02", list.get(1));
    }

    public void testDelete03() throws Exception {
	KEY = KEY_PREFIX + "testDelete03";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertTrue(LISTUTIL.append(KEY, "04"));
	assertTrue(LISTUTIL.delete(KEY, "02"));
	assertTrue(LISTUTIL.delete(KEY, "01"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(2, list.size());
	assertEquals("03", list.get(0));
	assertEquals("04", list.get(1));
    }

    public void testDelete04() throws Exception {
	KEY = KEY_PREFIX + "testDelete04";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertTrue(LISTUTIL.append(KEY, "04"));
	assertFalse(LISTUTIL.delete(KEY, "10"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(4, list.size());
	assertEquals("01", list.get(0));
	assertEquals("02", list.get(1));
	assertEquals("03", list.get(2));
	assertEquals("04", list.get(3));
    }

    public void testDelete05() throws Exception {
	KEY = KEY_PREFIX + "testDelete05";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.delete(KEY, "01"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(0, list.size());
    }

    public void testDelete06() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testDelete06";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertFalse(LISTUTIL.append(KEY, "04"));
	assertEquals(false, LISTUTIL.append(KEY, "04"));
	assertTrue(LISTUTIL.delete(KEY, 1));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(2, list.size());
	assertEquals("01", list.get(0));
	assertEquals("03", list.get(1));
    }

    public void testDelete07() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testDelete07";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertFalse(LISTUTIL.delete(KEY, 10));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(2, list.size());
	assertEquals("01", list.get(0));
	assertEquals("02", list.get(1));
    }

    public void testDelete08() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testDelete08";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertFalse(LISTUTIL.append(KEY, "04"));
	assertTrue(LISTUTIL.delete(KEY, "02"));
	assertTrue(LISTUTIL.delete(KEY, "01"));
	assertFalse(LISTUTIL.delete(KEY, "04"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(1, list.size());
	assertEquals("03", list.get(0));
    }

    public void testDelete09() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testDelete09";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertFalse(LISTUTIL.append(KEY, "04"));
	assertFalse(LISTUTIL.delete(KEY, "10"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(3, list.size());
	assertEquals("01", list.get(0));
	assertEquals("02", list.get(1));
	assertEquals("03", list.get(2));
    }

    public void testDelete10() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testDelete10";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.delete(KEY, "01"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(0, list.size());
    }

    public void testDeleteAndAppend01() throws Exception {
	KEY = KEY_PREFIX + "testDeleteAndAppend01";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertTrue(LISTUTIL.append(KEY, "04"));
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "03"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(4, list.size());
	assertEquals("01", list.get(0));
	assertEquals("02", list.get(1));
	assertEquals("04", list.get(2));
	assertEquals("03", list.get(3));
    }

    public void testDeleteAndAppend02() throws Exception {
	KEY = KEY_PREFIX + "testDeleteAndAppend02";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertTrue(LISTUTIL.append(KEY, "04"));
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "05"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(5, list.size());
	assertEquals("01", list.get(0));
	assertEquals("02", list.get(1));
	assertEquals("03", list.get(2));
	assertEquals("04", list.get(3));
	assertEquals("05", list.get(4));
    }

    public void testDeleteAndAppend03() throws Exception {
	KEY = KEY_PREFIX + "testDeleteAndAppend03";
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "01"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(1, list.size());
	assertEquals("01", list.get(0));
    }

    public void testDeleteAndAppend04() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testDeleteAndAppend04";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertFalse(LISTUTIL.append(KEY, "04"));
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "03"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(3, list.size());
	assertEquals("01", list.get(0));
	assertEquals("02", list.get(1));
	assertEquals("03", list.get(2));
    }

    public void testDeleteAndAppend05() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testDeleteAndAppend05";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertFalse(LISTUTIL.append(KEY, "04"));
	assertFalse(LISTUTIL.deleteAndAppend(KEY, "05"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(3, list.size());
	assertEquals("01", list.get(0));
	assertEquals("02", list.get(1));
	assertEquals("03", list.get(2));
    }

    public void testDeleteAndAppend06() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testDeleteAndAppend06";
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "01"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(1, list.size());
	assertEquals("01", list.get(0));
    }

    public void testDeleteAndAppend07() throws Exception {
	long expiry = 1;
	LISTUTIL = new StringListWrapper(CLIENT, true, expiry);
	KEY = KEY_PREFIX + "testDeleteAndAppend07";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertTrue(LISTUTIL.append(KEY, "04"));

	Thread.sleep(expiry * 1000 + 100);
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "05"));
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "06"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(2, list.size());
	assertEquals("05", list.get(0));
	assertEquals("06", list.get(1));
    }

    public void testDeleteAndAppend08() throws Exception {
	long expiry = 1;
	LISTUTIL = new StringListWrapper(CLIENT, true, expiry);
	KEY = KEY_PREFIX + "testDeleteAndAppend08";
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "01"));
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "02"));
	Thread.sleep(expiry * 1000 + 100);
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "03"));
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "04"));
	Thread.sleep(expiry * 1000 + 100);
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "05"));
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "06"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(2, list.size());
	assertEquals("05", list.get(0));
	assertEquals("06", list.get(1));
    }

    public void testDeleteAndAppend09() throws Exception {
	long expiry = 1;
	LISTUTIL = new StringListWrapper(CLIENT, true, expiry);
	KEY = KEY_PREFIX + "testDeleteAndAppend09";
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "01"));
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "02"));
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "03"));
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "04"));
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "02"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(4, list.size());
	assertEquals("01", new String(list.get(0)));
	assertEquals("03", new String(list.get(1)));
	assertEquals("04", new String(list.get(2)));
	assertEquals("02", new String(list.get(3)));

	Thread.sleep(expiry * 1000 + 100);
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "02"));
	list = LISTUTIL.get(KEY);
	assertEquals(1, list.size());
	assertEquals("02", new String(list.get(0)));
    }

    public void testDeleteAndAppend10() throws Exception {
	int size = 3;
	long expiry = 1;
	LISTUTIL = new StringListWrapper(CLIENT, true, size, expiry);
	KEY = KEY_PREFIX + "testDeleteAndAppend10";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertFalse(LISTUTIL.append(KEY, "04"));

	List<String> list = LISTUTIL.get(KEY);
	assertEquals(3, list.size());
	assertEquals("01", new String(list.get(0)));
	assertEquals("02", new String(list.get(1)));
	assertEquals("03", new String(list.get(2)));

	Thread.sleep(expiry * 1000 + 100);
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "02"));
	list = LISTUTIL.get(KEY);
	assertEquals(1, list.size());
	assertEquals("02", new String(list.get(0)));
    }

    public void testDeleteAndAppend11() throws Exception {
	int size = 3;
	long expiry = 1;
	LISTUTIL = new StringListWrapper(CLIENT, true, size, expiry);
	KEY = KEY_PREFIX + "testDeleteAndAppend11";
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "01"));
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "02"));
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "03"));
	assertFalse(LISTUTIL.deleteAndAppend(KEY, "04"));
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "02"));

	List<String> list = LISTUTIL.get(KEY);
	assertEquals(3, list.size());
	assertEquals("01", new String(list.get(0)));
	assertEquals("03", new String(list.get(1)));
	assertEquals("02", new String(list.get(2)));

	Thread.sleep(expiry * 1000 + 100);
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "02"));
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "03"));
	list = LISTUTIL.get(KEY);
	assertEquals(2, list.size());
	assertEquals("02", new String(list.get(0)));
	assertEquals("03", new String(list.get(1)));
    }

    public void testDeleteAndPrepend01() throws Exception {
	KEY = KEY_PREFIX + "testDeleteAndPrepend01";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertTrue(LISTUTIL.append(KEY, "04"));
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "03"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(4, list.size());
	assertEquals("03", list.get(0));
	assertEquals("01", list.get(1));
	assertEquals("02", list.get(2));
	assertEquals("04", list.get(3));
    }

    public void testDeleteAndPrepend02() throws Exception {
	KEY = KEY_PREFIX + "testDeleteAndPrepend02";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertTrue(LISTUTIL.append(KEY, "04"));
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "05"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(5, list.size());
	assertEquals("05", list.get(0));
	assertEquals("01", list.get(1));
	assertEquals("02", list.get(2));
	assertEquals("03", list.get(3));
	assertEquals("04", list.get(4));
    }

    public void testDeleteAndPrepend03() throws Exception {
	KEY = KEY_PREFIX + "testDeleteAndPrepend03";
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "01"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(1, list.size());
	assertEquals("01", list.get(0));
    }

    public void testDeleteAndPrepend04() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testDeleteAndPrepend04";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertFalse(LISTUTIL.append(KEY, "04"));
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "03"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(3, list.size());
	assertEquals("03", list.get(0));
	assertEquals("01", list.get(1));
	assertEquals("02", list.get(2));
    }

    public void testDeleteAndPrepend05() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testDeleteAndPrepend05";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertFalse(LISTUTIL.append(KEY, "04"));
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "05"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(3, list.size());
	assertEquals("05", list.get(0));
	assertEquals("01", list.get(1));
	assertEquals("02", list.get(2));
    }

    public void testDeleteAndPrepend06() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testDeleteAndPrepend06";
	assertTrue(LISTUTIL.deleteAndAppend(KEY, "01"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(1, list.size());
	assertEquals("01", list.get(0));
    }
    
    public void testDeleteAndPrepend07() throws Exception {
	long expiry = 1;
	LISTUTIL = new StringListWrapper(CLIENT, true, expiry);
	KEY = KEY_PREFIX + "testDeleteAndPrepend07";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertTrue(LISTUTIL.append(KEY, "04"));

	Thread.sleep(expiry * 1000 + 100);
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "05"));
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "06"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(2, list.size());
	assertEquals("06", list.get(0));
	assertEquals("05", list.get(1));
    }

    public void testDeleteAndPrepend08() throws Exception {
	long expiry = 1;
	LISTUTIL = new StringListWrapper(CLIENT, true, expiry);
	KEY = KEY_PREFIX + "testDeleteAndPrepend08";
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "01"));
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "02"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(2, list.size());
	assertEquals("02", list.get(0));
	assertEquals("01", list.get(1));
	
	Thread.sleep(expiry * 1000 + 100);
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "03"));
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "04"));
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "03"));
	list = LISTUTIL.get(KEY);
	assertEquals(2, list.size());
	assertEquals("03", list.get(0));
	assertEquals("04", list.get(1));
	
	Thread.sleep(expiry * 1000 + 100);
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "05"));
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "06"));
	list = LISTUTIL.get(KEY);
	assertEquals(2, list.size());
	assertEquals("06", list.get(0));
	assertEquals("05", list.get(1));
    }

    public void testDeleteAndPrepend09() throws Exception {
	long expiry = 1;
	LISTUTIL = new StringListWrapper(CLIENT, true, expiry);
	KEY = KEY_PREFIX + "testDeleteAndPrepend09";
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "01"));
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "02"));
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "03"));
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "04"));
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "02"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(4, list.size());
	assertEquals("02", list.get(0));
	assertEquals("04", list.get(1));
	assertEquals("03", list.get(2));
	assertEquals("01", list.get(3));

	Thread.sleep(expiry * 1000 + 100);
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "02"));
	list = LISTUTIL.get(KEY);
	assertEquals(1, list.size());
	assertEquals("02", list.get(0));
    }

    public void testDeleteAndPrepend10() throws Exception {
	int size = 3;
	long expiry = 1;
	LISTUTIL = new StringListWrapper(CLIENT, true, size, expiry);
	KEY = KEY_PREFIX + "testDeleteAndPrepend10";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertFalse(LISTUTIL.append(KEY, "04"));
	assertFalse(LISTUTIL.append(KEY, "05"));

	List<String> list = LISTUTIL.get(KEY);
	assertEquals(3, list.size());
	assertEquals("01", list.get(0));
	assertEquals("02", list.get(1));
	assertEquals("03", list.get(2));

	Thread.sleep(expiry * 1000 + 100);
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "02"));
	list = LISTUTIL.get(KEY);
	assertEquals(1, list.size());
	assertEquals("02", new String(list.get(0)));
    }

    public void testDeleteAndPrepend11() throws Exception {
	int size = 3;
	long expiry = 1;
	LISTUTIL = new StringListWrapper(CLIENT, true, size, expiry);
	KEY = KEY_PREFIX + "testDeleteAndPrepend11";
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "01"));
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "02"));
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "03"));
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "04"));
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "02"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(3, list.size());
	assertEquals("02", list.get(0));
	assertEquals("04", list.get(1));
	assertEquals("03", list.get(2));

	Thread.sleep(expiry * 1000 + 100);
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "02"));
	assertTrue(LISTUTIL.deleteAndPrepend(KEY, "03"));
	list = LISTUTIL.get(KEY);
	assertEquals(2, list.size());
	assertEquals("03", list.get(0));
	assertEquals("02", list.get(1));
    }

    public void testGet01() throws Exception {
	KEY = KEY_PREFIX + "testGet01";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertTrue(LISTUTIL.append(KEY, "04"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(4, list.size());
	assertEquals("01", list.get(0));
	assertEquals("02", list.get(1));
	assertEquals("03", list.get(2));
	assertEquals("04", list.get(3));
    }

    public void testGet02() throws Exception {
	KEY = KEY_PREFIX + "testGet02";
	List<String> list = LISTUTIL.get(KEY);
	assertTrue(list != null);
	assertTrue(list.size() == 0);
    }

    public void testGet03() throws Exception {
	KEY = KEY_PREFIX + "testGet03";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertTrue(LISTUTIL.append(KEY, "04"));
	List<String> list = LISTUTIL.get(KEY, 1, 2);
	assertEquals(2, list.size());
	assertEquals("02", list.get(0));
	assertEquals("03", list.get(1));
    }

    public void testGet04() throws Exception {
	KEY = KEY_PREFIX + "testGet04";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertTrue(LISTUTIL.append(KEY, "04"));
	List<String> list = LISTUTIL.get(KEY, 2, 5);
	assertEquals(2, list.size());
	assertEquals("03", list.get(0));
	assertEquals("04", list.get(1));
    }

    public void testGet05() throws Exception {
	KEY = KEY_PREFIX + "testGet05";
	try {
	    LISTUTIL.get(KEY, -1, -1);
	    fail();
	} catch (Exception e) {
	    // assertTrue(e instanceof IndexOutOfBoundsException);
	    assertTrue(e instanceof IllegalArgumentException);
	}
    }

    public void testGet06() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testGet06";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertFalse(LISTUTIL.append(KEY, "04"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(3, list.size());
	assertEquals("01", list.get(0));
	assertEquals("02", list.get(1));
	assertEquals("03", list.get(2));
    }

    public void testGet07() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testGet07";
	List<String> list = LISTUTIL.get(KEY);
	assertTrue(list != null);
	assertEquals(0, list.size());
    }

    public void testGet08() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testGet08";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertFalse(LISTUTIL.append(KEY, "04"));
	List<String> list = LISTUTIL.get(KEY, 1, 2);
	assertEquals(2, list.size());
	assertEquals("02", list.get(0));
	assertEquals("03", list.get(1));
    }

    public void testGet09() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testGet09";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertFalse(LISTUTIL.append(KEY, "04"));
	List<String> list = LISTUTIL.get(KEY, 2, 5);
	assertEquals(1, list.size());
	assertEquals("03", list.get(0));
    }

    public void testGet10() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testGet10";
	try {
	    LISTUTIL.get(KEY, -1, -1);
	    fail();
	} catch (Exception e) {
	    // assertTrue(e instanceof IndexOutOfBoundsException);
	    assertTrue(e instanceof IllegalArgumentException);
	}
    }

    public void testPrepend01() throws Exception {
	KEY = KEY_PREFIX + "testPrepend01";
	assertTrue(LISTUTIL.prepend(KEY, "01"));
	assertTrue(LISTUTIL.prepend(KEY, "02"));
	assertTrue(LISTUTIL.prepend(KEY, "03"));
	assertTrue(LISTUTIL.prepend(KEY, "04"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(4, list.size());
	assertEquals("04", list.get(0));
	assertEquals("03", list.get(1));
	assertEquals("02", list.get(2));
	assertEquals("01", list.get(3));
    }

    public void testPrepend02() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testPrepend02";
	assertTrue(LISTUTIL.prepend(KEY, "01"));
	assertTrue(LISTUTIL.prepend(KEY, "02"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(2, list.size());
	assertEquals("02", list.get(0));
	assertEquals("01", list.get(1));
    }

    public void testPrepend03() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testPrepend03";
	assertTrue(LISTUTIL.prepend(KEY, "01"));
	assertTrue(LISTUTIL.prepend(KEY, "02"));
	assertTrue(LISTUTIL.prepend(KEY, "03"));
	assertTrue(LISTUTIL.prepend(KEY, "04"));
	List<String> list = LISTUTIL.get(KEY);
	assertEquals(3, list.size());
	assertEquals("04", list.get(0));
	assertEquals("03", list.get(1));
	assertEquals("02", list.get(2));
    }

    public void testSize01() throws Exception {
	KEY = KEY_PREFIX + "testSize01";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertTrue(LISTUTIL.append(KEY, "04"));
	assertEquals(4, LISTUTIL.size(KEY));
    }

    public void testSize02() throws Exception {
	KEY = KEY_PREFIX + "testSize01";
	assertEquals(0, LISTUTIL.size(KEY));
    }

    public void testSize03() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testSize03";
	assertTrue(LISTUTIL.append(KEY, "01"));
	assertTrue(LISTUTIL.append(KEY, "02"));
	assertTrue(LISTUTIL.append(KEY, "03"));
	assertFalse(LISTUTIL.append(KEY, "04"));
	assertEquals(3, LISTUTIL.size(KEY));
    }

    public void testSize04() throws Exception {
	LISTUTIL = new StringListWrapper(CLIENT, true, 3);
	KEY = KEY_PREFIX + "testSize04";
	assertEquals(0, LISTUTIL.size(KEY));
    }

}
