import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import jp.co.rakuten.rit.roma.client.AllTests;
import jp.co.rakuten.rit.roma.client.Node;
import jp.co.rakuten.rit.roma.client.RomaClient;
import jp.co.rakuten.rit.roma.client.RomaClientFactory;
import jp.co.rakuten.rit.roma.client.commands.TimeoutException;
import jp.co.rakuten.rit.roma.client.util.StringListWrapper;
import junit.framework.TestCase;

public class PerfTest3 extends TestCase {
    private static String NODE_ID = AllTests.NODE_ID;

    private static String KEY_PREFIX1 = PerfTest3.class.getName() + "1";

    private static String KEY_PREFIX2 = PerfTest3.class.getName() + "2";

    private static String KEY_PREFIX3 = PerfTest3.class.getName() + "3";

    private static RomaClient CLIENT = null;

    private static StringListWrapper LISTUTIL2 = null;

    //public static int BIG_LOOP_COUNT = 100;

    public static int SMALL_LOOP_COUNT = 1000;

    public static int SIZE_OF_DATA = 30;

    public static int NUM_OF_THREADS = 5;

    public static long PERIOD_OF_SLEEP = 1;

    public static long PERIOD_OF_TIMEOUT = 5000;

    public PerfTest3() {
	super();
    }

    public void setUp() throws Exception {
	RomaClientFactory factory = RomaClientFactory.getInstance();
	CLIENT = factory.newRomaClient(new Properties());
	List<Node> nodes = new ArrayList<Node>();
	CLIENT.setNumOfThreads(NUM_OF_THREADS);
	nodes.add(Node.create(NODE_ID));
	LISTUTIL2 = new StringListWrapper(CLIENT, true, 15);
	CLIENT.open(nodes);
	CLIENT.setTimeout(PERIOD_OF_TIMEOUT);
	//CLIENT.setNumOfThreads(100);
    }

    public void tearDown() throws Exception {
	LISTUTIL2 = null;
	CLIENT.close();
	CLIENT = null;
    }

    public void testDummy() {
	assertTrue(true);
    }

    public void XtestDeleteAndPrependLoop01() throws Exception {
	big_loop();
    }

    public void XtestDeleteAndPrependLoop02() throws Exception {
	Thread[] threads = new Thread[NUM_OF_THREADS];
	for (int i = 0; i < threads.length; ++i) {
	    threads[i] = new Thread() {
		@Override
		public void run() {
		    try {
			big_loop();
		    } catch (Exception e) {
			e.printStackTrace();
		    }
		}
	    };
	}
	for (int i = 0; i < threads.length; ++i) {
	    threads[i].start();
	}

	while (true) {
	    Thread.sleep(1000);
	}
    }

    private void big_loop() throws Exception {
	int count = 0;
	//while (count < BIG_LOOP_COUNT) {
	while (true) {
	    small_loop(count);
	    count++;
	}
    }

    private void small_loop(int big_count) throws Exception {
	int count = 0;
	int count_threshold = 0;
	int count_threshold1 = 0;
	long count_max = 0;
	long count_min = 100000;
	long time0 = System.currentTimeMillis();
	while (count < SMALL_LOOP_COUNT) {
	    try {
		int[] rand = new int[5];
		for (int i = 0; i < rand.length; ++i) {
		    rand[i] = (int) (Math.random() * 20000);
		}
		int user = (int) (Math.random() * 5000000);
		int hotel = (int) (Math.random() * 20000);
		long time = System.currentTimeMillis();
		small_loop0(rand, user, hotel);
		time = System.currentTimeMillis() - time;
		if (time > PERIOD_OF_TIMEOUT) {
		    count_threshold++;
		}
		if (time > count_max) {
		    count_max = time;
		}
		if (time < count_min) {
		    count_min = time;
		}
	    } catch (TimeoutException e) {
		count_threshold1++;
		System.out.println(e.getMessage());
		//e.printStackTrace();
	    } catch (Exception e) {
		e.printStackTrace();
		throw e;
	    } finally {
		//Thread.sleep(PERIOD_OF_SLEEP);
		count++;
	    }
	}
	time0 = System.currentTimeMillis() - time0;

	StringBuilder sb = new StringBuilder();
	sb.append("qps: ")
		.append((int) (((double) (SMALL_LOOP_COUNT * 1000)) / time0))
		.append(" ")
		.append("(timeout count: ")
		.append(count_threshold)
		.append(", ")
		.append(count_threshold1).append(")")
		.append(" max = ")
		.append(count_max / 1000)
		.append(", min = ")
		.append(count_min / 1000);
	System.out.println(sb.toString());
	count_min = 0;
	count_max = 0;
    }

    private void small_loop0(int[] rand, int user, int hotel) throws Exception {
	String userID = "user:" + user;
	LISTUTIL2.get(userID, 0, 15);
	for (int i = 0; i < rand.length; ++i) {
	    CLIENT.get(new Integer(rand[i]).toString());
	}
	LISTUTIL2.deleteAndPrepend(userID, DUMMY_PREFIX + hotel);
    }

    private static final char A = 'b';

    private static final String DUMMY_PREFIX = makeDummyPrefix();

    private static String makeDummyPrefix() {
	StringBuilder sb = new StringBuilder();
	for (int i = 0; i < SIZE_OF_DATA; ++i) {
	    sb.append(A);
	    //sb.append(i);
	}
	//sb.append("::");
	return sb.toString();
    }

    public static void main(final String[] args) throws Exception {
	PerfTest3 test = new PerfTest3();
	test.setUp();
	//test.testDeleteAndPrependLoop02();
	//test.testDeleteAndPrependLoop01();
	test.tearDown();
    }
}