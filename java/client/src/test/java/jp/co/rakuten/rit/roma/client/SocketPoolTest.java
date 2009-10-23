package jp.co.rakuten.rit.roma.client;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import junit.framework.TestCase;

public class SocketPoolTest extends TestCase {

    private Logger log = Logger.getLogger(SocketPoolTest.class.getName());
    
    public void testDummy() {
	assertTrue(true);
    }

	public void XtestGet() throws Exception {

		final AtomicInteger connectionNum = new AtomicInteger(0);
		final AtomicInteger count = new AtomicInteger(0);

		new Thread() {

			@Override
			public void run() {
				try {
					ServerSocket serverSocket = new ServerSocket(10080);
					while (true) {
						Socket socket = serverSocket.accept();
						log.info("[" + connectionNum.incrementAndGet()
								+ "]Accept " + socket.getRemoteSocketAddress());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}.start();

		final SocketPool pool = new SocketPool("localhost", 10080, 3);

		for (int i = 0; i < 6; i++) {
			new Thread() {
				@Override
				public void run() {
					try {
						Socket sock = pool.get();
						sock.getOutputStream().write(
								new String("test").getBytes());
						// sock.getOutputStream().close();

						log.info("send");
						pool.put(sock);
						Thread.sleep(3 * 1000);

						count.incrementAndGet();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}.start();
		}

		Thread.sleep(10 * 1000);
		assertEquals(6, count.get());
		assertEquals(3, connectionNum.get());
	}
}
