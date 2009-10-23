package jp.co.rakuten.rit.roma.client.commands;

import java.io.IOException;

import jp.co.rakuten.rit.roma.client.Connection;
import jp.co.rakuten.rit.roma.client.ConnectionPool;
import jp.co.rakuten.rit.roma.client.Node;
import jp.co.rakuten.rit.roma.client.command.AbstractCommand;
import jp.co.rakuten.rit.roma.client.command.Command;
import jp.co.rakuten.rit.roma.client.command.CommandContext;
import jp.co.rakuten.rit.roma.client.command.CommandException;
import jp.co.rakuten.rit.roma.client.command.CommandFilter;
import junit.framework.TestCase;

public class TimeoutFilterTest extends TestCase {
    private static long PERIOD_OF_SLEEP;
    
    private static int NUM_OF_THREADS = 10;

    public static class TestCommand extends AbstractCommand {
	public TestCommand() {
	}

	public Command addFilter(CommandFilter filter) {
	    return super.addFilter(filter);
	}

	@Override
	public boolean execute(CommandContext context) {
	    try {
		Thread.sleep(PERIOD_OF_SLEEP);
		return true;
	    } catch (InterruptedException e) { // ignore
	    }
	    return false;
	}
    }

    public static class MockConnectionPool implements ConnectionPool {

	public Connection get(Node node) throws IOException {
	    return null;
	}

	public void put(Node node, Connection conn) throws IOException {
	}

	public void closeAll() {
	}

	public void delete(Node node) {
	}
    }

    public void testAroundExecute01() throws Exception {
	TimeoutFilter.timeout = 100;
	TimeoutFilterTest.PERIOD_OF_SLEEP = 1;
	int commandID = 1000;
	CommandContext context = new CommandContext();
	context.put(CommandContext.CONNECTION_POOL, new MockConnectionPool());
	context.put(CommandContext.COMMAND_ID, commandID);
	CommandGeneratorImpl gen = new CommandGeneratorImpl();
	gen.createCommand(commandID, TestCommand.class,
		new Class[] { TimeoutFilter.class });
	Command command = gen.getCommand(commandID);
	command.execute(context);
    }

    public void testAroundExecute02() throws Exception {
	TimeoutFilter.timeout = 100;
	TimeoutFilterTest.PERIOD_OF_SLEEP = 1000;
	int commandID = 1001;
	CommandContext context = new CommandContext();
	context.put(CommandContext.CONNECTION_POOL, new MockConnectionPool());
	context.put(CommandContext.COMMAND_ID, commandID);
	CommandGeneratorImpl gen = new CommandGeneratorImpl();
	gen.createCommand(commandID, TestCommand.class,
		new Class[] { TimeoutFilter.class });
	Command command = gen.getCommand(commandID);
	try {
	    command.execute(context);
	    fail();
	} catch (Exception e) {
	    assertTrue(e instanceof CommandException);
	    Throwable t = e.getCause();
	    assertTrue(t instanceof TimeoutException);
	}
    }

    public void testAroundExecute03() throws Exception {
	TimeoutFilter.timeout = 100;
	TimeoutFilterTest.PERIOD_OF_SLEEP = 1000;
	final CommandGeneratorImpl gen = new CommandGeneratorImpl();
	final int commandID = 1002;
	gen.createCommand(commandID, TestCommand.class,
		new Class[] { TimeoutFilter.class });

	Thread[] threads = new Thread[NUM_OF_THREADS];
	for (int i = 0; i < threads.length; ++i) {
	    threads[i] = new Thread() {
		@Override
		public void run() {
		    try {
			CommandContext context = new CommandContext();
			context.put(CommandContext.COMMAND_ID, commandID);
			context.put(CommandContext.CONNECTION_POOL,
				new MockConnectionPool());
			Command command = gen.getCommand(commandID);
			assert0(command, context);
		    } catch (Exception e) {
		    }
		}
	    };
	}
	for (int i = 0; i < threads.length; ++i) {
	    threads[i].start();
	}
    }
    
    static boolean assert0(Command command, CommandContext context) {
	try {
	    return command.execute(context);
	} catch (Exception e) {
	    assertTrue(e instanceof CommandException);
	    Throwable t = e.getCause();
	    assertTrue(t instanceof TimeoutException);
	}
	return false;
    }
}
