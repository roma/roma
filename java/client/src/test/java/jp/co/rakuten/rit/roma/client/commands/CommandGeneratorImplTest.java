package jp.co.rakuten.rit.roma.client.commands;

import java.io.IOException;

import jp.co.rakuten.rit.roma.client.Connection;
import jp.co.rakuten.rit.roma.client.ConnectionPool;
import jp.co.rakuten.rit.roma.client.Node;
import jp.co.rakuten.rit.roma.client.command.AbstractCommand;
import jp.co.rakuten.rit.roma.client.command.AbstractCommandFilter;
import jp.co.rakuten.rit.roma.client.command.Command;
import jp.co.rakuten.rit.roma.client.command.CommandContext;
import jp.co.rakuten.rit.roma.client.command.CommandException;
import junit.framework.TestCase;

public class CommandGeneratorImplTest extends TestCase {
    
    public void testDummy() {
	assertTrue(true);
    }

    public void XtestCreateCommand() throws Exception {
	TimeoutFilter.timeout = 10;
	CommandContext context = new CommandContext();
	context.put(CommandContext.CONNECTION_POOL, new MockConnectionPool());

	CommandGeneratorImpl gen = new CommandGeneratorImpl();
	gen.createCommand(1, TestCommand.class, new Class[] {
		FailOverFilter.class, TimeoutFilter.class });
	Command command = gen.getCommand(1);
	command.execute(context);
    }

    public static class TestCommand extends AbstractCommand {

	@Override
	public boolean execute(CommandContext context) throws CommandException {
	    try {
		Thread.sleep(100);
	    } catch (Exception e) {
		e.printStackTrace();
	    }
	    System.out.println("execute");
	    return false;
	}
    }

    public static class MockConnectionPool implements ConnectionPool {

	public Connection get(Node node) throws IOException {
	    // TODO Auto-generated method stub
	    return null;
	}

	public void put(Node node, Connection conn) throws IOException {
	    // TODO Auto-generated method stub

	}

	public void closeAll() {
	    // TODO Auto-generated method stub

	}

	public void delete(Node node) {
	    // TODO Auto-generated method stub

	}

    }

}
