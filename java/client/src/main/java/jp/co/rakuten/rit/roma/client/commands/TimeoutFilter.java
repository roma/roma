package jp.co.rakuten.rit.roma.client.commands;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import jp.co.rakuten.rit.roma.client.Config;
import jp.co.rakuten.rit.roma.client.Connection;
import jp.co.rakuten.rit.roma.client.ConnectionPool;
import jp.co.rakuten.rit.roma.client.Node;
import jp.co.rakuten.rit.roma.client.command.AbstractCommandFilter;
import jp.co.rakuten.rit.roma.client.command.Command;
import jp.co.rakuten.rit.roma.client.command.CommandContext;
import jp.co.rakuten.rit.roma.client.command.CommandException;

/**
 * 
 */
public class TimeoutFilter extends AbstractCommandFilter {

    // The maximum time to wait (millis)
    public static long timeout = Long.parseLong(Config.DEFAULT_TIMEOUT_PERIOD);

    public static int numOfThreads = Integer.parseInt(Config.DEFAULT_NUM_OF_THREADS);

    private static ExecutorService executor;

    public static void shutdown() {
	if (executor != null) {
	    //executor.shutdown();
	    executor.shutdownNow();
	}
	executor = null;
    }

    public TimeoutFilter() {
    }

    public static class CallableImpl implements Callable<Boolean> {
	private Command command;
	private CommandContext context;

	public CallableImpl(Command command, CommandContext context) {
	    this.command = command;
	    this.context = context;
	}

	public Boolean call() throws Exception {
	    int commandID = (Integer) context.get(CommandContext.COMMAND_ID);
	    Node node = null;
	    ConnectionPool connPool = null;
	    Connection conn = null;
	    try {
		node = (Node) context.get(CommandContext.NODE);
		if (commandID != CommandID.ROUTING_DUMP
			&& commandID != CommandID.ROUTING_MKLHASH) {
		    connPool = (ConnectionPool) context
			    .get(CommandContext.CONNECTION_POOL);
		    conn = connPool.get(node);
		    context.put(CommandContext.CONNECTION, conn);
		} else { // routingdump, routingmkh
		    Socket sock = new Socket(node.getHost(), node.getPort());
		    conn = new Connection(sock);
		    context.put(CommandContext.CONNECTION, conn);
		}
		return command.execute(context);
	    } finally {
		try {
		    if (conn != null) {
			if (commandID != CommandID.ROUTING_DUMP
				&& commandID != CommandID.ROUTING_MKLHASH) {
			    connPool.put(node, conn);
			    context.remove(CommandContext.CONNECTION);
			} else {
			    conn.close();
			}
			conn = null;
		    }
		} catch (IOException e) { // ignore
		}
	    }
	}
    }

    @Override
    public boolean aroundExecute(final Command command,
	    final CommandContext context) throws CommandException {
	int commandID = (Integer) context.get(CommandContext.COMMAND_ID);
	if (executor == null) {
	    executor = Executors.newFixedThreadPool(numOfThreads);
	    // executor = Executors.newSingleThreadExecutor();
	    //executor = Executors.newCachedThreadPool();
	}

	Callable<Boolean> task = new CallableImpl(command, context);
	Future<Boolean> future = executor.submit(task);
	Throwable t = null;
	try {
	    return future.get(timeout, TimeUnit.MILLISECONDS);
	} catch (java.util.concurrent.TimeoutException e) {
	    t = e;
	} catch (CancellationException e) {
	    t = e;
	} catch (ExecutionException e) {
	    t = e.getCause();
	} catch (InterruptedException e) {
	} // ignore

	// error handling
	if (t != null) {
	    if (t instanceof java.util.concurrent.TimeoutException) {
		ConnectionPool connPool = (ConnectionPool) context
			.get(CommandContext.CONNECTION_POOL);
		Connection conn = (Connection) context
			.get(CommandContext.CONNECTION);
		future.cancel(true);
		if (conn != null) {
		    if (commandID != CommandID.ROUTING_DUMP
			    && commandID != CommandID.ROUTING_MKLHASH) {
			Node node = (Node) context.get(CommandContext.NODE);
			connPool.delete(node);
			context.remove(CommandContext.CONNECTION);
		    } else {
			try {
			    conn.close();
			} catch (IOException e) { // ignore
			}
		    }
		    conn = null;
		}
		throw new CommandException(new TimeoutException(t));
	    } else { // otherwise
		if (t instanceof CommandException) {
		    throw (CommandException) t;
		} else {
		    throw new CommandException(t);
		}
	    }
	}
	return false;
    }

    @Override
    public void postExecute(CommandContext context) throws CommandException {
    }

    @Override
    public void preExecute(CommandContext context) throws CommandException {
    }
}
