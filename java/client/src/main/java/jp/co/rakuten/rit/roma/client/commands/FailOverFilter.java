package jp.co.rakuten.rit.roma.client.commands;

import java.io.IOException;
import java.math.BigInteger;

import jp.co.rakuten.rit.roma.client.BadRoutingTableFormatException;
import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.Config;
import jp.co.rakuten.rit.roma.client.Connection;
import jp.co.rakuten.rit.roma.client.ConnectionPool;
import jp.co.rakuten.rit.roma.client.Node;
import jp.co.rakuten.rit.roma.client.command.AbstractCommandFilter;
import jp.co.rakuten.rit.roma.client.command.Command;
import jp.co.rakuten.rit.roma.client.command.CommandContext;
import jp.co.rakuten.rit.roma.client.command.CommandException;
import jp.co.rakuten.rit.roma.client.routing.RoutingTable;

/**
 * 
 */
public class FailOverFilter extends AbstractCommandFilter {

    public static long sleepPeriod = Long.parseLong(Config.DEFAULT_RETRY_SLEEP_TIME);
    public static int retryThreshold = Integer.parseInt(Config.DEFAULT_RETRY_THRESHOLD);

    public FailOverFilter() {
    }

    @Override
    public boolean aroundExecute(Command command, CommandContext context)
            throws CommandException {
        RoutingTable routingTable = (RoutingTable) context.get(CommandContext.ROUTING_TABLE);
        String key = (String) context.get(CommandContext.KEY);
        if (routingTable == null) {
            throw new CommandException(new BadRoutingTableFormatException(
                    "routing table is null."));
        }
        BigInteger hash = routingTable.getHash(key);
        if (hash == null) {
            throw new CommandException(new BadRoutingTableFormatException(
                    "hash is null."));
        }
        ConnectionPool connPool = (ConnectionPool) context.get(CommandContext.CONNECTION_POOL);
        int retryCount = 0;
        while (true) {
            Throwable t = null;
            try {
                Node node = routingTable.searchNode(key, hash);
                context.put(CommandContext.HASH, hash);
                context.put(CommandContext.NODE, node);
                return command.execute(context);
            } catch (CommandException e) {
                t = e.getCause();
                //System.out.println(t);
            }

            // re-try message-passing or handle an error
            if (t != null) {
                int commandID = (Integer) context.get(CommandContext.COMMAND_ID);
                try {
                    Node node = (Node) context.get(CommandContext.NODE);
                    if (t instanceof IOException || t instanceof TimeoutException) {
                        routingTable.incrFailCount(node);
                        connPool.delete(node);
                    } else if (t instanceof ClientException) {
                        if (t.getCause() instanceof IOException) {
                            routingTable.incrFailCount(node);
                            connPool.delete(node);
                        } else {
                            Connection conn = (Connection) context.get(CommandContext.CONNECTION);
                            try {
                                if (conn != null) {
                                    if (commandID != CommandID.ROUTING_DUMP && commandID != CommandID.ROUTING_MKLHASH) {
                                        connPool.put(node, conn);
                                        context.remove(CommandContext.CONNECTION);
                                    } else {
                                        conn.close();
                                    }
                                    conn = null;
                                }
                            } catch (IOException e) {
                            } // ignore
                        }
                    }
                    Thread.sleep(sleepPeriod);
                } catch (InterruptedException e) { // ignore
                }
                if (retryCount < retryThreshold) {
                    retryCount++;
                } else {
                    throw new CommandException(new RetryOutException());
                }
            }
        }
    }

    @Override
    public void preExecute(CommandContext context) throws CommandException {
    }

    @Override
    public void postExecute(CommandContext context) throws CommandException {
    }
}
