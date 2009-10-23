package jp.co.rakuten.rit.roma.client;

import jp.co.rakuten.rit.roma.client.command.CommandGenerator;
import jp.co.rakuten.rit.roma.client.commands.FailOverFilter;
import jp.co.rakuten.rit.roma.client.commands.TimeoutFilter;
import jp.co.rakuten.rit.roma.client.routing.RoutingTable;

/**
 * 
 */
public abstract class AbstractRomaClient implements RomaClient {
    protected ConnectionPool connPool;

    protected RoutingTable routingTable;

    protected CommandGenerator commandGenerator;

    public final void setConnectionPool(ConnectionPool pool) {
	this.connPool = pool;
    }

    public final ConnectionPool getConnectionPool() {
	return connPool;
    }

    public final void setRoutingTable(RoutingTable routingTable) {
	this.routingTable = routingTable;
    }

    public final RoutingTable getRoutingTable() {
	return routingTable;
    }

    public final void setCommandGenerator(CommandGenerator commandGenerator) {
	this.commandGenerator = commandGenerator;
    }

    public final CommandGenerator getCommandGenerator() {
	return commandGenerator;
    }

    public final void setTimeout(long timeout) {
	TimeoutFilter.timeout = timeout;
    }

    public final long getTimeout() {
	return TimeoutFilter.timeout;
    }
    
    public final void setNumOfThreads(int num) {
	TimeoutFilter.numOfThreads = num;
    }
    
    public final int getNumOfThreads() {
	return TimeoutFilter.numOfThreads;
    }

    public void setRetryCount(int retryCount) {
	FailOverFilter.retryThreshold = retryCount;
    }

    public int getRetryCount() {
	return FailOverFilter.retryThreshold;
    }

    public void setRetrySleepTime(long sleepTime) {
	FailOverFilter.sleepPeriod = sleepTime;
    }
    
    public long getRetrySleepTime() {
	return FailOverFilter.sleepPeriod;
    }

    
}
