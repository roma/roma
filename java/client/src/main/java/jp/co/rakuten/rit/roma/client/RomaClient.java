package jp.co.rakuten.rit.roma.client;

import java.math.BigInteger;
import java.util.Date;
import java.util.List;

import jp.co.rakuten.rit.roma.client.command.Command;
import jp.co.rakuten.rit.roma.client.command.CommandContext;
import jp.co.rakuten.rit.roma.client.command.CommandException;
import jp.co.rakuten.rit.roma.client.command.CommandGenerator;
import jp.co.rakuten.rit.roma.client.routing.RoutingTable;

/**
 * This interface is provided as an interface for intaracting
 * with ROMA.  
 * 
 * The basic usage is written on {@link jp.co.rakuten.rit.roma.client.RomaClientFactory}.    
 * 
 * @version 0.3.5
 */
public interface RomaClient {

    void setConnectionPool(ConnectionPool pool);

    ConnectionPool getConnectionPool();

    void setRoutingTable(RoutingTable routingTable);

    RoutingTable getRoutingTable();

    void setCommandGenerator(CommandGenerator commandGenerator);

    CommandGenerator getCommandGenerator();

    void setTimeout(long timeout);

    long getTimeout();

    void setNumOfThreads(int num);

    int getNumOfThreads();

    void setRetryCount(int retryCount);

    int getRetryCount();

    void setRetrySleepTime(long sleepTime);

    long getRetrySleepTime();

    boolean isOpen();

    List<Object> routingdump(Node node) throws ClientException;

    /**
     * Return the names of ROMA processes.
     * 
     * @return
     * @throws ClientException
     */
    List<String> nodelist() throws ClientException;

    String routingmht(Node node) throws ClientException;

    /**
     * Open a connection with a ROMA using the <code>Node</code> object.
     * 
     * @param node
     * @throws jp.co.rakuten.rit.roma.client.ClientException
     */
    void open(Node node) throws ClientException;

    /**
     * Open a connnection with a ROMA using the <code>Node</code> objects.
     * 
     * @param nodes
     * @throws jp.co.rakuten.rit.roma.client.ClientException
     * @see #open(jp.co.rakuten.rit.roma.client.Node)
     */
    void open(List<Node> nodes) throws ClientException;

    /**
     * Close the connection.
     * 
     * @throws jp.co.rakuten.rit.roma.client.ClientException
     * @see #open(java.lang.String)
     */
    void close() throws ClientException;

    /**
     * Store the value in ROMA.
     * 
     * @param key key to store value
     * @param value value to store
     * @return true, if the value was successfully stored
     */
    boolean put(String key, byte[] value) throws ClientException;

    /**
     * Store the value in ROMA.  
     * 
     * @param key key to store value 
     * @param value value to store
     * @param expiry expire time
     * @return true, if the value was successfully stored
     * @throws ClientException
     */
    boolean put(String key, byte[] value, Date expiry) throws ClientException;

    /**
     * Get a value with a key.  
     * 
     * @param key
     * @return
     * @throws ClientException
     */
    byte[] get(String key) throws ClientException;

    /**
     * Append the given value to the existing value.
     * 
     * @param key
     * @param value
     * @return
     * @throws ClientException
     */
    boolean append(String key, byte[] value) throws ClientException;

    /**
     * Append the given value to the existing value.  
     * 
     * @param key
     * @param value
     * @param expiry expire time
     * @return
     * @throws ClientException
     */
    boolean append(String key, byte[] value, Date expiry)
            throws ClientException;

    /**
     * Prepend the given value to the existing value.  
     * 
     * @param key
     * @param value
     * @return
     * @throws ClientException
     */
    boolean prepend(String key, byte[] value) throws ClientException;

    /**
     * Prepend the given value to the existing value.  
     *  
     * @param key
     * @param value
     * @param expiry expire time
     * @return
     * @throws ClientException
     */
    boolean prepend(String key, byte[] value, Date expiry)
            throws ClientException;

    /**
     * Deletes ta stored value specified by the key
     * 
     * @param key key to store value
     * @return true, if the value was deleted successfully
     */
    boolean delete(String key) throws ClientException;

    /**
     * Increment the counter.  
     * 
     * @param key
     * @param count
     * @return
     * @throws ClientException
     */
    BigInteger incr(String key, int count) throws ClientException;

    /**
     * Decrement the counter.
     * 
     * @param key
     * @param count
     * @return
     * @throws ClientException
     */
    BigInteger decr(String key, int count) throws ClientException;

    boolean exec(Command command, CommandContext context)
            throws CommandException;
}
