package jp.co.rakuten.rit.roma.client;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import jp.co.rakuten.rit.roma.client.command.CommandContext;
import jp.co.rakuten.rit.roma.client.command.CommandException;
import jp.co.rakuten.rit.roma.client.command.Command;
import jp.co.rakuten.rit.roma.client.commands.CommandID;
import jp.co.rakuten.rit.roma.client.commands.TimeoutFilter;

/**
 * ROMA client to ROMA, which is a cluster of ROMA instances.  
 * The basic usage is written on {@link jp.co.rakuten.rit.roma.client.RomaClientFactory}.    
 * 
 * @version 0.3.5
 */
public class RomaClientImpl extends AbstractRomaClient {

    protected boolean opened = false;

    public RomaClientImpl() {
    }

    /**
     * @see RomaClient#isOpen()
     */
    public synchronized boolean isOpen() {
        return opened;
    }

    /**
     * @see RomaClient#open(Node)
     */
    public synchronized void open(final Node node) throws ClientException {
        List<Node> initNodes = new ArrayList<Node>();
        initNodes.add(node);
        open(initNodes);
    }

    static class ShutdownHookThread extends Thread {

        public void run() {
            //System.out.println("Call Shutdown hook");
            TimeoutFilter.shutdown();
        }
    }

    /**
     * @see RomaClient#open(List)
     */
    public synchronized void open(final List<Node> nodes)
            throws ClientException {
        if (nodes == null) {
            throw new IllegalArgumentException();
        }
        if (nodes.size() == 0) {
            throw new IllegalArgumentException();
        }

        // connect to ROMA and get the routing info via a network
        routingdump(nodes);
        if (!routingTable.enableLoop()) {
            routingTable.startLoop();
            opened = true;
        }
        Runtime r = Runtime.getRuntime();
        r.addShutdownHook(new ShutdownHookThread());
    }

    /**
     * @see RomaClient#close()
     */
    public synchronized void close() throws ClientException {
        if (!opened) {
            return;
        }
        routingTable.stopLoop();
        routingTable.clear();
        routingTable = null;
        TimeoutFilter.shutdown();
        opened = false;
    }

    public void routingdump(List<Node> nodes) throws ClientException {
        if (nodes == null) {
            throw new IllegalArgumentException();
        }
        if (nodes.size() == 0) {
            throw new IllegalArgumentException();
        }

        Iterator<Node> iter = nodes.iterator();
        List<Object> list = null;
        List<String> err = new ArrayList<String>();
        Throwable t = null;
        while (iter.hasNext()) {
            Node node = null;
            try {
                node = iter.next();
                list = routingdump(node);
                if (list != null) {
                    routingTable.init(list);
                    t = null;
                    break;
                }
            } catch (ClientException e) {
                err.add(e.getMessage());
                err.add(node.toString());
                t = e.getCause();
            }
        }
        if (t != null) {
            throw new ClientException(err.toString(), t);
        }
        if (list == null) {
            throw new ClientException("fatal error");
        }
    }

    /**
     * @see RomaClient#routingdump(Node)
     */
    @SuppressWarnings("unchecked")
    public List<Object> routingdump(Node node) throws ClientException {
        if (node == null) {
            throw new NullPointerException();
        }

        try {
            CommandContext context = new CommandContext();
            context.put(CommandContext.NODE, node);
            context.put(CommandContext.CONNECTION_POOL, connPool);
            context.put(CommandContext.ROUTING_TABLE, routingTable);
            context.put(CommandContext.COMMAND_ID, CommandID.ROUTING_DUMP);
            Command command = commandGenerator.getCommand(CommandID.ROUTING_DUMP);
            boolean ret = command.execute(context);
            if (ret) {
                return (List<Object>) context.get(CommandContext.RESULT);
            } else {
                return null;
            }
        } catch (CommandException e) {
            throw toClientException(e);
        }
    }

    /**
     * @see RomaClient#routingmht(Node)
     */
    public String routingmht(Node node) throws ClientException {
        if (node == null) {
            throw new NullPointerException();
        }

        try {
            CommandContext context = new CommandContext();
            context.put(CommandContext.NODE, node);
            context.put(CommandContext.CONNECTION_POOL, connPool);
            context.put(CommandContext.ROUTING_TABLE, routingTable);
            context.put(CommandContext.COMMAND_ID, CommandID.ROUTING_MKLHASH);
            Command command = commandGenerator.getCommand(CommandID.ROUTING_MKLHASH);
            boolean ret = exec(command, context);
            if (ret) {
                String s = (String) context.get(CommandContext.RESULT);
                if (s.equals("")) {
                    return null;
                } else {
                    return s;
                }
            } else {
                return null;
            }
        } catch (CommandException e) {
            throw toClientException(e);
        }
    }

    /**
     * @see RomaClient#nodelist()
     */
    public List<String> nodelist() throws ClientException {
        List<Node> nodes = routingTable.getPhysicalNodes();
        if (nodes == null) {
            throw new BadRoutingTableFormatException("nodes is null.");
        }
        List<String> ret = new ArrayList<String>();
        for (Iterator<Node> iter = nodes.iterator(); iter.hasNext();) {
            ret.add(iter.next().toString());
        }
        return ret;
    }

    /**
     * @see RomaClient#put(String, byte[])
     */
    public boolean put(String key, byte[] value) throws ClientException {
        return put(key, value, new Date(0));
    }

    /**
     * @see RomaClient#put(String, byte[], Date)
     */
    public boolean put(String key, byte[] value, Date expiry)
            throws ClientException {
        return update(CommandID.SET, key, value, expiry);
    }

    /**
     * @see RomaClient#append(String, byte[])
     */
    public boolean append(String key, byte[] value) throws ClientException {
        return append(key, value, new Date(0));
    }

    /**
     * @see RomaClient#append(String, byte[], Date)
     */
    public boolean append(String key, byte[] value, Date expiry)
            throws ClientException {
        return update(CommandID.APPEND, key, value, expiry);
    }

    /**
     * @see RomaClient#prepend(String, byte[])
     */
    public boolean prepend(String key, byte[] value) throws ClientException {
        return prepend(key, value, new Date(0));
    }

    /**
     * @see RomaClient#prepend(String, byte[], Date)
     */
    public boolean prepend(String key, byte[] value, Date expiry)
            throws ClientException {
        return update(CommandID.PREPEND, key, value, expiry);
    }

    protected boolean update(int commandID, String key, byte[] value,
            Date expiry) throws ClientException {
        CommandContext context = new CommandContext();
        try {
            context.put(CommandContext.CONNECTION_POOL, connPool);
            context.put(CommandContext.ROUTING_TABLE, routingTable);
            context.put(CommandContext.KEY, key);
            context.put(CommandContext.VALUE, value);
            context.put(CommandContext.EXPIRY, expiry);
            context.put(CommandContext.COMMAND_ID, commandID);
            Command command = commandGenerator.getCommand(commandID);
            return exec(command, context);
        } catch (CommandException e) {
            throw toClientException(e);
        }
    }

    /**
     * @see RomaClient#get(String)
     */
    public byte[] get(String key) throws ClientException {
        return (byte[])get(CommandID.GET, key);
    }
    
    @SuppressWarnings("unchecked")
	public Map<String, byte[]> gets(List<String> keys) throws ClientException {
    	return (Map<String, byte[]>)gets(CommandID.GETS, keys);
    }

    protected byte[] get(int commandID, String key) throws ClientException {
        CommandContext context = new CommandContext();
        try {
            context.put(CommandContext.CONNECTION_POOL, connPool);
            context.put(CommandContext.ROUTING_TABLE, routingTable);
            context.put(CommandContext.KEY, key);
            context.put(CommandContext.COMMAND_ID, commandID);
            Command command = commandGenerator.getCommand(commandID);
            boolean ret = exec(command, context);
            if (ret) {
                return (byte[]) context.get(CommandContext.RESULT);
            } else {
                return null;
            }
        } catch (CommandException e) {
            throw toClientException(e);
        }
    }
    
    protected Object gets(int commandID, List<String> keys)
    		throws ClientException {
    	if (keys.size() == 0) {
    		return new HashMap<String, byte[]>();
    	}
    	
        CommandContext context = new CommandContext();
        try {
            context.put(CommandContext.CONNECTION_POOL, connPool);
            context.put(CommandContext.ROUTING_TABLE, routingTable);
            context.put(CommandContext.KEY, keys.get(0));
            context.put(CommandContext.KEYS, keys);
            context.put(CommandContext.COMMAND_ID, commandID);
            Command command = commandGenerator.getCommand(commandID);
            boolean ret = exec(command, context);
            if (ret) {
            	return context.get(CommandContext.RESULT);
            } else {
                return null;
            }
        } catch (CommandException e) {
            throw toClientException(e);
        }
    }

    /**
     * @see RomaClient#delete(String)
     */
    public boolean delete(String key) throws ClientException {
        return delete(CommandID.DELETE, key);
    }

    protected boolean delete(int commandID, String key) throws ClientException {
        CommandContext context = new CommandContext();
        try {
            context.put(CommandContext.CONNECTION_POOL, connPool);
            context.put(CommandContext.ROUTING_TABLE, routingTable);
            context.put(CommandContext.KEY, key);
            context.put(CommandContext.COMMAND_ID, commandID);
            Command command = commandGenerator.getCommand(commandID);
            return exec(command, context);
        } catch (CommandException e) {
            throw toClientException(e);
        }
    }

    /**
     * @see RomaClient#incr(String, int)
     */
    public BigInteger incr(String key, int count) throws ClientException {
        return incrAndDecr(CommandID.INCREMENT, key, count);
    }

    /**
     * @see RomaClient#decr(String, int)
     */
    public BigInteger decr(String key, int count) throws ClientException {
        return incrAndDecr(CommandID.DECREMENT, key, count);
    }

    protected BigInteger incrAndDecr(int commandID, String key, int count)
            throws ClientException {
        CommandContext context = new CommandContext();
        try {
            context.put(CommandContext.CONNECTION_POOL, connPool);
            context.put(CommandContext.ROUTING_TABLE, routingTable);
            context.put(CommandContext.KEY, key);
            context.put(CommandContext.VALUE, new Integer(count));
            context.put(CommandContext.COMMAND_ID, commandID);
            Command command = commandGenerator.getCommand(commandID);
            boolean ret = exec(command, context);
            if (ret) {
                return (BigInteger) context.get(CommandContext.RESULT);
            } else {
                return new BigInteger("-1");
            }
        } catch (CommandException e) {
            throw toClientException(e);
        }
    }

    public boolean exec(Command command, CommandContext context)
            throws CommandException {
        return command.execute(context);
    }

    public static ClientException toClientException(CommandException e)
            throws ClientException {
        Throwable t = e.getCause();
        if (t instanceof ClientException) {
            return (ClientException) t;
        } else {
            return new ClientException(e.getCause());
        }
    }
}
