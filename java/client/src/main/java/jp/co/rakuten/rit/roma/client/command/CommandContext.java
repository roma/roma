package jp.co.rakuten.rit.roma.client.command;

import java.util.HashMap;

/**
 * 
 */
public class CommandContext extends HashMap<String, Object> {

    private static final long serialVersionUID = 3317922575242590794L;
    public static final String COMMAND_ID = "COMMAND_ID";
    public static final String RESULT = "RESULT";
    public static final String CONNECTION = "CONNECTION";
    public static final String KEY = "KEY";
    public static final String HASH = "HASH";
    public static final String VALUE = "VALUE";
    public static final String EXPIRY = "EXPIRY";
    public static final String EXCEPTION = "EXCEPTION";
    public static final String STRING_DATA = "STRING_DATA";
    public static final String NODE = "NODE";
    public static final String ROUTING_TABLE = "ROUTINGTABLE";
    public static final String CONNECTION_POOL = "CONNECTIONPOOL";

    public CommandContext() {
        super();
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
