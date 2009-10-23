package jp.co.rakuten.rit.roma.client;

/**
 * 
 */
public final class Config {

    public static final String CONNECTION_POOL_SIZE = "connection_pool.size";

    public static final String DEFAULT_CONNECTION_POOL_SIZE = "30";

    public static final String TIMEOUT_PERIOD = "timeout.period";

    public static final String DEFAULT_TIMEOUT_PERIOD = "5000";

    public static final String NUM_OF_THREADS = "timeout.threads.num";

    public static final String DEFAULT_NUM_OF_THREADS = "200";
    
    public static final String RETRY_THRESHOLD = "retry.threshold";
    
    public static final String DEFAULT_RETRY_THRESHOLD = "10";
    
    public static final String RETRY_SLEEP_TIME = "retry.timeout";
    
    public static final String DEFAULT_RETRY_SLEEP_TIME = "100";
}
