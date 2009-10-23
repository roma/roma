package jp.co.rakuten.rit.roma.client;

import java.io.IOException;

/**
 * 
 */
public interface ConnectionPool {

    public Connection get(Node node) throws IOException;

    public void put(Node node, Connection conn) throws IOException;

    public void delete(Node node);

    public void closeAll();

}
