package jp.co.rakuten.rit.roma.client.util;

import java.util.List;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.util.StringListWrapper.Entry;

/**
 * 
 */
interface StringListAdaptor {

    void setListSize(int listSize);

    int getListSize();
    
    void setExpiry(long expiry);
    
    long getExpiry();

    void setStringWrapper(StringWrapper wrapper);

    StringWrapper getStringWrapper();

    boolean append(String key, String value) throws ClientException;

    void deleteList(String key) throws ClientException;

    boolean delete(String key, int index) throws ClientException;

    boolean delete(String key, String value) throws ClientException;

    boolean deleteAndAppend(String key, String value) throws ClientException;

    boolean deleteAndPrepend(String key, String value) throws ClientException;

    List<String> get(String key) throws ClientException;

    List<Entry> getEntries(String key) throws ClientException;

    List<String> get(String key, int begin, int len) throws ClientException;

    List<Entry> getEntries(String key, int begin, int len)
	    throws ClientException;

    boolean prepend(String key, String value) throws ClientException;

    int size(String key) throws ClientException;
}
