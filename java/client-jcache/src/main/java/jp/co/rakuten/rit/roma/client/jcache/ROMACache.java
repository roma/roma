package jp.co.rakuten.rit.roma.client.jcache;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.ClientRuntimeException;
import jp.co.rakuten.rit.roma.client.RomaClient;
import jp.co.rakuten.rit.roma.client.util.ObjectAppender.DataSerialization;
import net.sf.jsr107cache.Cache;
import net.sf.jsr107cache.CacheEntry;
import net.sf.jsr107cache.CacheException;
import net.sf.jsr107cache.CacheListener;
import net.sf.jsr107cache.CacheStatistics;

/**
 * 
 */
public class ROMACache implements Cache {

    private RomaClient client;

    public ROMACache(RomaClient client) {
	this.client = client;
    }

    public Object get(Object key) {
	try {
	    byte[] bytes = client.get((String) key);
	    return DataSerialization.toObject(bytes);
	} catch (ClientException e) {
	    throw new ClientRuntimeException();
	} catch (IOException e) {
	    throw new ClientRuntimeException();
	} catch (ClassNotFoundException e) {
	    throw new ClientRuntimeException();
	}
    }

    public Object put(Object key, Object value) {
	try {
	    return client.put((String) key, DataSerialization
		    .toByteArray(value));
	} catch (ClientException e) {
	    throw new ClientRuntimeException();
	} catch (IOException e) {
	    throw new ClientRuntimeException();
	}
    }

    public Object remove(Object key) {
	try {
	    return client.delete((String) key);
	} catch (ClientException e) {
	    throw new ClientRuntimeException();
	}
    }

    public CacheEntry getCacheEntry(Object key) {
	return new ROMACacheEntry(key, get(key));
    }

    public void clear() {
    }

    public boolean containsKey(Object key) {
	// TODO Auto-generated method stub
	return false;
    }

    public boolean containsValue(Object value) {
	// TODO Auto-generated method stub
	return false;
    }

    public Set entrySet() {
	// TODO Auto-generated method stub
	return null;
    }

    public void evict() {
	// TODO Auto-generated method stub

    }

    public Map getAll(Collection keys) throws CacheException {
	// TODO Auto-generated method stub
	return null;
    }

    public CacheStatistics getCacheStatistics() {
	// TODO Auto-generated method stub
	return null;
    }

    public boolean isEmpty() {
	// TODO Auto-generated method stub
	return false;
    }

    public Set keySet() {
	// TODO Auto-generated method stub
	return null;
    }

    public void load(Object key) throws CacheException {
	// TODO Auto-generated method stub

    }

    public void loadAll(Collection keys) throws CacheException {
	// TODO Auto-generated method stub

    }

    public Object peek(Object key) {
	// TODO Auto-generated method stub
	return null;
    }

    public void putAll(Map t) {
	throw new UnsupportedOperationException();
    }

    public void removeListener(CacheListener listener) {
	throw new UnsupportedOperationException();
    }

    public int size() {
	throw new UnsupportedOperationException();
    }

    public Collection values() {
	throw new UnsupportedOperationException();
    }

    public void addListener(CacheListener listener) {
	throw new UnsupportedOperationException();
    }
}
