package jp.co.rakuten.rit.roma.client.jcache;

import net.sf.jsr107cache.CacheEntry;

public class ROMACacheEntry implements CacheEntry {

    private Object key;

    private Object value;

    public ROMACacheEntry(Object key, Object value) {
	this.key = key;
	this.value = value;
    }

    public Object getKey() {
	return key;
    }

    public Object getValue() {
	return value;
    }

    public Object setValue(Object arg0) {
	throw new UnsupportedOperationException(
		"ROMA does not support modification of CacheEntries. They are immutable."); // TODO
    }

    public long getCost() {
	return 0;
    }

    public long getCreationTime() {
	// TODO Auto-generated method stub
	return 0;
    }

    public long getExpirationTime() {
	// TODO Auto-generated method stub
	return 0;
    }

    public int getHits() {
	// TODO Auto-generated method stub
	return 0;
    }

    public long getLastAccessTime() {
	// TODO Auto-generated method stub
	return 0;
    }

    public long getLastUpdateTime() {
	// TODO Auto-generated method stub
	return 0;
    }

    public long getVersion() {
	// TODO Auto-generated method stub
	return 0;
    }

    public boolean isValid() {
	// TODO Auto-generated method stub
	return false;
    }

}
