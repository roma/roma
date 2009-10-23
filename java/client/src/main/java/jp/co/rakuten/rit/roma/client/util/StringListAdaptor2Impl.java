package jp.co.rakuten.rit.roma.client.util;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.RomaClient;
import jp.co.rakuten.rit.roma.client.util.StringListWrapper.Entry;
import jp.co.rakuten.rit.roma.client.util.commands.ListCommandID;

/**
 * 
 */
class StringListAdaptor2Impl implements StringListAdaptor {
    protected StringWrapper appender;

    protected RomaClient client;

    protected ListWrapper listUtil;

    StringListAdaptor2Impl(StringWrapper appender) throws ClientException {
	this.appender = appender;
	client = this.appender.getRomaClient();
	listUtil = new ListWrapper(client);
    }

    public void setListSize(int listSize) {
	listUtil.setListSize(listSize);
    }

    public int getListSize() {
	return listUtil.getListSize();
    }

    public void setStringWrapper(StringWrapper appender) {
	this.appender = appender;
    }
    
    public long getExpiry() {
	return listUtil.getExpiry();
    }
    
    public void setExpiry(long expiry) {
	listUtil.setExpiry(expiry);
    }

    public StringWrapper getStringWrapper() {
	return appender;
    }

    public boolean append(String key, String value) throws ClientException {
	try {
	    return listUtil.append(key, value.getBytes(StringUtil.ENCODING));
	} catch (UnsupportedEncodingException e) {
	    throw new ClientException(e);
	}
    }

    public boolean delete(String key, int index) throws ClientException {
	try {
	    return listUtil.updateList(ListCommandID.ALIST_DELETE_AT, key,
		    (new Integer(index)).toString().getBytes(
			    StringUtil.ENCODING));
	} catch (UnsupportedEncodingException e) {
	    throw new ClientException(e);
	}
    }

    public boolean delete(String key, String value) throws ClientException {
	try {
	    return listUtil.delete(key, value.getBytes(StringUtil.ENCODING));
	} catch (UnsupportedEncodingException e) {
	    throw new ClientException(e);
	}
    }

    public boolean deleteAndAppend(String key, String value)
	    throws ClientException {
	try {
	    return listUtil.deleteAndAppend(key, value
		    .getBytes(StringUtil.ENCODING));
	} catch (UnsupportedEncodingException e) {
	    throw new ClientException(e);
	}
    }

    public boolean deleteAndPrepend(String key, String value)
	    throws ClientException {
	try {
	    return listUtil.deleteAndPrepend(key, value
		    .getBytes(StringUtil.ENCODING));
	} catch (UnsupportedEncodingException e) {
	    throw new ClientException(e);
	}
    }

    public void deleteList(String key) throws ClientException {
	try {
	    listUtil.updateList(ListCommandID.ALIST_CLEAR, key, ""
		    .getBytes(StringUtil.ENCODING));
	} catch (UnsupportedEncodingException e) {
	    throw new ClientException(e);
	}
    }

    public boolean prepend(String key, String value) throws ClientException {
	try {
	    return listUtil.prepend(key, value.getBytes(StringUtil.ENCODING));
	} catch (UnsupportedEncodingException e) {
	    throw new ClientException(e);
	}
    }

    private static List<String> toStringList(List<ListWrapper.Entry> input) {
	List<String> output = new ArrayList<String>();
	Iterator<ListWrapper.Entry> iter = input.iterator();
	while (iter.hasNext()) {
	    ListWrapper.Entry e = iter.next();
	    try {
		output.add(new String(e.getValue(), StringUtil.ENCODING));
	    } catch (UnsupportedEncodingException e1) {
		e1.printStackTrace();
	    }
	}
	return output;
    }

    private static List<Entry> toEntryList(List<ListWrapper.Entry> input) {
	List<Entry> output = new ArrayList<Entry>();
	Iterator<ListWrapper.Entry> iter = input.iterator();
	while (iter.hasNext()) {
	    ListWrapper.Entry oldEntry = (ListWrapper.Entry) iter.next();
	    StringListWrapper.Entry newEntry = new StringListWrapper.Entry(
		    oldEntry.getValue(), oldEntry.getTime());
	    output.add(newEntry);
	}
	return output;
    }

    public List<String> get(String key) throws ClientException {
	List<ListWrapper.Entry> list = listUtil.getEntries(key);
	return toStringList(list);
    }

    public List<Entry> getEntries(String key) throws ClientException {
	List<ListWrapper.Entry> list = listUtil.getEntries(key);
	return toEntryList(list);
    }

    public List<String> get(String key, int begin, int len)
	    throws ClientException {
	List<ListWrapper.Entry> list = listUtil.getEntries(key, begin, len);
	return toStringList(list);
    }

    public List<Entry> getEntries(String key, int begin, int len)
	    throws ClientException {
	List<ListWrapper.Entry> list = listUtil.getEntries(key, begin, len);
	return toEntryList(list);
    }

    public int size(String key) throws ClientException {
	return listUtil.size(key);
    }

}
