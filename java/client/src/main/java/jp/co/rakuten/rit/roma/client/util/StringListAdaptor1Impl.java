package jp.co.rakuten.rit.roma.client.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.util.StringListWrapper.Entry;

/**
 * 
 */
class StringListAdaptor1Impl implements StringListAdaptor {

    private static final String SEP = "_"; // "_$_$_";
    private static final String SEP2 = ","; // "_$,$_";
    private int listSize;
    private long expiry;
    protected StringWrapper sWrapper;

    StringListAdaptor1Impl(StringWrapper wrapper) throws ClientException {
        this.sWrapper = wrapper;
    }

    public void setListSize(int listSize) {
        if (listSize < 0) {
            throw new IllegalArgumentException();
        }
        this.listSize = listSize;
    }

    public int getListSize() {
        return listSize;
    }

    public void setExpiry(long expiry) {
        if (expiry < 0) {
            throw new IllegalArgumentException();
        }
        this.expiry = expiry;
    }

    public long getExpiry() {
        return expiry;
    }

    public void setStringWrapper(StringWrapper wrapper) {
        this.sWrapper = wrapper;
    }

    public StringWrapper getStringWrapper() {
        return this.sWrapper;
    }

    public boolean append(String key, String value) throws ClientException {
        List<String> rawList = getRawList(sWrapper, key);
        Date d = new Date();
        rawList.add(value + SEP + d.getTime());
        boolean ret;
        if (getListSize() == 0) {
            ret = true;
        } else {
            ret = getListSize() >= rawList.size();
            if (!ret) {
                rawList = limitListSize(rawList, getListSize());
            }
        }
        return ret && sWrapper.put(key, toString(rawList));
    }

    public void deleteList(String key) throws ClientException {
        sWrapper.delete(key);
    }

    public boolean delete(String key, int index) throws ClientException {
        List<String> rawList = getRawList(sWrapper, key);
        if (rawList.size() <= index) {
            return false;
        }
        if (rawList.remove(index) == null) {
            return false;
        }
        if (rawList.size() == 0) {
            return sWrapper.delete(key);
        } else {
            return sWrapper.put(key, toString(rawList));
        }
    }

    public boolean delete(String key, String value) throws ClientException {
        List<String> rawList = getRawList(sWrapper, key);
        boolean isRemoved = remove(rawList, value);
        boolean ret;
        if (rawList.size() == 0) {
            ret = sWrapper.delete(key);
        } else {
            ret = sWrapper.put(key, toString(rawList));
        }
        return isRemoved && ret;
    }

    public boolean deleteAndAppend(String key, String value)
            throws ClientException {
        List<String> rawList = getRawList(sWrapper, key);
        Date d = new Date();
        if (getExpiry() != 0) {
            rawList = limitExpiry(rawList, d.getTime(), getExpiry());
        }
        remove(rawList, value);
        boolean ret;
        rawList.add(value + SEP + d.getTime());
        if (getListSize() == 0) {
            ret = true;
        } else { // getListSize() != 0
            ret = getListSize() >= rawList.size();
            if (!ret) {
                rawList = limitListSize(rawList, getListSize());
            }
        }
        return ret && sWrapper.put(key, toString(rawList));
    }

    public boolean deleteAndPrepend(String key, String value)
            throws ClientException {
        List<String> rawList = getRawList(sWrapper, key);
        Date d = new Date();
        if (getExpiry() != 0) {
            rawList = limitExpiry(rawList, d.getTime(), getExpiry());
        }
        remove(rawList, value);
        rawList.add(0, value + SEP + d.getTime());
        if (getListSize() != 0 && getListSize() < rawList.size()) {
            rawList = limitListSize(rawList, getListSize());
        }
        return sWrapper.put(key, toString(rawList));
    }

    public List<String> get(String key) throws ClientException {
        List<String> rawList = getRawList(sWrapper, key);
        return toStringList(rawList);
    }

    public List<Entry> getEntries(String key) throws ClientException {
        List<String> rawList = getRawList(sWrapper, key);
        return toEntryList(rawList);
    }

    public List<String> get(String key, int begin, int len)
            throws ClientException {
        if (begin < 0 || len < 0) {
            throw new IllegalArgumentException();
        }
        List<String> rawList = getRawList(sWrapper, key);
        int size = rawList.size();
        if (begin >= size) {
            return new ArrayList<String>();
        } else { // begin < size
            List<String> ret = new ArrayList<String>();
            for (int i = 0; i < len; ++i) {
                int index = i + begin;
                if (index < size) {
                    String rawString = rawList.get(index);
                    ret.add(rawString);
                } else {
                    break;
                }
            }
            return toStringList(ret);
        }
    }

    public List<Entry> getEntries(String key, int begin, int len)
            throws ClientException {
        if (begin < 0 || len < 0) {
            throw new IllegalArgumentException();
        }
        List<String> rawList = getRawList(sWrapper, key);
        int size = rawList.size();
        if (begin >= size) {
            return new ArrayList<Entry>();
        } else { // begin < size
            List<String> ret = new ArrayList<String>();
            for (int i = 0; i < len; ++i) {
                int index = i + begin;
                if (index < size) {
                    String rawString = rawList.get(index);
                    ret.add(rawString);
                } else {
                    break;
                }
            }
            return toEntryList(ret);
        }
    }

    public boolean prepend(String key, String value) throws ClientException {
        List<String> rawList = getRawList(sWrapper, key);
        Date d = new Date();
        rawList.add(0, value + SEP + d.getTime());
        if (getListSize() != 0 && getListSize() < rawList.size()) {
            rawList = limitListSize(rawList, getListSize());
        }
        return sWrapper.put(key, toString(rawList));
    }

    public int size(String key) throws ClientException {
        List<String> list = getRawList(sWrapper, key);
        return list.size();
    }

    private static List<String> getRawList(StringWrapper sWrapper,
            final String key) throws ClientException {
        String s = sWrapper.get(key);
        if (s != null) {
            return toRawStringList(s);
        } else {
            return new ArrayList<String>();
        }
    }

    private static boolean remove(List<String> rawList, String value) {
        String prefix = value + SEP;
        int index = -1;
        Iterator<String> iter = rawList.iterator();
        for (int i = 0; iter.hasNext(); ++i) {
            String v = iter.next();
            if (v.startsWith(prefix)) {
                index = i;
                break;
            }
        }
        if (index != -1) {
            rawList.remove(index);
            return true;
        } else {
            return false;
        }
    }

    private static List<String> limitExpiry(List<String> rawList, long current, long expiry) {
        List<String> newRawList = new ArrayList<String>();
        Iterator<String> iter = rawList.iterator();
        while (iter.hasNext()) {
            String rawData = iter.next();
            Entry e = toEntry(rawData);
            if (current - e.getTime() >= expiry * 1000) {
                break;
            } else {
                newRawList.add(rawData);
            }
        }
        return newRawList;
    }

    private static List<String> limitListSize(List<String> rawList, int size) {
        List<String> newRawList = new ArrayList<String>();
        Iterator<String> iter = rawList.iterator();
        for (int i = 0; i < size; ++i) {
            String e = iter.next();
            newRawList.add(e);
        }
        return newRawList;
    }

    private static String toString(List<String> rawStringList) {
        StringBuilder sb = new StringBuilder();
        Iterator<String> iter = rawStringList.iterator();
        while (iter.hasNext()) {
            sb.append(iter.next()).append(SEP2);
        }
        sb.delete(sb.length() - SEP2.length(), sb.length());
        return sb.toString();
    }

    private static List<String> toRawStringList(String rawString) {
        StringTokenizer t = new StringTokenizer(rawString, SEP2);
        List<String> ret = new ArrayList<String>();
        while (t.hasMoreTokens()) {
            String s = t.nextToken();
            ret.add(s);
        }
        return ret;
    }

    private static List<String> toStringList(List<String> rawStringList) {
        List<String> ret = new ArrayList<String>();
        for (Iterator<String> iter = rawStringList.iterator(); iter.hasNext();) {
            String rawString = iter.next();
            int index = rawString.indexOf(SEP);
            ret.add(rawString.substring(0, index));
        }
        return ret;
    }

    private static List<Entry> toEntryList(List<String> rawStringList) {
        List<Entry> ret = new ArrayList<Entry>();
        for (Iterator<String> iter = rawStringList.iterator(); iter.hasNext();) {
            String rawString = iter.next();
            Entry e = toEntry(rawString);
            ret.add(e);
        }
        return ret;
    }

    private static Entry toEntry(final String rawString) {
        StringTokenizer t = new StringTokenizer(rawString, SEP);
        String v = t.nextToken();
        String l = t.nextToken();
        return new Entry(v, Long.parseLong(l));
    }
}
