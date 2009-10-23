package jp.co.rakuten.rit.roma.client.jcache;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.Node;
import jp.co.rakuten.rit.roma.client.RomaClient;
import jp.co.rakuten.rit.roma.client.RomaClientFactory;
import net.sf.jsr107cache.Cache;
import net.sf.jsr107cache.CacheException;
import net.sf.jsr107cache.CacheFactory;

public class ROMACacheFactory implements CacheFactory {

    public static final String PARAM_URL = "url";

    public Cache createCache(Map env) throws CacheException {
		RomaClient client = null;
		try {
			client = RomaClientFactory.createDefaultRomaClient();
		} catch (ClientException e) {
			throw new CacheException("cannot create roma client.", e);
		}
		String value = (String) env.get("url");
		if (value == null) {
			throw new IllegalArgumentException("needs url value.");
		}
		List<String> list = new ArrayList<String>();
		for (String v : value.split("\t")) {
			list.add(v);
		}
		try {
			client.open(Node.create(list));
		} catch (ClientException e) {
			throw new CacheException("can not create roma client.", e);
		}
		return new ROMACache(client);
	}

}
