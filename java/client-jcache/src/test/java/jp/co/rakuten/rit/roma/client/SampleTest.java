package jp.co.rakuten.rit.roma.client;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import javax.swing.ImageIcon;

import jp.co.rakuten.rit.roma.client.jcache.ROMACacheFactory;
import junit.framework.TestCase;
import net.sf.jsr107cache.Cache;
import net.sf.jsr107cache.CacheEntry;
import net.sf.jsr107cache.CacheException;
import net.sf.jsr107cache.CacheFactory;
import net.sf.jsr107cache.CacheManager;

public class SampleTest extends TestCase {

    public void testGet() throws Exception {
	CacheSample sample = new CacheSample();

	// CacheManagerからCacheを取得し、オブジェクトを取り出す
	CacheManager singletonManager = CacheManager.getInstance();
	Cache cache = singletonManager.getCache("sampleCache");
	CacheEntry entry = cache.getCacheEntry("image1");
	ImageIcon value = (ImageIcon) entry.getValue();
	System.out.println(value);

    }

    public static class CacheSample {

	public CacheSample() {
	    CacheManager manager = CacheManager.getInstance();

	    try {
		// CacheFactoryを取得
		CacheFactory cacheFactory = manager.getCacheFactory();

		// Cacheの設定をMapに書き込み、CacheFactoryからCacheを生成
		Map config = new HashMap();
		// config.put("name", "sampleCache");
		// config.put("maxElementsInMemory", "10");
		// config.put("memoryStoreEvictionPolicy", "LFU"); //
		// LFU,LRU,FIFOのどれか
		// config.put("overflowToDisk", "true");
		// config.put("eternal", "false");
		// config.put("timeToLiveSeconds", "5");
		// config.put("timeToIdleSeconds", "5");
		// config.put("diskPersistent", "false");
		// config.put("diskExpiryThreadIntervalSeconds", "120");
		config.put(ROMACacheFactory.PARAM_URL, "10.162.127.145_11211");

		Cache cache = cacheFactory.createCache(config);

		// CacheManagerにCacheを登録
		manager.registerCache("sampleCache", cache);
	    } catch (CacheException ex) {
		ex.printStackTrace();
	    }

	    Cache cache = manager.getCache("sampleCache");
	    try {
		// Cacheにオブジェクトを追加
		ImageIcon icon = new ImageIcon(
			new URL(
				"http://journal.mycom.co.jp/images/ci_mycomjournal.gif"));
		cache.put("image1", icon);
	    } catch (MalformedURLException ex) {
		ex.printStackTrace();
	    }
	}
    }

}
