package jp.co.rakuten.rit.roma.client.routing;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import jp.co.rakuten.rit.roma.client.ClientException;
import jp.co.rakuten.rit.roma.client.Node;
import jp.co.rakuten.rit.roma.client.RomaClient;

/**
 * 
 */
public class RoutingTable {

    private static final String DIGEST_ALGORITHM_SHA1 = "SHA1";

    private static final String ZERO = "0";

    private static final String ONE = "1";

    private static final String TWO = "2";

    private RomaClient client;

    private RoutingInfoUpdatingThread thread;

    protected boolean loopEnabled = false;

    protected int interval = 3000;

    private BigInteger hbits;

    private BigInteger searchMask;

    private String markleHashTree = null;

    protected List<Node> physicalNodes = null;

    protected List<String> virtualNodes = null;

    protected HashMap<String, List<Node>> virtualNodeMap = null;

    protected HashMap<Node, Integer> failCount = new HashMap<Node, Integer>();

    public RoutingTable(RomaClient client) {
	this.client = client;
	//thread = new RoutingInfoUpdatingThread(this);
    }

    public synchronized boolean enableLoop() {
	return loopEnabled;
    }

    public synchronized void startLoop() {
	loopEnabled = true;
	thread = new RoutingInfoUpdatingThread(this);
	thread.doStart();
    }

    public synchronized void stopLoop() {
	thread.doStop();
	loopEnabled = false;
    }

    public synchronized List<Node> getPhysicalNodes() {
	return physicalNodes;
    }

    private synchronized List<Node> getVirtualNodes(String virtualNodeID) {
	return virtualNodeMap.get(virtualNodeID);
    }

    private synchronized BigInteger getVirtualNodeID(BigInteger hash) {
	if (searchMask != null) {
	    return hash.and(searchMask);
	} else {
	    return null;
	}
    }

    public synchronized int getFailCount(Node node) {
	Integer i = failCount.get(node);
	if (i == null) {
	    return 0;
	} else {
	    return i.intValue();
	}
    }

    public synchronized void incrFailCount(Node node) {
	int i = getFailCount(node);
	i++;
	failCount.put(node, new Integer(i));
	markleHashTree = "";
    }

    private synchronized void clearFailCount() {
	failCount.clear();
    }

    @SuppressWarnings("unchecked")
    public void init(List<Object> list) {
	HashMap<String, Object> map = (HashMap<String, Object>) list.get(0);
	// hbits = 2**dgst_bits
	int dgst_bits = ((BigDecimal) map.get("dgst_bits")).intValue();
	BigInteger hbits0 = new BigInteger(TWO).pow(dgst_bits);

	// search_mask = 2**div_bits - 1 << (dgst_bits - div_bits)
	int div_bits = ((BigDecimal) map.get("div_bits")).intValue();
	BigInteger searchMask0 = (new BigInteger(TWO).pow(div_bits)
		.subtract(new BigInteger(ONE))).shiftLeft(dgst_bits - div_bits);

	// rn = ((BigDecimal) map.get("rn")).intValue();

	// physical nodes
	List<String> nodeList = (List<String>) list.get(1);
	List<Node> physicalNodes0 = Node.create(nodeList);

	// virtual nodes
	HashMap<String, List<String>> map2 = (HashMap<String, List<String>>) list
		.get(2);
	HashMap<String, List<Node>> virtualNodeMap0 = toVirtualNodeMap(map2);
	List<String> virtualNodes0 = toVirtualNodes(map2);

	set(hbits0, searchMask0, physicalNodes0, virtualNodeMap0, virtualNodes0);
	clearFailCount();
    }

    private static HashMap<String, List<Node>> toVirtualNodeMap(
	    HashMap<String, List<String>> map) {
	HashMap<String, List<Node>> ret = new HashMap<String, List<Node>>();
	Iterator<String> keys = map.keySet().iterator();
	while (keys.hasNext()) {
	    String key = keys.next();
	    List<String> nodes = map.get(key);
	    try {
		List<Node> nodes0 = toVirtualNodeMap0(nodes);
		ret.put(key, nodes0);
	    } catch (NumberFormatException e) {
		throw e;
	    }
	}
	return ret;
    }

    private static List<Node> toVirtualNodeMap0(List<String> list) {
	List<Node> ret = new ArrayList<Node>();
	Iterator<String> nodes = list.iterator();
	while (nodes.hasNext()) {
	    String node = nodes.next();
	    Node node0 = Node.create(node);
	    ret.add(node0);
	}
	return ret;
    }

    private static List<String> toVirtualNodes(HashMap<String, List<String>> map) {
	TreeSet<BigInteger> set = new TreeSet<BigInteger>();
	Iterator<String> keys = map.keySet().iterator();
	while (keys.hasNext()) {
	    String key = keys.next();
	    set.add(new BigInteger(key));
	}

	List<String> list = new ArrayList<String>();
	Iterator<BigInteger> iter = set.iterator();
	while (iter.hasNext()) {
	    String s = iter.next().toString();
	    list.add(s);
	}
	return list;
    }

    private synchronized void set(BigInteger hbits, BigInteger sMask,
	    List<Node> pNodes, HashMap<String, List<Node>> vnMap,
	    List<String> vNodes) {
	this.hbits = hbits;
	this.searchMask = sMask;
	this.physicalNodes = pNodes;
	this.virtualNodeMap = vnMap;
	this.virtualNodes = vNodes;
    }

    public synchronized void clear() {
	hbits = null;
	searchMask = null;
	if (physicalNodes != null) {
	    physicalNodes.clear();
	}
	if (virtualNodeMap != null) {
	    virtualNodeMap.clear();
	}
    }

    public void update() {
	List<Node> tmpNodes = getPhysicalNodes();
	Iterator<Node> iter = tmpNodes.iterator();
	List<Object> list = null;
	while (iter.hasNext()) {
	    try {
		Node node = iter.next();

		// checks a markle hash tree
		//System.out.println("RoutingTable#update()1: node: " + node);
		String mkh = client.routingmht(node);
		//System.out.println("RoutingTable#update()1: node: " + node + ", mkh: " + mkh);
		if (mkh == null) {
		    continue;
		} else if (mkh.equals(markleHashTree)) {
		    return;
		}

		// If the new one is not same as the previous one,
		// then it gets all information of the routing table.
		//System.out.println("RoutingTable#update()2: node: " + node);
		list = client.routingdump(node);
		//System.out.println("RoutingTable#update()2: node: " + node + ", list: " + (list != null));
		if (list != null) {
		    markleHashTree = mkh;
		    init(list);
		    return;
		}
	    } catch (ClientException e) { // ignore 
		// e.printStackTrace();
	    }
	}
    }

    public Node searchNode(String key, BigInteger hash) {
	Node node = null;

	List<Node> nodeList;
	Iterator<Node> nodes;
	String virtualNodeID = getVirtualNodeID(hash).toString();

	nodeList = getVirtualNodes(virtualNodeID);
	nodes = nodeList.iterator();
	while (nodes.hasNext()) {
	    node = nodes.next();
	    if (getFailCount(node) == 0) {
		return node;
	    }
	}

	synchronized (virtualNodes) {
	    Iterator<String> vns = virtualNodes.iterator();
	    String vnID = null;
	    while (vns.hasNext()) {
		vnID = vns.next();
		if (vnID.equals(virtualNodeID)) {
		    break;
		}
	    }

	    do {
		if (vns.hasNext()) {
		    vnID = vns.next();
		} else {
		    // vnID = virtualNodes.get(0);
		    vns = virtualNodes.iterator();
		    vnID = vns.next();
		}

		if (vnID.equals(virtualNodeID)) {
		    return node;
		}

		nodeList = getVirtualNodes(vnID);
		nodes = nodeList.iterator();
		while (nodes.hasNext()) {
		    node = nodes.next();
		    if (getFailCount(node) == 0) {
			return node;
		    }
		}
	    } while (vns.hasNext());
	}
	return node;
    }

    public synchronized BigInteger getHash(String key) {
	if (hbits != null) {
	    return getDigestHash(key, hbits);
	} else {
	    return null;
	}
    }

    private BigInteger getDigestHash(String key, BigInteger hbits) {
	try {
	    String s = getStringDigestSHA1(key);
	    BigInteger b = new BigInteger(s, 16);
	    b = b.remainder(hbits);
	    return b;
	} catch (Exception e) { // ignore
	    e.printStackTrace();
	}
	return null;
    }

    private String getStringDigestSHA1(String key)
	    throws NoSuchAlgorithmException {
	MessageDigest md = MessageDigest.getInstance(DIGEST_ALGORITHM_SHA1);
	md.update(key.getBytes());
	byte[] b = md.digest();
	StringBuffer buf = new StringBuffer("");
	for (int i = 0; i < b.length; ++i) {
	    int val = b[i] & 0xFF;
	    if (val < 16) {
		buf.append(ZERO);
	    }
	    buf.append(Integer.toString(val, 16));
	}
	return buf.toString();
    }
}
