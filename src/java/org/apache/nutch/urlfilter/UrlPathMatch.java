package org.apache.nutch.urlfilter;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UrlPathMatch implements Comparable<UrlPathMatch>, Serializable {
	public static final Logger LOG = LoggerFactory.getLogger(UrlPathMatch.class);

	/**
	 * 
	 */
	private static final long serialVersionUID = 1114978948359127736L;
	// 数组大小上限
	private static final int ARRAY_LENGTH_LIMIT = 3;
	// Map存储结构
	private Map<String, UrlPathMatch> childrenMap;
	// 数组方式存储结构
	private UrlPathMatch[] childrenArray;
	// 当前节点上存储的字符
	private String nodeChar;
	// 当前节点存储的Segment数目
	// storeSize <=ARRAY_LENGTH_LIMIT ，使用数组存储， storeSize >ARRAY_LENGTH_LIMIT ,则使用Map存储
	private int storeSize = 0;
	// 当前DictSegment状态 ,默认 0 , 1表示从根节点到当前节点的路径表示一个配置路径 2表示是否配置地址
	private byte nodeState = 0;
	private byte nodeType = 0;// 0:protocol 1:host 2:path host的正则接到host的最后一个节点上 path的正则接到 Pathr 第一个节点上
	Object nodeValue = null;
	public UrlPathMatch parentNode = null;

	List<UrlMatchRule> matchRules = null;

	public static class UrlMatchRule implements Serializable {
		private static final long serialVersionUID = -4239841837039339029L;
		private Pattern matchRule = null; // 主机后的匹配规则
		public UrlPathMatch matchNode = null;
	};

	public Object getNodeValue() {
		return nodeValue;
	}

	public void setNodeValue(Object nodeValue) {
		this.nodeValue = nodeValue;
	}

	public static UrlPathMatch urlSegment = new UrlPathMatch();

	//
	public static class UrlNodeConfig implements Serializable {
		private static final long serialVersionUID = 1653948753000627048L;
		public int cfgUrlId = 0;
		public int onceCount = 0;
		public int crawlType = 0;
		public int customInterval = 2592000;
		public float customScore = 1.0f;
		public int fetchDepth = -1;// 不限制
		public boolean needLogin = false;
		public int loginType = 0; // 1：服务器端登录 2：客户端登录，必需使用代理客户端抓取
		public String loginAddress = null;
		public String loginJS = null;
		public String loginCLASS = null;
		public Map<String, String> loginParam = null;
		public String loginValidateAddress = null;
		public String loginoutAddress = null;
		public String loginUserName = null;
		public String loginPassword = null;

		public long rootSiteId;// 配置根地址ID
		public long mediaTypeId;// 媒体类型ID
		public long areaId;// 地域ID
		public long mediaLevelId;// 媒体级别ID
		public long topicTypeId;// 栏目类型ID
		public int subFetchInterval;// 子地址周期
		public HashMap<String, String> httpClientCfg = null;// HTTPCLIENT_CONFIG
		public RegexRule regFormat = null;
		public List<ExpFilter> subFilters;
		public List<ExpFilter> subCfgRules;
		public Map<String, String> conf;

		// regex: +pattern
		// regex: -pattern
		// calc:pattern $1$2 -long
		// / pattern = Pattern.compile(regex);
		public String getConf(String key) {
			if (conf == null)
				return null;
			return conf.get(key);
		}

		public int getInt(String key) {
			if (conf == null)
				return 0;
			String v = conf.get(key);
			if (v == null)
				return 0;
			return Integer.parseInt(conf.get(key));
		}

		public float getFloat(String key) {
			if (conf == null)
				return 0.0f;
			String v = conf.get(key);
			if (v == null)
				return 0.0f;
			return Float.parseFloat(conf.get(key));
		}
	}

	private UrlPathMatch(String nodeChar) {
		this.nodeChar = nodeChar;
	}

	public UrlPathMatch() {
	}

	public List<UrlPathMatch> getNodePath() {
		List<UrlPathMatch> res = new ArrayList<UrlPathMatch>();
		UrlPathMatch parent = this;
		while (parent != null) {
			res.add(0, parent);
			parent = parent.parentNode;
		}
		return res;
	}

	public String getConfigPath() {
		List<UrlPathMatch> res = getNodePath();
		String protocol = "", host = "", path = "";
		for (UrlPathMatch u : res) {
			if (u.nodeType == 0) {
				protocol = u.nodeChar;
			} else if (u.nodeType == 1) {
				host += u.nodeChar + ".";
			} else if (u.nodeType == 2) {
				path += "/" + u.nodeChar;
			}
		}
		return protocol + "://" + TableUtil.unreverseHost(host.substring(0, host.length() - 1)) + path;
	}

	/*
	 * 判断是否有下一个节点
	 */
	boolean hasNextNode() {
		return this.storeSize > 0;
	}

	public UrlPathMatch match(String url) {
		return match(url, this);
	}

	public static UrlPathMatch match(String url, UrlPathMatch searchHit) {
		try {
			if (searchHit == null)
				searchHit = urlSegment;
			java.net.URL r;
			r = new URL(url);
			String protocol = r.getProtocol();
			String host = r.getHost();
			String path = r.getPath();
			host = TableUtil.reverseHost(host);
			String[] paths = null;
			if (path != null && !path.trim().equals("") && !path.equals("/")) {
				paths = path.substring(1).split("/");
			}
			UrlPathMatch ds = match(protocol, searchHit, 0);// 找协议节点
			if (ds == null)
				return null;
			String[] hosts = host.split("\\.");
			UrlPathMatch lastMatch = null;
			List<UrlPathMatch> hostMatchs = new ArrayList<UrlPathMatch>();
			for (String h : hosts) {
				lastMatch = ds;
				ds = match(h, ds, 1);
				if (ds != null && ds.matchRules != null) {
					hostMatchs.add(ds);
				}
				if (ds == null)
					break;
			}
			UrlPathMatch resMatch = null;
			if (ds != null && ds.nodeState == 1)
				resMatch = ds;
			if (paths == null && resMatch != null) {
				return resMatch;
			}
			List<UrlPathMatch> matchDs = new ArrayList<UrlPathMatch>();
			int l = hostMatchs.size();
			for (int i = l - 1; i >= 0; i--) {// 倒序，最长匹配HOST先识别
				UrlPathMatch ma = hostMatchs.get(i);
				for (UrlMatchRule mr : ma.matchRules) {
					Matcher m = mr.matchRule.matcher(url);
					if (m != null && m.find()) {
						matchDs.add(mr.matchNode);
					}
				}
			}
			UrlPathMatch hostNode = ds;
			if (ds != null) {
				if (paths != null) {
					int len = 0;
					for (String h : paths) {// 主机及全路径匹配
						lastMatch = ds;
						ds = match(h, ds, 2);
						if (ds == null)
							break;
						if (len++ == 0 && !lastMatch.equals(hostNode)) {// 未找到路径
							if (resMatch != null)
								return resMatch;
							if (hostNode.matchRules != null)
								for (UrlMatchRule mr : hostNode.matchRules) {
									Matcher m = mr.matchRule.matcher(url);
									if (m != null && m.find()) {
										return (mr.matchNode);
									}
								}
							break;
						} else {
							if (ds.nodeState == 1)
								resMatch = ds;
						}
					}
					if (resMatch != null)
						return resMatch;
					// else
					// return null;
				}
			}
			if (matchDs.size() == 0) {// 主机未满足
				return null;
			}
			if (paths == null) {
				return matchDs.get(0);
			} else {
				for (UrlPathMatch ma : matchDs) {// 路径 匹配，主机最长
					UrlPathMatch lshost = null;
					UrlPathMatch pma = ma.parentNode;
					while (!pma.equals(ma)) {
						if (pma.nodeType == 1)// host 节点
							break;
						pma = pma.parentNode;
					}
					lshost = pma;
					ds = lshost;
					for (String h : paths) {
						lastMatch = ds;
						ds = match(h, ds, 2);
						if (ds == null) {
							if (lastMatch.matchRules != null)
								for (UrlMatchRule mr : lastMatch.matchRules) {
									Matcher m = mr.matchRule.matcher(url);
									if (m != null && m.find()) {
										return (mr.matchNode);
									}
								}
							break;
						}
						if (ds.nodeState == 1)
							resMatch = ds;
					}
					if (resMatch != null)
						return resMatch;
				}
				for (UrlPathMatch ma : matchDs) {// 主机匹配
					UrlPathMatch lshost = null;
					UrlPathMatch pma = ma.parentNode;
					while (!pma.equals(ma)) {
						if (pma.nodeType == 1)// host 节点
							break;
						pma = pma.parentNode;
					}
					lshost = pma;
					ds = lshost;
					if (lshost.matchRules != null)
						for (UrlMatchRule mr : lshost.matchRules) {
							Matcher m = mr.matchRule.matcher(url);
							if (m != null && m.find()) {
								return (mr.matchNode);
							}
						}
				}
			}
			return null;
			// for (String h : paths) {
			// lastMatch = ds;
			// ds = match(h, ds, 2);
			// if (ds == null) {
			// if (lastMatch != null && lastMatch.matchRule != null) {
			// Matcher m = lastMatch.matchRule.matcher(url);
			// if (m != null && m.find()) {
			// return lastMatch.matchNode;
			// // if (ds.nodeState == 1)
			// // resMatch = ds;
			// }
			// }
			// break;
			// } else {
			// if (ds.nodeState == 1)
			// resMatch = ds;
			// }
			// }
			// if (resMatch != null)
			// return resMatch;
			// return hostMatchNode;
		} catch (MalformedURLException e) {
		}
		return null;
	}

	private static UrlPathMatch match(String sp, UrlPathMatch searchHit, int type) {
		if (searchHit == null)
			return null;
		UrlPathMatch ds = null;
		// 引用实例变量为本地变量，避免查询时遇到更新的同步问题
		UrlPathMatch[] segmentArray = null;
		Map<String, UrlPathMatch> segmentMap = null;
		segmentArray = searchHit.childrenArray;
		segmentMap = searchHit.childrenMap;
		if (segmentArray != null) {// 先判断，后循环
			for (int i = 0; i < searchHit.storeSize; i++) {
				if (segmentArray[i].nodeChar.equals(sp) && segmentArray[i].nodeType == type) {
					ds = segmentArray[i];
					break;
				}
			}
			if (ds == null)
				return null;
		} else if (segmentMap != null) {
			ds = (UrlPathMatch) segmentMap.get(sp);// 在map中查找
			if (ds == null || ds.nodeType != type)
				return null;
		}
		return ds;
	}

	// public synchronized void fillSegment(String url, Object value) {
	// fillSegment(url, value, null);
	// }

	public synchronized void fillSegment(String url, Object value, String matchRule) {
		try {
			java.net.URL r = new URL(url);
			String protocol = r.getProtocol();
			String host = r.getHost();
			String path = r.getPath();
			host = TableUtil.reverseHost(host);
			fillSegment(protocol, host, path, matchRule, value);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}

	// private byte nodeType = 0;// 0:protocol 1:host 2:path host的正则拉到host的最后一个节点上 path的正则接到 Pathr 第一个节点上
	// private Pattern matchRule = null; // 主机~路径匹配规则 正则~路径正则
	private synchronized void fillSegment(String protocel, String host, String path, String matchRule, Object value) {
		if (path == null || path.trim().equals("") || path.equals("/")) {
			if (matchRule == null || matchRule.trim().equals(""))
				fillSegment(protocel, host, (String[]) null, null, value);
			else
				fillSegment(protocel, host, (String[]) null, matchRule.split("~"), value);
		} else {
			String[] pathArray = path.substring(1).split("/");
			if (matchRule == null || matchRule.trim().equals(""))
				fillSegment(protocel, host, (String[]) pathArray, null, value);
			else
				fillSegment(protocel, host, (String[]) pathArray, matchRule.split("~"), value);
		}
	}

	private synchronized void fillSegment(String protocel, String host, String[] pathArray, String[] matchRule, Object value) {
		String keyChar = protocel;
		UrlPathMatch ds = null;
		ds = this.lookforSegment(keyChar);// 搜索当前节点的存储，查询对应keyChar的keyChar，如果没有则创建
		ds.nodeType = 0;
		String[] hosts = host.split("\\.");

		UrlPathMatch hostMatchNode = null;
		UrlPathMatch pathMatchNode = null;
		for (int i = 0; i < hosts.length; i++) {
			String h = hosts[i];
			keyChar = h;
			if (i > 0)
				hostMatchNode = ds;
			ds = ds.lookforSegment(keyChar);
			ds.nodeType = 1;
		}
		if (hostMatchNode == null) {
			hostMatchNode = ds;
		}
		pathMatchNode = ds;
		UrlMatchRule hostMatchRule = null;
		UrlMatchRule pathMatchRule = null;
		if (matchRule != null && matchRule.length > 0 && matchRule[0] != null && !matchRule[0].equals("")) {
			try {
				hostMatchRule = new UrlMatchRule();
				hostMatchRule.matchRule = Pattern.compile(matchRule[0]);
			} catch (Exception e) {
				LOG.error("主机正则匹配规则错误：" + TableUtil.unreverseHost(host) + " rule:" + matchRule[0]);
				hostMatchRule = null;
			}
		}
		if (matchRule != null && matchRule.length > 1 && matchRule[1] != null && !matchRule[1].equals("")) {
			try {
				pathMatchRule = new UrlMatchRule();
				pathMatchRule.matchRule = Pattern.compile(matchRule[1]);
			} catch (Exception e) {
				LOG.error("路径匹配规则错误：" + TableUtil.unreverseHost(host) + "/" + NutchConstant.Join(pathArray, "/") + " rule:" + matchRule[1]);
				pathMatchRule = null;
			}
		}
		if (pathArray != null) {
			keyChar = pathArray[0];
			ds = ds.lookforSegment(keyChar);
			int length = pathArray.length;
			int len = 1;
			while (len < length && ds != null) {
				ds.nodeType = 2;
				keyChar = pathArray[len];
				ds = ds.lookforSegment(keyChar);
				len++;
			}
			if (ds != null) {
				ds.nodeType = 2;
				ds.nodeState = 1;
				ds.nodeValue = value;
			}
		} else {
			ds.nodeState = 1;
			ds.nodeValue = value;
		}
		if (hostMatchRule != null) {
			if (hostMatchNode.matchRules == null)
				hostMatchNode.matchRules = new ArrayList<UrlMatchRule>();
			hostMatchRule.matchNode = ds;
			hostMatchNode.matchRules.add(hostMatchRule);
		}
		if (pathMatchRule != null) {
			if (pathMatchNode.matchRules == null)
				pathMatchNode.matchRules = new ArrayList<UrlMatchRule>();
			pathMatchRule.matchNode = ds;
			pathMatchNode.matchRules.add(pathMatchRule);
		}
	}

	/**
	 * 查找本节点下对应的keyChar的segment *
	 * 
	 * @param keyChar
	 * @param create
	 *            =1如果没有找到，则创建新的segment ; =0如果没有找到，不创建，返回null
	 * @return
	 */
	private UrlPathMatch lookforSegment(String keyChar) {
		UrlPathMatch tmpDs = null;
		if (this.storeSize <= ARRAY_LENGTH_LIMIT) {
			// 获取数组容器，如果数组未创建则创建数组
			UrlPathMatch[] segmentArray = this.getChildrenArray();
			// 搜寻数组
			for (int i = 0; i < this.storeSize; i++) {
				if (segmentArray[i] != null && segmentArray[i].nodeChar.equals(keyChar)) {
					tmpDs = segmentArray[i];
					break;
				}
			}
			// 遍历数组后没有找到对应的segment
			if (tmpDs == null) {
				tmpDs = new UrlPathMatch(keyChar);
				tmpDs.parentNode = this;
				if (this.storeSize < ARRAY_LENGTH_LIMIT) {
					segmentArray[this.storeSize] = tmpDs;
					this.storeSize++;
					Arrays.sort(segmentArray, 0, this.storeSize);
				} else {
					// 数组容量已满，切换Map存储
					// 获取Map容器，如果Map未创建,则创建Map
					Map<String, UrlPathMatch> segmentMap = this.getChildrenMap();
					// 将数组中的segment迁移到Map中
					this.migrate(segmentArray, segmentMap);
					// 存储新的segment
					segmentMap.put(keyChar, tmpDs);
					// segment数目+1 ， 必须在释放数组前执行storeSize++ ， 确保极端情况下，不会取到空的数组
					this.storeSize++;
					// 释放当前的数组引用
					this.childrenArray = null;
				}
			}
		} else {
			// 获取Map容器，如果Map未创建,则创建Map
			Map<String, UrlPathMatch> segmentMap = this.getChildrenMap();
			// 搜索Map
			tmpDs = (UrlPathMatch) segmentMap.get(keyChar);
			if (tmpDs == null) {
				// 构造新的segment
				tmpDs = new UrlPathMatch(keyChar);
				tmpDs.parentNode = this;
				segmentMap.put(keyChar, tmpDs);
				// 当前节点存储segment数目+1
				this.storeSize++;
			}
		}
		return tmpDs;
	}

	/**
	 * 获取数组容器 线程同步方法
	 */
	private UrlPathMatch[] getChildrenArray() {
		if (this.childrenArray == null) {
			synchronized (this) {
				if (this.childrenArray == null) {
					this.childrenArray = new UrlPathMatch[ARRAY_LENGTH_LIMIT];
				}
			}
		}
		return this.childrenArray;
	}

	/**
	 * 获取Map容器 线程同步方法
	 */
	private Map<String, UrlPathMatch> getChildrenMap() {
		if (this.childrenMap == null) {
			synchronized (this) {
				if (this.childrenMap == null) {
					this.childrenMap = new HashMap<String, UrlPathMatch>(ARRAY_LENGTH_LIMIT * 2, 0.1f);
				}
			}
		}
		return this.childrenMap;
	}

	/**
	 * 将数组中的segment迁移到Map中
	 * 
	 * @param segmentArray
	 */
	private void migrate(UrlPathMatch[] segmentArray, Map<String, UrlPathMatch> segmentMap) {
		for (UrlPathMatch segment : segmentArray) {
			if (segment != null) {
				segmentMap.put(segment.nodeChar, segment);
			}
		}
	}

	/**
	 * 实现Comparable接口
	 * 
	 * @param o
	 * @return int
	 */
	public int compareTo(UrlPathMatch o) {
		if (o == null)
			return -1;
		// 对当前节点存储的char进行比较
		return this.nodeChar.compareTo(o.nodeChar);
	}

	public static void main(String[] args) throws NumberFormatException, Exception {
		Configuration conf = NutchConfiguration.create();
		NutchConstant.setUrlConfig(conf, 3);
		String url = "http://news.qq.com/a";
		String url1 = "http://cd.news.qq.com";
		String url2 = "http://(\\w*\\.)?news\\.qq\\.com/a";
		String url3 = "http://news.qq.com/a.*";
		// urlSegment.fillSegment(url, url);
		// urlSegment.fillSegment(url1, url);
		// urlSegment.fillSegment(url2, url);
		// urlSegment.fillSegment(url3, url);

		UrlPathMatch ma = urlSegment.match(url);
		if (ma == null) {
			// 不满足配置要求
			GeneratorJob.LOG.warn("url:" + url + " is not in urlconfig rowkey:" + url);
			return;
		}
		UrlPathMatch.UrlNodeConfig value = (UrlPathMatch.UrlNodeConfig) ma.getNodeValue();
		String configUrl = ma.getConfigPath();

		System.out.println(configUrl + "  value=" + value);
	}
}
