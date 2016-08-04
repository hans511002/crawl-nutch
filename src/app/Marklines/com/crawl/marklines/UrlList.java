package com.crawl.marklines;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.util.Utf8;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.CookieStore;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.hbase.Convert;
import org.apache.nutch.parse.element.ConfigAttrParses;
import org.apache.nutch.parse.element.ConfigAttrParses.TopicTypeRule;
import org.apache.nutch.parse.element.ExpandAttrsParses;
import org.apache.nutch.parse.element.ExprCalcParses;
import org.apache.nutch.parse.element.ExprCalcParses.SegmentGroupRule;
import org.apache.nutch.parse.element.ExprCalcRule;
import org.apache.nutch.parse.element.PolicticTypeParses;
import org.apache.nutch.parse.element.colrule.ColSegmentParse;
import org.apache.nutch.parse.element.wordfre.WordFreqRule;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.urlfilter.SubURLFilters;
import org.apache.nutch.urlfilter.UrlPathMatch;
import org.apache.nutch.util.TableUtil;

import com.sobey.jcg.support.log4j.LogUtils;

public class UrlList {
	public static LinkedList<urlCfg> pages = new LinkedList<urlCfg>();
	static Map<String, urlCfg> urlMap = new HashMap<String, urlCfg>();
	static WebPage page = null;
	static urlCfg cfg;
	static String langType = CrawlMain.langType;
	static String rooturl = "https://www.marklines.com/" + langType;
	public static LinkedList<GroupConfig> groupCfg = new LinkedList<GroupConfig>();
	static Map<String, WebPage> subBufferPages = new HashMap<String, WebPage>();

	public static class GroupConfig {
		public int gid;
		public String name;
		public String url;
		public Pattern regUrl = Pattern.compile("");

		public String startKey = "";
		public String endKey = "";

		public String groupRule = "";
		List<GroupSegmentConfig> segment = new ArrayList<GroupSegmentConfig>();

		public Pattern addFetchListRule = Pattern.compile("");
		public boolean needAddList = false;

		public String toString() {
			return url + " regUrl=" + regUrl + " groupRule=" + groupRule + " \n";
		}

	}

	public static class GroupSegmentConfig {
		public int segid;
		public String colname;
		public String colParseRule = "";
		public String colid;
	}

	public static class urlCfg {
		public String url;
		public Pattern regUrl = Pattern.compile("");
		public Pattern regUrlName = Pattern.compile(".*");
		public Pattern subregUrl = Pattern.compile("");
		public Pattern subregName = Pattern.compile(".*");

		public String toString() {
			return url + " regUrl=" + regUrl + " regUrlName=" + regUrlName + " subregUrl=" + subregUrl + "\n";
		}

	}

	// 读取设置配置抓取地址
	public static void setUrlConfig(Configuration conf) throws IOException {
		UrlPathMatch.UrlNodeConfig value = new UrlPathMatch.UrlNodeConfig();
		value.cfgUrlId = 1;
		value.onceCount = 100000;
		String httpClientCfg = ""; // 客户端配置，header cookies NutchConstant.filterComment( )
		if (httpClientCfg != null && !httpClientCfg.trim().equals("")) {
			String[] pars = httpClientCfg.split("\n");
			HashMap<String, String> map = new HashMap<String, String>();
			for (String par : pars) {
				int index = par.indexOf("=");
				if (index != -1) {
					map.put(par.substring(0, index), par.substring(index + 1));
				}
			}
			if (map.size() > 0)
				value.httpClientCfg = map;
			else
				value.httpClientCfg = null;
		}
		List<String> reverseHosts = new ArrayList<String>();
		reverseHosts.add(TableUtil.reverseHost(new URL(rooturl).getHost()));

		String urlFormat = "";
		if (urlFormat != null) {
			String[] tmp = urlFormat.split("~");
			try {
				value.regFormat = new org.apache.nutch.urlfilter.RegexRule(tmp[0], tmp.length > 1 ? tmp[1] : "");
			} catch (Exception exp) {
				value.regFormat = null;
			}
		}
		value.crawlType = 1;
		value.customInterval = Integer.MAX_VALUE;
		value.customScore = 1;
		value.fetchDepth = Integer.MAX_VALUE;// http://aa/
		int needLogin = 0;// 是否需要登录
		if (needLogin > 0) {
			value.needLogin = true;
			int loginType = 1;
			value.loginType = loginType;
			if (loginType == 1) {// 服务器1：服务器端登录 2：客户端登录，必需使用代理客户端抓取
				String loginAddress = "";
				NutchConstant.decodeLoginAddress(loginAddress, value);
			} else {
				value.crawlType = (value.crawlType & 6);
			}
			value.loginJS = ""; // 登录的js
			value.loginCLASS = "";// 实现登录的java类
		}
		String filStr = "regex:+.*/year\\?nationCode=.*\n" // prod sale
				+ "regex:+.*/" + langType
				+ "/statistics/engineproduction/engine_prod/.*\n" // engine
				+ "regex:+/" + langType
				+ "/global/\\d+\n"// holl
				+ "regex:+(.*/" + langType + "/top\\d+/\\w\\d+_\\d+)|(.*/"
				+ langType
				+ "/supplier_db/partDetail\\?page=\\d.*)";// part
		value.subFilters = SubURLFilters.buildExp(filStr);

		value.subCfgRules = null;

		String confs = "";// key:val;
		if (confs != null && !confs.trim().equals("")) {
			confs = NutchConstant.filterComment(confs);
			if (confs != null && !confs.trim().equals("")) {
				value.conf = new HashMap<String, String>();
				String[] cfs = confs.trim().split("\n");
				for (int cf = 0; cf < cfs.length; cf++) {
					String[] tmp = cfs[cf].split(":");
					// if (tmp.length == 2)
					value.conf.put(tmp[0], cfs[cf].substring(tmp[0].length() + 1));
				}
			}
		}
		value.subFetchInterval = Integer.MAX_VALUE;
		if (value.subFetchInterval == 0)
			value.subFetchInterval = value.customInterval;
		value.rootSiteId = 1;// 配置根地址ID
		value.mediaTypeId = 0;// 媒体类型ID
		value.mediaLevelId = 0;// 媒体级别ID
		value.topicTypeId = 0;// 栏目类型ID
		value.areaId = 0;// 地域ID
		String mr = "";
		UrlPathMatch.urlSegment.fillSegment(rooturl, value, mr);

		String serStr = NutchConstant.serialObject(UrlPathMatch.urlSegment, true, true);

		conf.set(NutchConstant.stepConfigUrl, serStr);
		LogUtils.info("url config Serializable size:" + serStr.length());
		LogUtils.debug("set " + NutchConstant.stepConfigUrl + "=" + conf.get(NutchConstant.stepConfigUrl));
		UrlPathMatch.urlSegment = null;
		// 设置generat的startkey 和 endkey
		if (reverseHosts.size() > 0 && NutchConstant.BatchNode.generatNode == NutchConstant.curBatchNode) {
			String startkey = reverseHosts.get(0);
			String endKey = reverseHosts.get(0);
			for (String host : reverseHosts) {
				if (host.compareTo(startkey) < 0) {
					startkey = host;
				}
				if (host.compareTo(endKey) > 0) {
					endKey = host;
				}
			}
			conf.set(NutchConstant.stepHbaseStartRowKey, startkey);
			conf.set(NutchConstant.stepHbaseEndRowKey, endKey + "zzzz");
		}
	}

	// 从conf中反解析配置地址树对象
	public static UrlPathMatch getUrlConfig(Configuration conf) throws IOException {
		if (NutchConstant.urlcfg == null) {
			String serStr = conf.get(NutchConstant.stepConfigUrl);
			if (serStr == null) {
				setUrlConfig(conf);
				serStr = conf.get(NutchConstant.stepConfigUrl);
				if (serStr == null)
					throw new IOException(NutchConstant.stepConfigUrl + " in config is null");
			}
			UrlPathMatch.urlSegment = (UrlPathMatch) NutchConstant.deserializeObject(serStr, true, true);
			NutchConstant.urlcfg = UrlPathMatch.urlSegment;
			UrlPathMatch.urlSegment = NutchConstant.urlcfg;
		}
		return NutchConstant.urlcfg;
	}

	// 从数据读取内容要素解析规则，并序列化到配置对象中
	public static void setSegmentParseRules(Configuration conf) throws IOException {
		UrlPathMatch urlSegment = new UrlPathMatch();
		String url = rooturl;
		String mr = "";
		urlSegment.fillSegment(url, null, mr);

		HashMap<String, ColSegmentParse> regParContains = new HashMap<String, ColSegmentParse>();
		HashMap<String, ExprCalcRule> contains = new HashMap<String, ExprCalcRule>();
		ConfigAttrParses.topicTypeRules = new HashMap<Long, TopicTypeRule>();
		String serStr = NutchConstant.serialObject(new Object[] { urlSegment, ConfigAttrParses.topicTypeRules }, true,
				true);
		urlSegment = null;

		conf.set(NutchConstant.segmentRuleBaseConfigKey, serStr);
		LogUtils.info("segment attrs Serializable size:" + serStr.length());
		LogUtils.debug("set " + NutchConstant.segmentRuleBaseConfigKey + "="
				+ conf.get(NutchConstant.segmentRuleBaseConfigKey));
		ConfigAttrParses.baseUrlAttr = null;
		ConfigAttrParses.topicTypeRules = null;

		// 要素分组规则
		ExprCalcParses.cfgSegGroupColRuleParses = new HashMap<String, List<SegmentGroupRule>>();
		conf.get(NutchConstant.segmentGroupRuleSQL,
				" SELECT g.SEGMENT_GROUP_ID,g.SEGMENT_GROUP_NAME,g.SEGMENT_GROUP_URL_RULE,a.BASE_URL"
						+ " FROM ERIT_KL_BASE_SEGMENT_GROUP g INNER JOIN ERIT_KL_FETCH_BASE_URL a  "
						+ " ON a.BASE_URL_ID=g.BASE_URL_ID AND a.URL_STATE=1 "
						+ "  ORDER BY g.BASE_URL_ID,g.SEGMENT_GROUP_ID");

		for (GroupConfig entry : groupCfg) {
			long groupId = entry.gid;
			String groupName = entry.name;
			String groupRule = entry.groupRule;
			String config = rooturl;
			if (config.charAt(config.length() - 1) == '/') {
				config = config.substring(0, config.length() - 1);
			}
			List<SegmentGroupRule> groupRules = ExprCalcParses.cfgSegGroupColRuleParses.get(config);
			if (groupRules == null) {
				groupRules = new ArrayList<SegmentGroupRule>();
				ExprCalcParses.cfgSegGroupColRuleParses.put(config, groupRules);
			}
			SegmentGroupRule sgrule = new SegmentGroupRule();
			sgrule.groupId = groupId;
			sgrule.groupName = groupName;
			if (groupRule != null && groupRule.trim().equals("") == false && groupRule.trim().startsWith("#") == false)
				sgrule.pattern = Pattern.compile(groupRule.trim());
			LogUtils.debug("添加 configUrl:" + config + " 分组:" + groupName + " 解析规则：" + groupRule);
			groupRules.add(sgrule);
		}

		// 要素表达式规则
		conf.get(
				NutchConstant.segmentParseRuleSQL,
				"SELECT a.BASE_URL,b.SEGMENT_COL_NAME,b.SEGMENT_RULES,b.SEGMENT_GROUP_ID,b.BASE_SEGMENT_ID FROM ERIT_KL_FETCH_BASE_URL a "
						+ "INNER JOIN ERIT_KL_BASE_SEGMENT b ON a.BASE_URL_ID=b.BASE_URL_ID "
						+ "WHERE a.URL_STATE=1 ORDER BY a.BASE_URL,b.SEGMENT_COL_NAME ");
		for (GroupConfig entry : groupCfg) {
			for (GroupSegmentConfig seg : entry.segment) {
				String config = rooturl;
				String colName = seg.colname;
				String rules = seg.colParseRule;
				long groupId = entry.gid;
				String baseSegId = seg.colid; // 字段ID
				if (config.charAt(config.length() - 1) == '/') {
					config = config.substring(0, config.length() - 1);
				}
				SegmentGroupRule curGroupRule = null;
				List<SegmentGroupRule> groupRules = ExprCalcParses.cfgSegGroupColRuleParses.get(config);
				if (groupRules == null) {// 只有groupId==0 才为空
					groupRules = new ArrayList<SegmentGroupRule>();
					ExprCalcParses.cfgSegGroupColRuleParses.put(config, groupRules);
					curGroupRule = new SegmentGroupRule();
					curGroupRule.groupId = -1;
					curGroupRule.groupName = "无分组";
					curGroupRule.pattern = null;
					groupRules.add(curGroupRule);
				} else {
					if (groupId == 0) {
						curGroupRule = groupRules.get(0);
					} else {
						for (SegmentGroupRule grule : groupRules) {
							if (grule.groupId == groupId) {
								curGroupRule = grule;
								break;
							}
						}
					}
					if (curGroupRule == null) {//
						LogUtils.error("configUrl:" + config + " 字段：" + colName + " 对应分组ID:" + groupId + " 不存在  解析规则："
								+ rules);
						continue;
					}
				}
				rules = NutchConstant.filterComment(rules);
				ColSegmentParse parse = regParContains.get(baseSegId);
				if (curGroupRule.segmentRuls == null) {
					curGroupRule.segmentRuls = new ArrayList<ColSegmentParse>();
				}
				List<ColSegmentParse> listExp = curGroupRule.segmentRuls;
				if (parse == null) {
					LogUtils.debug(config + " 添加字段" + colName + " 解析规则:" + rules);
					List<ExprCalcRule> regRules = NutchConstant.bulidExprCalcRules(colName, rules, contains);
					if (regRules == null) {
						LogUtils.warn("column:" + colName + "  no right parse rule");
					} else {
						parse = new ColSegmentParse(new Utf8(colName), regRules);
						regParContains.put(baseSegId, parse);
						listExp.add(parse);
					}
				} else {
					listExp.add(parse);
				}

			}
		}
		contains.clear();
		regParContains.clear();
		ExpandAttrsParses.rules = new ArrayList<WordFreqRule>();// 词频规则，主题规则 信息正负面规则
		serStr = NutchConstant.serialObject(new Object[] { ExprCalcParses.cfgSegGroupColRuleParses,
				PolicticTypeParses.rules, ExpandAttrsParses.rules }, true, true);
		conf.set(NutchConstant.segmentRuleSerializKey, serStr);
		LogUtils.info("segment parse rules Serializable size:" + serStr.length());
		LogUtils.debug("set " + NutchConstant.segmentRuleSerializKey + "="
				+ conf.get(NutchConstant.segmentRuleSerializKey));
		ExprCalcParses.cfgSegGroupColRuleParses.clear();
		ExprCalcParses.cfgSegGroupColRuleParses = null;
		PolicticTypeParses.rules = null;
		ExpandAttrsParses.rules.clear();
		ExpandAttrsParses.rules = null;

	}

	public static void add(urlCfg cfg) {
		if (!urlMap.containsKey(cfg.url)) {
			pages.push(cfg);
		} else {
			LogUtils.warn("================ add cfg failed:" + cfg);
		}
	}

	public static void setGroupConfig() {
		GroupConfig prod = new GroupConfig();
		prod.gid = 1;
		prod.name = "prod";
		prod.url = "https://www.marklines.com/" + langType + "/vehicle_production/";
		prod.regUrl = Pattern.compile(".*/" + langType + "/vehicle_production/year\\?nationCode=.*");
		prod.groupRule = ".*/" + langType + "/vehicle_production/year\\?nationCode=.*";
		prod.startKey = "https://www.marklines.com/" + langType + "/vehicle_production/";
		prod.endKey = "https://www.marklines.com/" + langType + "/vehicle_production/yeaz";
		prod.addFetchListRule = prod.regUrl;
		groupCfg.add(prod);

		GroupConfig sale = new GroupConfig();
		sale.gid = 2;
		sale.url = "https://www.marklines.com/" + langType + "/vehicle_sales/";
		sale.name = "sale";
		sale.regUrl = Pattern.compile(".*/" + langType + "/vehicle_sales/year\\?nationCode=.*");
		sale.groupRule = ".*/" + langType + "/vehicle_sales/year\\?nationCode=.*";
		sale.startKey = "https://www.marklines.com/" + langType + "/vehicle_sales/";
		sale.endKey = "https://www.marklines.com/" + langType + "/vehicle_sales/yeaz";
		sale.addFetchListRule = sale.regUrl;
		groupCfg.add(sale);

		GroupConfig engine = new GroupConfig();
		engine.gid = 3;
		engine.url = "https://www.marklines.com/" + langType + "/statistics/engineproduction/engineproduction";
		engine.name = "engine";
		engine.regUrl = Pattern.compile(".*/" + langType + "/statistics/engineproduction/engine_prod/.*");
		engine.groupRule = ".*/" + langType + "/statistics/engineproduction/engine_prod/.*";
		engine.startKey = "https://www.marklines.com/" + langType + "/statistics/engineproduction/engine_prod";
		engine.endKey = "https://www.marklines.com/" + langType + "/statistics/engineproduction/engine_proe";
		engine.addFetchListRule = engine.regUrl;
		engine.needAddList = true;
		groupCfg.add(engine);

		GroupConfig holcar = new GroupConfig();
		holcar.gid = 4;
		holcar.url = "";
		holcar.name = "holcar";
		holcar.regUrl = Pattern.compile("https://www.marklines.com/" + langType + "/global/\\d+");
		holcar.groupRule = "https://www.marklines.com/" + langType + "/global/\\d+";
		holcar.startKey = "https://www.marklines.com/" + langType + "/global/";
		holcar.endKey = "https://www.marklines.com/" + langType + "/global/99999";
		holcar.addFetchListRule = holcar.regUrl;
		groupCfg.add(holcar);

		GroupConfig part = new GroupConfig();
		part.gid = 5;
		part.url = "";
		part.name = "part";
		// part.regUrl =
		// Pattern.compile("(.*/"+langTyep+"/top\\d+/\\w\\d+_\\d+)|(.*/"+langTyep+"/supplier_db/partDetail\\?page=\\d.*)");
		part.groupRule = "https://www.marklines.com//" + langType
				+ "/supplier_db/\\d+/?no_frame=true&_is=true&isPartial=false&";
		part.startKey = "https://www.marklines.com/" + langType + "/supplier_db/partDetail?page";
		part.endKey = "https://www.marklines.com/" + langType + "/supplier_db/partDetail?pagf";

		// part.startKey = "https://www.marklines.com/"+langTyep+"/top500/";
		// part.endKey = "https://www.marklines.com/"+langTyep+"/top500/z";
		// part.addFetchListRule = Pattern
		// .compile("(.*/"+langTyep+"/top\\d+/\\w\\d+_\\d+)|(.*/"+langTyep+"/supplier_db/partDetail\\?page=\\d.*)");
		groupCfg.add(part);
		// //////////////////添加字段解析规则/////////////
		// 真实列表页面
		// https://www.marklines.com/"+langTyep+"/supplier_db/partDetail/top?_is=true&containsSub=true&isPartial=false&oth.place[]=a,71&oth.isPartial=true&page=1&size=10
		// 简介地址
		// https://www.marklines.com//"+langTyep+"/supplier_db/000000000019882/?no_frame=true&_is=true&isPartial=false&
		// 详细报告
		// https://www.marklines.com/"+langTyep+"/top500/s500_018
		// https://www.marklines.com/"+langTyep+"/top500/s500_515
		part = new GroupConfig();
		part.gid = 6;
		part.url = "";
		part.name = "partdetail";
		part.groupRule = "https://www.marklines.com/" + langType + "/top\\d+/\\w\\d+_\\d+";
		groupCfg.add(part);
	}

	static int allRecodsize = 0;

	// 解析分页数
	public static void parsePartPages(DataStore<String, WebPage> store, String key, WebPage page, int step)
			throws MalformedURLException {
		if (step == 2) {// partDetail 解析出分页数
			String keyFlag = "";
			Pattern kpar = Pattern
					.compile("(oth.place\\[\\]=(.*?)&oth.isPartial)|(oth.place%5B%5D=(.*?)&oth.isPartial)");
			Matcher km = kpar.matcher(key);
			if (km.find()) {
				keyFlag = Convert.ToString(km.group(2)) + Convert.ToString(km.group(4));
				LogUtils.info(" keyFlag=" + keyFlag + "  key=" + key);
			}
			String cnt = new String(page.getContent().array());
			int index = -1;
			if (UrlList.langType.equals("cn")) {
				index = cnt.indexOf("检索结果");
			} else if (UrlList.langType.equals("en")) {
				index = cnt.indexOf("Search Results");
			} else if (UrlList.langType.equals("ja")) {
				index = cnt.indexOf("class=\"result-count\">");
			}
			if (index == -1) {
				LogUtils.error("===key=" + key + "===解析分页数异常============================================");
			}
			int end = cnt.indexOf("更改检索条件");
			if (end < 10)
				end = cnt.length();
			cnt = cnt.substring(index, end);
			// String a = "<span class=\"result-count\">27</span><span class=\"result-count-unit\">条</span>";
			Pattern par = Pattern.compile("class=\"result-count\">(\\d+)</span><span");
			Matcher m = par.matcher(cnt);
			if (m.find()) {
				int recodSize = Convert.ToInt(m.group(1), 0);
				allRecodsize += recodSize;
				int pageNum = 0;
				while (pageNum * 10 < recodSize) {
					String subUrl = "https://www.marklines.com/" + langType
							+ "/supplier_db/partDetail/top?_is=true&containsSub=true&isPartial=false&oth.place%5B%5D="
							+ keyFlag + "&oth.isPartial=true&page=" + pageNum + "&size=10";
					WebPage subpage = new WebPage();
					subpage.setTitle(new Utf8("init"));
					subpage.setBaseUrl(new Utf8(subUrl));
					store.put(TableUtil.reverseUrl(subUrl), subpage);
					pageNum++;
				}
				LogUtils.info("keyFlag=" + keyFlag + " recodSize=" + recodSize + " pageNum=" + pageNum
						+ " allRecodsize=" + allRecodsize);
			}
		} else if (step == 3) {// partDetail 解析出简介及详细页
			String cnt = new String(page.getContent().array());
			Pattern par = Pattern.compile("detail-link-button\" href='(/" + langType + "/top\\d+/.\\d+_\\d+)'>");
			Matcher m = par.matcher(cnt);
			while (m.find()) {
				String href = m.group(1);
				String subUrl = "https://www.marklines.com" + href;
				WebPage subpage = new WebPage();
				subpage.setTitle(new Utf8("detail"));
				subpage.setBaseUrl(new Utf8(subUrl));
				LogUtils.info("detail=" + subUrl);
				store.put(TableUtil.reverseUrl(subUrl), subpage);
			}
			par = Pattern.compile("<div data-app=\"sub-ajax-container\" data-source=\"(/" + langType
					+ "/supplier_db/.*)\"></div>");
			m = par.matcher(cnt);
			while (m.find()) {
				String href = m.group(1);
				String subUrl = "https://www.marklines.com" + href;
				WebPage subpage = new WebPage();
				subpage.setTitle(new Utf8("info"));
				subpage.setBaseUrl(new Utf8(subUrl));
				LogUtils.info("info=" + subUrl);
				store.put(TableUtil.reverseUrl(subUrl), subpage);
			}
		}
	}

	public static void addUrls(boolean flags[]) {
		setGroupConfig();
		urlCfg cfg = null;
		// 产量
		if (flags[0]) {
			cfg = new urlCfg();
			cfg.url = groupCfg.get(0).url;// "https://www.marklines.com/"+langTyep+"/vehicle_production/";
			// cfg.regUrlName = Pattern.compile("(分车型)|(分整车厂)");
			cfg.regUrl = groupCfg.get(0).regUrl;// Pattern.compile(".*/"+langTyep+"/vehicle_production/year\\?nationCode=.*");
			add(cfg);
		}
		// // 销量
		if (flags[1]) {
			cfg = new urlCfg();
			cfg.url = groupCfg.get(1).url;// "https://www.marklines.com/"+langTyep+"/vehicle_sales/";
			// cfg.regUrlName = Pattern.compile("(分车型)|(分整车厂)");
			cfg.regUrl = groupCfg.get(1).regUrl;// Pattern.compile(".*/"+langTyep+"/vehicle_sales/year\\?nationCode=.*");
			// https://www.marklines.com/"+langTyep+"/vehicle_sales/year?nationCode=MYS&fromYear=2006&toYear=2015
			// https://www.marklines.com/"+langTyep+"/vehicle_sales/year?nationCode=SGP&fromYear=2006&toYear=2015
			add(cfg);
		}
		// 发动机
		if (flags[2]) {
			cfg = new urlCfg();
			cfg.url = groupCfg.get(2).url;// "https://www.marklines.com/"+langTyep+"/statistics/engineproduction/engineproduction";
			cfg.regUrl = groupCfg.get(2).regUrl;// Pattern.compile(".*/"+langTyep+"/statistics/engineproduction/engine_prod/.*");
			cfg.regUrlName = Pattern.compile(".*");
			add(cfg);
		}

		// 整车厂分布
		if (flags[3]) {
			String cnt = " href=\"/"
					+ langType
					+ "/global/3609\"  href=\"/"
					+ langType
					+ "/global/4315\"  href=\"/"
					+ langType
					+ "/global/3371\"  href=\"/"
					+ langType
					+ "/global/3611\"  href=\"/"
					+ langType
					+ "/global/3615\"  href=\"/"
					+ langType
					+ "/global/3621\"  href=\"/"
					+ langType
					+ "/global/3623\"  href=\"/"
					+ langType
					+ "/global/3687\"  href=\"/"
					+ langType
					+ "/global/3691\"  href=\"/"
					+ langType
					+ "/global/3697\"  href=\"/"
					+ langType
					+ "/global/3731\"  href=\"/"
					+ langType
					+ "/global/3735\"  href=\"/"
					+ langType
					+ "/global/3737\"  href=\"/"
					+ langType
					+ "/global/3739\"  href=\"/"
					+ langType
					+ "/global/3753\"  href=\"/"
					+ langType
					+ "/global/4153\"  href=\"/"
					+ langType
					+ "/global/4187\"  href=\"/"
					+ langType
					+ "/global/6451\"  href=\"/"
					+ langType
					+ "/global/8679\"  href=\"/"
					+ langType
					+ "/global/8736\"  href=\"/"
					+ langType
					+ "/global/8769\"  href=\"/"
					+ langType
					+ "/global/9039\"  href=\"/"
					+ langType
					+ "/global/9096\"  href=\"/"
					+ langType
					+ "/global/9108\"  href=\"/"
					+ langType
					+ "/global/3619\"  href=\"/"
					+ langType
					+ "/global/3699\"  href=\"/"
					+ langType
					+ "/global/4189\"  href=\"/"
					+ langType
					+ "/global/3617\"  href=\"/"
					+ langType
					+ "/global/9045\"  href=\"/"
					+ langType
					+ "/global/9228\"  href=\"/"
					+ langType
					+ "/global/285\"  href=\"/"
					+ langType
					+ "/global/1557\"  href=\"/"
					+ langType
					+ "/global/1157\"  href=\"/"
					+ langType
					+ "/global/1159\"  href=\"/"
					+ langType
					+ "/global/1161\"  href=\"/"
					+ langType
					+ "/global/2357\"  href=\"/"
					+ langType
					+ "/global/8889\"  href=\"/"
					+ langType
					+ "/global/3765\"  href=\"/"
					+ langType
					+ "/global/3971\"  href=\"/"
					+ langType
					+ "/global/3973\"  href=\"/"
					+ langType
					+ "/global/3975\"  href=\"/"
					+ langType
					+ "/global/3979\"  href=\"/"
					+ langType
					+ "/global/9282\"  href=\"/"
					+ langType
					+ "/global/3559\"  href=\"/"
					+ langType
					+ "/global/3767\"  href=\"/"
					+ langType
					+ "/global/3769\"  href=\"/"
					+ langType
					+ "/global/3793\"  href=\"/"
					+ langType
					+ "/global/3809\"  href=\"/"
					+ langType
					+ "/global/3823\"  href=\"/"
					+ langType
					+ "/global/3945\"  href=\"/"
					+ langType
					+ "/global/3951\"  href=\"/"
					+ langType
					+ "/global/3955\"  href=\"/"
					+ langType
					+ "/global/3977\"  href=\"/"
					+ langType
					+ "/global/3981\"  href=\"/"
					+ langType
					+ "/global/3983\"  href=\"/"
					+ langType
					+ "/global/3985\"  href=\"/"
					+ langType
					+ "/global/3995\"  href=\"/"
					+ langType
					+ "/global/4001\"  href=\"/"
					+ langType
					+ "/global/4003\"  href=\"/"
					+ langType
					+ "/global/4005\"  href=\"/"
					+ langType
					+ "/global/4007\"  href=\"/"
					+ langType
					+ "/global/4011\"  href=\"/"
					+ langType
					+ "/global/4017\"  href=\"/"
					+ langType
					+ "/global/4019\"  href=\"/"
					+ langType
					+ "/global/4021\"  href=\"/"
					+ langType
					+ "/global/4023\"  href=\"/"
					+ langType
					+ "/global/4025\"  href=\"/"
					+ langType
					+ "/global/4067\"  href=\"/"
					+ langType
					+ "/global/4101\"  href=\"/"
					+ langType
					+ "/global/4129\"  href=\"/"
					+ langType
					+ "/global/4145\"  href=\"/"
					+ langType
					+ "/global/4255\"  href=\"/"
					+ langType
					+ "/global/4285\"  href=\"/"
					+ langType
					+ "/global/4311\"  href=\"/"
					+ langType
					+ "/global/8853\"  href=\"/"
					+ langType
					+ "/global/9024\"  href=\"/"
					+ langType
					+ "/global/9165\"  href=\"/"
					+ langType
					+ "/global/9252\"  href=\"/"
					+ langType
					+ "/global/4027\"  href=\"/"
					+ langType
					+ "/global/4089\"  href=\"/"
					+ langType
					+ "/global/9207\"  href=\"/"
					+ langType
					+ "/global/4133\"  href=\"/"
					+ langType
					+ "/global/1981\"  href=\"/"
					+ langType
					+ "/global/8526\"  href=\"/"
					+ langType
					+ "/global/9162\"  href=\"/"
					+ langType
					+ "/global/965\"  href=\"/"
					+ langType
					+ "/global/2953\"  href=\"/"
					+ langType
					+ "/global/3001\"  href=\"/"
					+ langType
					+ "/global/1877\"  href=\"/"
					+ langType
					+ "/global/3449\"  href=\"/"
					+ langType
					+ "/global/3453\"  href=\"/"
					+ langType
					+ "/global/3905\"  href=\"/"
					+ langType
					+ "/global/4169\"  href=\"/"
					+ langType
					+ "/global/9405\"  href=\"/"
					+ langType
					+ "/global/3321\"  href=\"/"
					+ langType
					+ "/global/3451\"  href=\"/"
					+ langType
					+ "/global/3539\"  href=\"/"
					+ langType
					+ "/global/3541\"  href=\"/"
					+ langType
					+ "/global/3547\"  href=\"/"
					+ langType
					+ "/global/3741\"  href=\"/"
					+ langType
					+ "/global/3743\"  href=\"/"
					+ langType
					+ "/global/3875\"  href=\"/"
					+ langType
					+ "/global/3907\"  href=\"/"
					+ langType
					+ "/global/3909\"  href=\"/"
					+ langType
					+ "/global/3913\"  href=\"/"
					+ langType
					+ "/global/4127\"  href=\"/"
					+ langType
					+ "/global/4163\"  href=\"/"
					+ langType
					+ "/global/4165\"  href=\"/"
					+ langType
					+ "/global/4167\"  href=\"/"
					+ langType
					+ "/global/4171\"  href=\"/"
					+ langType
					+ "/global/8742\"  href=\"/"
					+ langType
					+ "/global/9408\"  href=\"/"
					+ langType
					+ "/global/9426\"  href=\"/"
					+ langType
					+ "/global/9429\"  href=\"/"
					+ langType
					+ "/global/3745\"  href=\"/"
					+ langType
					+ "/global/983\"  href=\"/"
					+ langType
					+ "/global/3333\"  href=\"/"
					+ langType
					+ "/global/3335\"  href=\"/"
					+ langType
					+ "/global/3493\"  href=\"/"
					+ langType
					+ "/global/3501\"  href=\"/"
					+ langType
					+ "/global/3323\"  href=\"/"
					+ langType
					+ "/global/3337\"  href=\"/"
					+ langType
					+ "/global/3339\"  href=\"/"
					+ langType
					+ "/global/3341\"  href=\"/"
					+ langType
					+ "/global/3343\"  href=\"/"
					+ langType
					+ "/global/3347\"  href=\"/"
					+ langType
					+ "/global/3349\"  href=\"/"
					+ langType
					+ "/global/3351\"  href=\"/"
					+ langType
					+ "/global/3357\"  href=\"/"
					+ langType
					+ "/global/3401\"  href=\"/"
					+ langType
					+ "/global/3489\"  href=\"/"
					+ langType
					+ "/global/3491\"  href=\"/"
					+ langType
					+ "/global/3495\"  href=\"/"
					+ langType
					+ "/global/3497\"  href=\"/"
					+ langType
					+ "/global/3575\"  href=\"/"
					+ langType
					+ "/global/3589\"  href=\"/"
					+ langType
					+ "/global/3689\"  href=\"/"
					+ langType
					+ "/global/3783\"  href=\"/"
					+ langType
					+ "/global/3857\"  href=\"/"
					+ langType
					+ "/global/4119\"  href=\"/"
					+ langType
					+ "/global/4139\"  href=\"/"
					+ langType
					+ "/global/4213\"  href=\"/"
					+ langType
					+ "/global/4215\"  href=\"/"
					+ langType
					+ "/global/4217\"  href=\"/"
					+ langType
					+ "/global/4231\"  href=\"/"
					+ langType
					+ "/global/4265\"  href=\"/"
					+ langType
					+ "/global/8568\"  href=\"/"
					+ langType
					+ "/global/9099\"  href=\"/"
					+ langType
					+ "/global/9294\"  href=\"/"
					+ langType
					+ "/global/9414\"  href=\"/"
					+ langType
					+ "/global/9444\"  href=\"/"
					+ langType
					+ "/global/3345\"  href=\"/"
					+ langType
					+ "/global/3403\"  href=\"/"
					+ langType
					+ "/global/3503\"  href=\"/"
					+ langType
					+ "/global/3505\"  href=\"/"
					+ langType
					+ "/global/3517\"  href=\"/"
					+ langType
					+ "/global/8709\"  href=\"/"
					+ langType
					+ "/global/803\"  href=\"/"
					+ langType
					+ "/global/1833\"  href=\"/"
					+ langType
					+ "/global/8907\"  href=\"/"
					+ langType
					+ "/global/3415\"  href=\"/"
					+ langType
					+ "/global/3417\"  href=\"/"
					+ langType
					+ "/global/3425\"  href=\"/"
					+ langType
					+ "/global/3431\"  href=\"/"
					+ langType
					+ "/global/3441\"  href=\"/"
					+ langType
					+ "/global/3923\"  href=\"/"
					+ langType
					+ "/global/3419\"  href=\"/"
					+ langType
					+ "/global/3421\"  href=\"/"
					+ langType
					+ "/global/3427\"  href=\"/"
					+ langType
					+ "/global/3429\"  href=\"/"
					+ langType
					+ "/global/3433\"  href=\"/"
					+ langType
					+ "/global/3435\"  href=\"/"
					+ langType
					+ "/global/3437\"  href=\"/"
					+ langType
					+ "/global/3469\"  href=\"/"
					+ langType
					+ "/global/3557\"  href=\"/"
					+ langType
					+ "/global/3685\"  href=\"/"
					+ langType
					+ "/global/3721\"  href=\"/"
					+ langType
					+ "/global/3925\"  href=\"/"
					+ langType
					+ "/global/4031\"  href=\"/"
					+ langType
					+ "/global/4059\"  href=\"/"
					+ langType
					+ "/global/4111\"  href=\"/"
					+ langType
					+ "/global/4113\"  href=\"/"
					+ langType
					+ "/global/4253\"  href=\"/"
					+ langType
					+ "/global/6427\"  href=\"/"
					+ langType
					+ "/global/8898\"  href=\"/"
					+ langType
					+ "/global/8901\"  href=\"/"
					+ langType
					+ "/global/9126\"  href=\"/"
					+ langType
					+ "/global/9129\"  href=\"/"
					+ langType
					+ "/global/9147\"  href=\"/"
					+ langType
					+ "/global/9174\"  href=\"/"
					+ langType
					+ "/global/9219\"  href=\"/"
					+ langType
					+ "/global/9246\"  href=\"/"
					+ langType
					+ "/global/9306\"  href=\"/"
					+ langType
					+ "/global/9369\"  href=\"/"
					+ langType
					+ "/global/9372\"  href=\"/"
					+ langType
					+ "/global/9417\"  href=\"/"
					+ langType
					+ "/global/9420\"  href=\"/"
					+ langType
					+ "/global/2041\"  href=\"/"
					+ langType
					+ "/global/1005\"  href=\"/"
					+ langType
					+ "/global/8997\"  href=\"/"
					+ langType
					+ "/global/9333\"  href=\"/"
					+ langType
					+ "/global/277\"  href=\"/"
					+ langType
					+ "/global/1621\"  href=\"/"
					+ langType
					+ "/global/8607\"  href=\"/"
					+ langType
					+ "/global/8964\"  href=\"/"
					+ langType
					+ "/global/9438\"  href=\"/"
					+ langType
					+ "/global/6441\"  href=\"/"
					+ langType
					+ "/global/9030\"  href=\"/"
					+ langType
					+ "/global/8664\"  href=\"/"
					+ langType
					+ "/global/9381\"  href=\"/"
					+ langType
					+ "/global/4073\"  href=\"/"
					+ langType
					+ "/global/4079\"  href=\"/"
					+ langType
					+ "/global/3353\"  href=\"/"
					+ langType
					+ "/global/3381\"  href=\"/"
					+ langType
					+ "/global/3683\"  href=\"/"
					+ langType
					+ "/global/3817\"  href=\"/"
					+ langType
					+ "/global/3851\"  href=\"/"
					+ langType
					+ "/global/3853\"  href=\"/"
					+ langType
					+ "/global/4035\"  href=\"/"
					+ langType
					+ "/global/4063\"  href=\"/"
					+ langType
					+ "/global/4075\"  href=\"/"
					+ langType
					+ "/global/4077\"  href=\"/"
					+ langType
					+ "/global/4081\"  href=\"/"
					+ langType
					+ "/global/4083\"  href=\"/"
					+ langType
					+ "/global/4087\"  href=\"/"
					+ langType
					+ "/global/4093\"  href=\"/"
					+ langType
					+ "/global/4105\"  href=\"/"
					+ langType
					+ "/global/8808\"  href=\"/"
					+ langType
					+ "/global/9243\"  href=\"/"
					+ langType
					+ "/global/9276\"  href=\"/"
					+ langType
					+ "/global/4097\"  href=\"/"
					+ langType
					+ "/global/3813\"  href=\"/"
					+ langType
					+ "/global/9273\"  href=\"/"
					+ langType
					+ "/global/3407\"  href=\"/"
					+ langType
					+ "/global/3605\"  href=\"/"
					+ langType
					+ "/global/3879\"  href=\"/"
					+ langType
					+ "/global/3883\"  href=\"/"
					+ langType
					+ "/global/3885\"  href=\"/"
					+ langType
					+ "/global/3969\"  href=\"/"
					+ langType
					+ "/global/8862\"  href=\"/"
					+ langType
					+ "/global/8895\"  href=\"/"
					+ langType
					+ "/global/9390\"  href=\"/"
					+ langType
					+ "/global/1977\"  href=\"/"
					+ langType
					+ "/global/949\"  href=\"/"
					+ langType
					+ "/global/1633\"  href=\"/"
					+ langType
					+ "/global/2823\"  href=\"/"
					+ langType
					+ "/global/9180\"  href=\"/"
					+ langType
					+ "/global/2765\"  href=\"/"
					+ langType
					+ "/global/2945\"  href=\"/"
					+ langType
					+ "/global/8586\"  href=\"/"
					+ langType
					+ "/global/1311\"  href=\"/"
					+ langType
					+ "/global/683\"  href=\"/"
					+ langType
					+ "/global/791\"  href=\"/"
					+ langType
					+ "/global/1841\"  href=\"/"
					+ langType
					+ "/global/3633\"  href=\"/"
					+ langType
					+ "/global/3807\"  href=\"/"
					+ langType
					+ "/global/3553\"  href=\"/"
					+ langType
					+ "/global/3631\"  href=\"/"
					+ langType
					+ "/global/3669\"  href=\"/"
					+ langType
					+ "/global/3831\"  href=\"/"
					+ langType
					+ "/global/3837\"  href=\"/"
					+ langType
					+ "/global/3849\"  href=\"/"
					+ langType
					+ "/global/4055\"  href=\"/"
					+ langType
					+ "/global/4233\"  href=\"/"
					+ langType
					+ "/global/4241\"  href=\"/"
					+ langType
					+ "/global/4303\"  href=\"/"
					+ langType
					+ "/global/6439\"  href=\"/"
					+ langType
					+ "/global/9075\"  href=\"/"
					+ langType
					+ "/global/9144\"  href=\"/"
					+ langType
					+ "/global/9327\"  href=\"/"
					+ langType
					+ "/global/9345\"  href=\"/"
					+ langType
					+ "/global/9354\"  href=\"/"
					+ langType
					+ "/global/9360\"  href=\"/"
					+ langType
					+ "/global/55\"  href=\"/"
					+ langType
					+ "/global/1017\"  href=\"/"
					+ langType
					+ "/global/8553\"  href=\"/"
					+ langType
					+ "/global/9324\"  href=\"/"
					+ langType
					+ "/global/9048\"  href=\"/"
					+ langType
					+ "/global/2349\"  href=\"/"
					+ langType
					+ "/global/9321\"  href=\"/"
					+ langType
					+ "/global/1512\"  href=\"/"
					+ langType
					+ "/global/2727\"  href=\"/"
					+ langType
					+ "/global/2729\"  href=\"/"
					+ langType
					+ "/global/9084\"  href=\"/"
					+ langType
					+ "/global/2731\"  href=\"/"
					+ langType
					+ "/global/2733\"  href=\"/"
					+ langType
					+ "/global/667\"  href=\"/"
					+ langType
					+ "/global/8904\"  href=\"/"
					+ langType
					+ "/global/3361\"  href=\"/"
					+ langType
					+ "/global/3363\"  href=\"/"
					+ langType
					+ "/global/3373\"  href=\"/"
					+ langType
					+ "/global/3365\"  href=\"/"
					+ langType
					+ "/global/3369\"  href=\"/"
					+ langType
					+ "/global/3375\"  href=\"/"
					+ langType
					+ "/global/3377\"  href=\"/"
					+ langType
					+ "/global/4243\"  href=\"/"
					+ langType
					+ "/global/8559\"  href=\"/"
					+ langType
					+ "/global/8619\"  href=\"/"
					+ langType
					+ "/global/8628\"  href=\"/"
					+ langType
					+ "/global/9105\"  href=\"/"
					+ langType
					+ "/global/8952\"  href=\"/"
					+ langType
					+ "/global/8637\"  href=\"/"
					+ langType
					+ "/global/717\"  href=\"/"
					+ langType
					+ "/global/8982\"  href=\"/"
					+ langType
					+ "/global/4307\"  href=\"/"
					+ langType
					+ "/global/4043\"  href=\"/"
					+ langType
					+ "/global/4125\"  href=\"/"
					+ langType
					+ "/global/4269\"  href=\"/"
					+ langType
					+ "/global/8955\"  href=\"/"
					+ langType
					+ "/global/9213\"  href=\"/"
					+ langType
					+ "/global/9258\"  href=\"/"
					+ langType
					+ "/global/9201\"  href=\"/"
					+ langType
					+ "/global/9198\"  href=\"/"
					+ langType
					+ "/global/789\"  href=\"/"
					+ langType
					+ "/global/3529\"  href=\"/"
					+ langType
					+ "/global/3533\"  href=\"/"
					+ langType
					+ "/global/8556\"  href=\"/"
					+ langType
					+ "/global/333\"  href=\"/"
					+ langType
					+ "/global/243\"  href=\"/"
					+ langType
					+ "/global/1553\"  href=\"/"
					+ langType
					+ "/global/9267\"  href=\"/"
					+ langType
					+ "/global/1873\"  href=\"/"
					+ langType
					+ "/global/3863\"  href=\"/"
					+ langType
					+ "/global/3723\"  href=\"/"
					+ langType
					+ "/global/3865\"  href=\"/"
					+ langType
					+ "/global/3867\"  href=\"/"
					+ langType
					+ "/global/3869\"  href=\"/"
					+ langType
					+ "/global/3871\"  href=\"/"
					+ langType
					+ "/global/3897\"  href=\"/"
					+ langType
					+ "/global/9033\"  href=\"/"
					+ langType
					+ "/global/9102\"  href=\"/"
					+ langType
					+ "/global/9357\"  href=\"/"
					+ langType
					+ "/global/9399\"  href=\"/"
					+ langType
					+ "/global/8856\"  href=\"/"
					+ langType
					+ "/global/9000\"  href=\"/"
					+ langType
					+ "/global/4287\"  href=\"/"
					+ langType
					+ "/global/8550\"  href=\"/"
					+ langType
					+ "/global/1823\"  href=\"/"
					+ langType
					+ "/global/4179\"  href=\"/"
					+ langType
					+ "/global/4181\"  href=\"/"
					+ langType
					+ "/global/4183\"  href=\"/"
					+ langType
					+ "/global/4267\"  href=\"/"
					+ langType
					+ "/global/9309\"  href=\"/"
					+ langType
					+ "/global/8498\"  href=\"/"
					+ langType
					+ "/global/2949\"  href=\"/"
					+ langType
					+ "/global/9300\"  href=\"/"
					+ langType
					+ "/global/3659\"  href=\"/"
					+ langType
					+ "/global/3661\"  href=\"/"
					+ langType
					+ "/global/3665\"  href=\"/"
					+ langType
					+ "/global/3695\"  href=\"/"
					+ langType
					+ "/global/3949\"  href=\"/"
					+ langType
					+ "/global/4147\"  href=\"/"
					+ langType
					+ "/global/4195\"  href=\"/"
					+ langType
					+ "/global/4223\"  href=\"/"
					+ langType
					+ "/global/8916\"  href=\"/"
					+ langType
					+ "/global/9063\"  href=\"/"
					+ langType
					+ "/global/9195\"  href=\"/"
					+ langType
					+ "/global/9135\"  href=\"/"
					+ langType
					+ "/global/9168\"  href=\"/"
					+ langType
					+ "/global/8730\"  href=\"/"
					+ langType
					+ "/global/3593\"  href=\"/"
					+ langType
					+ "/global/3595\"  href=\"/"
					+ langType
					+ "/global/3701\"  href=\"/"
					+ langType
					+ "/global/4199\"  href=\"/"
					+ langType
					+ "/global/8580\"  href=\"/"
					+ langType
					+ "/global/3523\"  href=\"/"
					+ langType
					+ "/global/3893\"  href=\"/"
					+ langType
					+ "/global/3895\"  href=\"/"
					+ langType
					+ "/global/4061\"  href=\"/"
					+ langType
					+ "/global/4271\"  href=\"/"
					+ langType
					+ "/global/3859\"  href=\"/"
					+ langType
					+ "/global/4045\"  href=\"/"
					+ langType
					+ "/global/4273\"  href=\"/"
					+ langType
					+ "/global/4275\"  href=\"/"
					+ langType
					+ "/global/4279\"  href=\"/"
					+ langType
					+ "/global/4299\"  href=\"/"
					+ langType
					+ "/global/8892\"  href=\"/"
					+ langType
					+ "/global/9027\"  href=\"/"
					+ langType
					+ "/global/9339\"  href=\"/"
					+ langType
					+ "/global/8766\"  href=\"/"
					+ langType
					+ "/global/3391\"  href=\"/"
					+ langType
					+ "/global/3393\"  href=\"/"
					+ langType
					+ "/global/3395\"  href=\"/"
					+ langType
					+ "/global/3397\"  href=\"/"
					+ langType
					+ "/global/3399\"  href=\"/"
					+ langType
					+ "/global/3791\"  href=\"/"
					+ langType
					+ "/global/3457\"  href=\"/"
					+ langType
					+ "/global/3777\"  href=\"/"
					+ langType
					+ "/global/3779\"  href=\"/"
					+ langType
					+ "/global/3749\"  href=\"/"
					+ langType
					+ "/global/3803\"  href=\"/"
					+ langType
					+ "/global/3939\"  href=\"/"
					+ langType
					+ "/global/3941\"  href=\"/"
					+ langType
					+ "/global/3943\"  href=\"/"
					+ langType
					+ "/global/9423\"  href=\"/"
					+ langType
					+ "/global/2063\"  href=\"/"
					+ langType
					+ "/global/3555\"  href=\"/"
					+ langType
					+ "/global/3957\"  href=\"/"
					+ langType
					+ "/global/3965\"  href=\"/"
					+ langType
					+ "/global/8520\"  href=\"/"
					+ langType
					+ "/global/3959\"  href=\"/"
					+ langType
					+ "/global/9375\"  href=\"/"
					+ langType
					+ "/global/3481\"  href=\"/"
					+ langType
					+ "/global/3405\"  href=\"/"
					+ langType
					+ "/global/8946\"  href=\"/"
					+ langType
					+ "/global/2121\"  href=\"/"
					+ langType
					+ "/global/2119\"  href=\"/"
					+ langType
					+ "/global/2123\"  href=\"/"
					+ langType
					+ "/global/9009\"  href=\"/"
					+ langType
					+ "/global/1001\"  href=\"/"
					+ langType
					+ "/global/929\"  href=\"/"
					+ langType
					+ "/global/4291\"  href=\"/"
					+ langType
					+ "/global/9240\"  href=\"/"
					+ langType
					+ "/global/1301\"  href=\"/"
					+ langType
					+ "/global/1304\"  href=\"/"
					+ langType
					+ "/global/1305\"  href=\"/"
					+ langType
					+ "/global/1303\"  href=\"/"
					+ langType
					+ "/global/1306\"  href=\"/"
					+ langType
					+ "/global/8691\"  href=\"/"
					+ langType
					+ "/global/3307\"  href=\"/"
					+ langType
					+ "/global/3309\"  href=\"/"
					+ langType
					+ "/global/909\"  href=\"/"
					+ langType
					+ "/global/8739\"  href=\"/"
					+ langType
					+ "/global/885\"  href=\"/"
					+ langType
					+ "/global/911\"  href=\"/"
					+ langType
					+ "/global/913\"  href=\"/"
					+ langType
					+ "/global/2929\"  href=\"/"
					+ langType
					+ "/global/2881\"  href=\"/"
					+ langType
					+ "/global/2911\"  href=\"/"
					+ langType
					+ "/global/2931\"  href=\"/"
					+ langType
					+ "/global/2933\"  href=\"/"
					+ langType
					+ "/global/2935\"  href=\"/"
					+ langType
					+ "/global/2937\"  href=\"/"
					+ langType
					+ "/global/2813\"  href=\"/"
					+ langType
					+ "/global/2815\"  href=\"/"
					+ langType
					+ "/global/2807\"  href=\"/"
					+ langType
					+ "/global/3031\"  href=\"/"
					+ langType
					+ "/global/2169\"  href=\"/"
					+ langType
					+ "/global/2187\"  href=\"/"
					+ langType
					+ "/global/2197\"  href=\"/"
					+ langType
					+ "/global/2259\"  href=\"/"
					+ langType
					+ "/global/2165\"  href=\"/"
					+ langType
					+ "/global/2171\"  href=\"/"
					+ langType
					+ "/global/2173\"  href=\"/"
					+ langType
					+ "/global/2189\"  href=\"/"
					+ langType
					+ "/global/2191\"  href=\"/"
					+ langType
					+ "/global/2199\"  href=\"/"
					+ langType
					+ "/global/2201\"  href=\"/"
					+ langType
					+ "/global/2239\"  href=\"/"
					+ langType
					+ "/global/2261\"  href=\"/"
					+ langType
					+ "/global/2267\"  href=\"/"
					+ langType
					+ "/global/2269\"  href=\"/"
					+ langType
					+ "/global/2275\"  href=\"/"
					+ langType
					+ "/global/2277\"  href=\"/"
					+ langType
					+ "/global/2175\"  href=\"/"
					+ langType
					+ "/global/2271\"  href=\"/"
					+ langType
					+ "/global/2279\"  href=\"/"
					+ langType
					+ "/global/2281\"  href=\"/"
					+ langType
					+ "/global/2265\"  href=\"/"
					+ langType
					+ "/global/2273\"  href=\"/"
					+ langType
					+ "/global/8685\"  href=\"/"
					+ langType
					+ "/global/115\"  href=\"/"
					+ langType
					+ "/global/63\"  href=\"/"
					+ langType
					+ "/global/1953\"  href=\"/"
					+ langType
					+ "/global/1955\"  href=\"/"
					+ langType
					+ "/global/1965\"  href=\"/"
					+ langType
					+ "/global/1961\"  href=\"/"
					+ langType
					+ "/global/1957\"  href=\"/"
					+ langType
					+ "/global/1357\"  href=\"/"
					+ langType
					+ "/global/1384\"  href=\"/"
					+ langType
					+ "/global/1378\"  href=\"/"
					+ langType
					+ "/global/1514\"  href=\"/"
					+ langType
					+ "/global/1487\"  href=\"/"
					+ langType
					+ "/global/8814\"  href=\"/"
					+ langType
					+ "/global/1815\"  href=\"/"
					+ langType
					+ "/global/2693\"  href=\"/"
					+ langType
					+ "/global/2695\"  href=\"/"
					+ langType
					+ "/global/2697\"  href=\"/"
					+ langType
					+ "/global/815\"  href=\"/"
					+ langType
					+ "/global/6443\"  href=\"/"
					+ langType
					+ "/global/701\"  href=\"/"
					+ langType
					+ "/global/769\"  href=\"/"
					+ langType
					+ "/global/817\"  href=\"/"
					+ langType
					+ "/global/1675\"  href=\"/"
					+ langType
					+ "/global/1679\"  href=\"/"
					+ langType
					+ "/global/1695\"  href=\"/"
					+ langType
					+ "/global/1711\"  href=\"/"
					+ langType
					+ "/global/9216\"  href=\"/"
					+ langType
					+ "/global/1715\"  href=\"/"
					+ langType
					+ "/global/1691\"  href=\"/"
					+ langType
					+ "/global/1737\"  href=\"/"
					+ langType
					+ "/global/1739\"  href=\"/"
					+ langType
					+ "/global/1741\"  href=\"/"
					+ langType
					+ "/global/1743\"  href=\"/"
					+ langType
					+ "/global/1769\"  href=\"/"
					+ langType
					+ "/global/1771\"  href=\"/"
					+ langType
					+ "/global/1773\"  href=\"/"
					+ langType
					+ "/global/1777\"  href=\"/"
					+ langType
					+ "/global/1837\"  href=\"/"
					+ langType
					+ "/global/9021\"  href=\"/"
					+ langType
					+ "/global/1893\"  href=\"/"
					+ langType
					+ "/global/1430\"  href=\"/"
					+ langType
					+ "/global/627\"  href=\"/"
					+ langType
					+ "/global/643\"  href=\"/"
					+ langType
					+ "/global/655\"  href=\"/"
					+ langType
					+ "/global/659\"  href=\"/"
					+ langType
					+ "/global/9312\"  href=\"/"
					+ langType
					+ "/global/2703\"  href=\"/"
					+ langType
					+ "/global/1149\"  href=\"/"
					+ langType
					+ "/global/1683\"  href=\"/"
					+ langType
					+ "/global/623\"  href=\"/"
					+ langType
					+ "/global/625\"  href=\"/"
					+ langType
					+ "/global/9003\"  href=\"/"
					+ langType
					+ "/global/3483\"  href=\"/"
					+ langType
					+ "/global/3933\"  href=\"/"
					+ langType
					+ "/global/35\"  href=\"/"
					+ langType
					+ "/global/37\"  href=\"/"
					+ langType
					+ "/global/9\"  href=\"/"
					+ langType
					+ "/global/579\"  href=\"/"
					+ langType
					+ "/global/581\"  href=\"/"
					+ langType
					+ "/global/589\"  href=\"/"
					+ langType
					+ "/global/585\"  href=\"/"
					+ langType
					+ "/global/2075\"  href=\"/"
					+ langType
					+ "/global/933\"  href=\"/"
					+ langType
					+ "/global/321\"  href=\"/"
					+ langType
					+ "/global/273\"  href=\"/"
					+ langType
					+ "/global/325\"  href=\"/"
					+ langType
					+ "/global/329\"  href=\"/"
					+ langType
					+ "/global/231\"  href=\"/"
					+ langType
					+ "/global/1593\"  href=\"/"
					+ langType
					+ "/global/1125\"  href=\"/"
					+ langType
					+ "/global/1131\"  href=\"/"
					+ langType
					+ "/global/1129\"  href=\"/"
					+ langType
					+ "/global/1133\"  href=\"/"
					+ langType
					+ "/global/1249\"  href=\"/"
					+ langType
					+ "/global/1041\"  href=\"/"
					+ langType
					+ "/global/3047\"  href=\"/"
					+ langType
					+ "/global/3051\"  href=\"/"
					+ langType
					+ "/global/3049\"  href=\"/"
					+ langType
					+ "/global/3055\"  href=\"/"
					+ langType
					+ "/global/3057\"  href=\"/"
					+ langType
					+ "/global/3061\"  href=\"/"
					+ langType
					+ "/global/3069\"  href=\"/"
					+ langType
					+ "/global/3073\"  href=\"/"
					+ langType
					+ "/global/3081\"  href=\"/"
					+ langType
					+ "/global/3191\"  href=\"/"
					+ langType
					+ "/global/3059\"  href=\"/"
					+ langType
					+ "/global/3065\"  href=\"/"
					+ langType
					+ "/global/6449\"  href=\"/"
					+ langType
					+ "/global/845\"  href=\"/"
					+ langType
					+ "/global/849\"  href=\"/"
					+ langType
					+ "/global/9249\"  href=\"/"
					+ langType
					+ "/global/2827\"  href=\"/"
					+ langType
					+ "/global/2829\"  href=\"/"
					+ langType
					+ "/global/9159\"  href=\"/"
					+ langType
					+ "/global/2769\"  href=\"/"
					+ langType
					+ "/global/2803\"  href=\"/"
					+ langType
					+ "/global/8694\"  href=\"/"
					+ langType
					+ "/global/9342\"  href=\"/"
					+ langType
					+ "/global/3021\"  href=\"/"
					+ langType
					+ "/global/2133\"  href=\"/"
					+ langType
					+ "/global/2221\"  href=\"/"
					+ langType
					+ "/global/2135\"  href=\"/"
					+ langType
					+ "/global/2137\"  href=\"/"
					+ langType
					+ "/global/2139\"  href=\"/"
					+ langType
					+ "/global/2223\"  href=\"/"
					+ langType
					+ "/global/2225\"  href=\"/"
					+ langType
					+ "/global/2237\"  href=\"/"
					+ langType
					+ "/global/2241\"  href=\"/"
					+ langType
					+ "/global/2243\"  href=\"/"
					+ langType
					+ "/global/2227\"  href=\"/"
					+ langType
					+ "/global/2233\"  href=\"/"
					+ langType
					+ "/global/2235\"  href=\"/"
					+ langType
					+ "/global/9315\"  href=\"/"
					+ langType
					+ "/global/2229\"  href=\"/"
					+ langType
					+ "/global/2231\"  href=\"/"
					+ langType
					+ "/global/2245\"  href=\"/"
					+ langType
					+ "/global/2247\"  href=\"/"
					+ langType
					+ "/global/8517\"  href=\"/"
					+ langType
					+ "/global/9396\"  href=\"/"
					+ langType
					+ "/global/121\"  href=\"/"
					+ langType
					+ "/global/103\"  href=\"/"
					+ langType
					+ "/global/123\"  href=\"/"
					+ langType
					+ "/global/173\"  href=\"/"
					+ langType
					+ "/global/67\"  href=\"/"
					+ langType
					+ "/global/1917\"  href=\"/"
					+ langType
					+ "/global/1921\"  href=\"/"
					+ langType
					+ "/global/1925\"  href=\"/"
					+ langType
					+ "/global/1897\"  href=\"/"
					+ langType
					+ "/global/1388\"  href=\"/"
					+ langType
					+ "/global/1809\"  href=\"/"
					+ langType
					+ "/global/2749\"  href=\"/"
					+ langType
					+ "/global/737\"  href=\"/"
					+ langType
					+ "/global/741\"  href=\"/"
					+ langType
					+ "/global/9441\"  href=\"/"
					+ langType
					+ "/global/1723\"  href=\"/"
					+ langType
					+ "/global/1781\"  href=\"/"
					+ langType
					+ "/global/1889\"  href=\"/"
					+ langType
					+ "/global/1431\"  href=\"/"
					+ langType
					+ "/global/1433\"  href=\"/"
					+ langType
					+ "/global/1434\"  href=\"/"
					+ langType
					+ "/global/609\"  href=\"/"
					+ langType
					+ "/global/6433\"  href=\"/"
					+ langType
					+ "/global/9297\"  href=\"/"
					+ langType
					+ "/global/327\"  href=\"/"
					+ langType
					+ "/global/8718\"  href=\"/"
					+ langType
					+ "/global/1440\"  href=\"/"
					+ langType
					+ "/global/1386\"  href=\"/"
					+ langType
					+ "/global/3485\"  href=\"/"
					+ langType
					+ "/global/1971\"  href=\"/"
					+ langType
					+ "/global/1973\"  href=\"/"
					+ langType
					+ "/global/1121\"  href=\"/"
					+ langType
					+ "/global/1123\"  href=\"/"
					+ langType
					+ "/global/3045\"  href=\"/"
					+ langType
					+ "/global/9231\"  href=\"/"
					+ langType
					+ "/global/9255\"  href=\"/"
					+ langType
					+ "/global/8991\"  href=\"/"
					+ langType
					+ "/global/2203\"  href=\"/"
					+ langType
					+ "/global/2127\"  href=\"/"
					+ langType
					+ "/global/2205\"  href=\"/"
					+ langType
					+ "/global/2207\"  href=\"/"
					+ langType
					+ "/global/2209\"  href=\"/"
					+ langType
					+ "/global/2215\"  href=\"/"
					+ langType
					+ "/global/2211\"  href=\"/"
					+ langType
					+ "/global/2213\"  href=\"/"
					+ langType
					+ "/global/2217\"  href=\"/"
					+ langType
					+ "/global/2219\"  href=\"/"
					+ langType
					+ "/global/4305\"  href=\"/"
					+ langType
					+ "/global/2283\"  href=\"/"
					+ langType
					+ "/global/2373\"  href=\"/"
					+ langType
					+ "/global/2285\"  href=\"/"
					+ langType
					+ "/global/2375\"  href=\"/"
					+ langType
					+ "/global/2289\"  href=\"/"
					+ langType
					+ "/global/2293\"  href=\"/"
					+ langType
					+ "/global/1485\"  href=\"/"
					+ langType
					+ "/global/1801\"  href=\"/"
					+ langType
					+ "/global/671\"  href=\"/"
					+ langType
					+ "/global/599\"  href=\"/"
					+ langType
					+ "/global/515\"  href=\"/"
					+ langType
					+ "/global/517\"  href=\"/"
					+ langType
					+ "/global/977\"  href=\"/"
					+ langType
					+ "/global/2869\"  href=\"/"
					+ langType
					+ "/global/2903\"  href=\"/"
					+ langType
					+ "/global/2795\"  href=\"/"
					+ langType
					+ "/global/2797\"  href=\"/"
					+ langType
					+ "/global/2799\"  href=\"/"
					+ langType
					+ "/global/129\"  href=\"/"
					+ langType
					+ "/global/131\"  href=\"/"
					+ langType
					+ "/global/133\"  href=\"/"
					+ langType
					+ "/global/119\"  href=\"/"
					+ langType
					+ "/global/137\"  href=\"/"
					+ langType
					+ "/global/139\"  href=\"/"
					+ langType
					+ "/global/141\"  href=\"/"
					+ langType
					+ "/global/143\"  href=\"/"
					+ langType
					+ "/global/151\"  href=\"/"
					+ langType
					+ "/global/159\"  href=\"/"
					+ langType
					+ "/global/99\"  href=\"/"
					+ langType
					+ "/global/147\"  href=\"/"
					+ langType
					+ "/global/153\"  href=\"/"
					+ langType
					+ "/global/161\"  href=\"/"
					+ langType
					+ "/global/187\"  href=\"/"
					+ langType
					+ "/global/149\"  href=\"/"
					+ langType
					+ "/global/155\"  href=\"/"
					+ langType
					+ "/global/157\"  href=\"/"
					+ langType
					+ "/global/9189\"  href=\"/"
					+ langType
					+ "/global/1931\"  href=\"/"
					+ langType
					+ "/global/1935\"  href=\"/"
					+ langType
					+ "/global/1939\"  href=\"/"
					+ langType
					+ "/global/1375\"  href=\"/"
					+ langType
					+ "/global/1392\"  href=\"/"
					+ langType
					+ "/global/753\"  href=\"/"
					+ langType
					+ "/global/1735\"  href=\"/"
					+ langType
					+ "/global/1767\"  href=\"/"
					+ langType
					+ "/global/1428\"  href=\"/"
					+ langType
					+ "/global/1443\"  href=\"/"
					+ langType
					+ "/global/6429\"  href=\"/"
					+ langType
					+ "/global/2423\"  href=\"/"
					+ langType
					+ "/global/2425\"  href=\"/"
					+ langType
					+ "/global/1088\"  href=\"/"
					+ langType
					+ "/global/1089\"  href=\"/"
					+ langType
					+ "/global/895\"  href=\"/"
					+ langType
					+ "/global/2907\"  href=\"/"
					+ langType
					+ "/global/2963\"  href=\"/"
					+ langType
					+ "/global/2979\"  href=\"/"
					+ langType
					+ "/global/163\"  href=\"/"
					+ langType
					+ "/global/165\"  href=\"/"
					+ langType
					+ "/global/167\"  href=\"/"
					+ langType
					+ "/global/169\"  href=\"/"
					+ langType
					+ "/global/171\"  href=\"/"
					+ langType
					+ "/global/175\"  href=\"/"
					+ langType
					+ "/global/179\"  href=\"/"
					+ langType
					+ "/global/177\"  href=\"/"
					+ langType
					+ "/global/183\"  href=\"/"
					+ langType
					+ "/global/4295\"  href=\"/"
					+ langType
					+ "/global/8514\"  href=\"/"
					+ langType
					+ "/global/1941\"  href=\"/"
					+ langType
					+ "/global/1927\"  href=\"/"
					+ langType
					+ "/global/1943\"  href=\"/"
					+ langType
					+ "/global/1947\"  href=\"/"
					+ langType
					+ "/global/1945\"  href=\"/"
					+ langType
					+ "/global/1394\"  href=\"/"
					+ langType
					+ "/global/673\"  href=\"/"
					+ langType
					+ "/global/675\"  href=\"/"
					+ langType
					+ "/global/729\"  href=\"/"
					+ langType
					+ "/global/757\"  href=\"/"
					+ langType
					+ "/global/9132\"  href=\"/"
					+ langType
					+ "/global/1847\"  href=\"/"
					+ langType
					+ "/global/1849\"  href=\"/"
					+ langType
					+ "/global/1857\"  href=\"/"
					+ langType
					+ "/global/1438\"  href=\"/"
					+ langType
					+ "/global/635\"  href=\"/"
					+ langType
					+ "/global/6431\"  href=\"/"
					+ langType
					+ "/global/727\"  href=\"/"
					+ langType
					+ "/global/679\"  href=\"/"
					+ langType
					+ "/global/8697\"  href=\"/"
					+ langType
					+ "/global/8700\"  href=\"/"
					+ langType
					+ "/global/8994\"  href=\"/"
					+ langType
					+ "/global/9285\"  href=\"/"
					+ langType
					+ "/global/1825\"  href=\"/"
					+ langType
					+ "/global/3657\"  href=\"/"
					+ langType
					+ "/global/1279\"  href=\"/"
					+ langType
					+ "/global/2623\"  href=\"/"
					+ langType
					+ "/global/2625\"  href=\"/"
					+ langType
					+ "/global/2627\"  href=\"/"
					+ langType
					+ "/global/2641\"  href=\"/"
					+ langType
					+ "/global/2647\"  href=\"/"
					+ langType
					+ "/global/2653\"  href=\"/"
					+ langType
					+ "/global/2655\"  href=\"/"
					+ langType
					+ "/global/2663\"  href=\"/"
					+ langType
					+ "/global/2631\"  href=\"/"
					+ langType
					+ "/global/2637\"  href=\"/"
					+ langType
					+ "/global/2645\"  href=\"/"
					+ langType
					+ "/global/2658\"  href=\"/"
					+ langType
					+ "/global/2659\"  href=\"/"
					+ langType
					+ "/global/2661\"  href=\"/"
					+ langType
					+ "/global/2662\"  href=\"/"
					+ langType
					+ "/global/2635\"  href=\"/"
					+ langType
					+ "/global/2643\"  href=\"/"
					+ langType
					+ "/global/2649\"  href=\"/"
					+ langType
					+ "/global/2651\"  href=\"/"
					+ langType
					+ "/global/2669\"  href=\"/"
					+ langType
					+ "/global/2671\"  href=\"/"
					+ langType
					+ "/global/2675\"  href=\"/"
					+ langType
					+ "/global/2673\"  href=\"/"
					+ langType
					+ "/global/837\"  href=\"/"
					+ langType
					+ "/global/839\"  href=\"/"
					+ langType
					+ "/global/843\"  href=\"/"
					+ langType
					+ "/global/840\"  href=\"/"
					+ langType
					+ "/global/841\"  href=\"/"
					+ langType
					+ "/global/842\"  href=\"/"
					+ langType
					+ "/global/844\"  href=\"/"
					+ langType
					+ "/global/2831\"  href=\"/"
					+ langType
					+ "/global/2833\"  href=\"/"
					+ langType
					+ "/global/2834\"  href=\"/"
					+ langType
					+ "/global/2773\"  href=\"/"
					+ langType
					+ "/global/2997\"  href=\"/"
					+ langType
					+ "/global/1317\"  href=\"/"
					+ langType
					+ "/global/1319\"  href=\"/"
					+ langType
					+ "/global/1321\"  href=\"/"
					+ langType
					+ "/global/1307\"  href=\"/"
					+ langType
					+ "/global/1323\"  href=\"/"
					+ langType
					+ "/global/1325\"  href=\"/"
					+ langType
					+ "/global/1327\"  href=\"/"
					+ langType
					+ "/global/1329\"  href=\"/"
					+ langType
					+ "/global/1361\"  href=\"/"
					+ langType
					+ "/global/9225\"  href=\"/"
					+ langType
					+ "/global/8703\"  href=\"/"
					+ langType
					+ "/global/1655\"  href=\"/"
					+ langType
					+ "/global/1881\"  href=\"/"
					+ langType
					+ "/global/8880\"  href=\"/"
					+ langType
					+ "/global/781\"  href=\"/"
					+ langType
					+ "/global/1793\"  href=\"/"
					+ langType
					+ "/global/1315\"  href=\"/"
					+ langType
					+ "/global/9288\"  href=\"/"
					+ langType
					+ "/global/1079\"  href=\"/"
					+ langType
					+ "/global/2789\"  href=\"/"
					+ langType
					+ "/global/3013\"  href=\"/"
					+ langType
					+ "/global/87\"  href=\"/"
					+ langType
					+ "/global/91\"  href=\"/"
					+ langType
					+ "/global/1911\"  href=\"/"
					+ langType
					+ "/global/1913\"  href=\"/"
					+ langType
					+ "/global/1915\"  href=\"/"
					+ langType
					+ "/global/1349\"  href=\"/"
					+ langType
					+ "/global/1351\"  href=\"/"
					+ langType
					+ "/global/1353\"  href=\"/"
					+ langType
					+ "/global/9411\"  href=\"/"
					+ langType
					+ "/global/807\"  href=\"/"
					+ langType
					+ "/global/1731\"  href=\"/"
					+ langType
					+ "/global/1883\"  href=\"/"
					+ langType
					+ "/global/9006\"  href=\"/"
					+ langType
					+ "/global/2923\"  href=\"/"
					+ langType
					+ "/global/8835\"  href=\"/"
					+ langType
					+ "/global/8838\"  href=\"/"
					+ langType
					+ "/global/95\"  href=\"/"
					+ langType
					+ "/global/1333\"  href=\"/"
					+ langType
					+ "/global/1335\"  href=\"/"
					+ langType
					+ "/global/1339\"  href=\"/"
					+ langType
					+ "/global/1341\"  href=\"/"
					+ langType
					+ "/global/1343\"  href=\"/"
					+ langType
					+ "/global/1345\"  href=\"/"
					+ langType
					+ "/global/9177\"  href=\"/"
					+ langType
					+ "/global/1337\"  href=\"/"
					+ langType
					+ "/global/1347\"  href=\"/"
					+ langType
					+ "/global/1659\"  href=\"/"
					+ langType
					+ "/global/4277\"  href=\"/"
					+ langType
					+ "/global/9402\"  href=\"/"
					+ langType
					+ "/global/3\"  href=\"/"
					+ langType
					+ "/global/8913\"  href=\"/"
					+ langType
					+ "/global/571\"  href=\"/"
					+ langType
					+ "/global/573\"  href=\"/"
					+ langType
					+ "/global/577\"  href=\"/"
					+ langType
					+ "/global/2111\"  href=\"/"
					+ langType
					+ "/global/341\"  href=\"/"
					+ langType
					+ "/global/191\"  href=\"/"
					+ langType
					+ "/global/1137\"  href=\"/"
					+ langType
					+ "/global/1139\"  href=\"/"
					+ langType
					+ "/global/1295\"  href=\"/"
					+ langType
					+ "/global/1299\"  href=\"/"
					+ langType
					+ "/global/1049\"  href=\"/"
					+ langType
					+ "/global/1091\"  href=\"/"
					+ langType
					+ "/global/3287\"  href=\"/"
					+ langType
					+ "/global/3291\"  href=\"/"
					+ langType
					+ "/global/8919\"  href=\"/"
					+ langType
					+ "/global/3295\"  href=\"/"
					+ langType
					+ "/global/3301\"  href=\"/"
					+ langType
					+ "/global/3299\"  href=\"/"
					+ langType
					+ "/global/3303\"  href=\"/"
					+ langType
					+ "/global/3305\"  href=\"/"
					+ langType
					+ "/global/917\"  href=\"/"
					+ langType
					+ "/global/2941\"  href=\"/"
					+ langType
					+ "/global/8667\"  href=\"/"
					+ langType
					+ "/global/3025\"  href=\"/"
					+ langType
					+ "/global/105\"  href=\"/"
					+ langType
					+ "/global/109\"  href=\"/"
					+ langType
					+ "/global/111\"  href=\"/"
					+ langType
					+ "/global/107\"  href=\"/"
					+ langType
					+ "/global/1510\"  href=\"/"
					+ langType
					+ "/global/2705\"  href=\"/"
					+ langType
					+ "/global/2707\"  href=\"/"
					+ langType
					+ "/global/2713\"  href=\"/"
					+ langType
					+ "/global/2721\"  href=\"/"
					+ langType
					+ "/global/2709\"  href=\"/"
					+ langType
					+ "/global/2725\"  href=\"/"
					+ langType
					+ "/global/2723\"  href=\"/"
					+ langType
					+ "/global/2711\"  href=\"/"
					+ langType
					+ "/global/2715\"  href=\"/"
					+ langType
					+ "/global/2719\"  href=\"/"
					+ langType
					+ "/global/813\"  href=\"/"
					+ langType
					+ "/global/1707\"  href=\"/"
					+ langType
					+ "/global/639\"  href=\"/"
					+ langType
					+ "/global/663\"  href=\"/"
					+ langType
					+ "/global/8922\"  href=\"/"
					+ langType
					+ "/global/1534\"  href=\"/"
					+ langType
					+ "/global/1535\"  href=\"/"
					+ langType
					+ "/global/9384\"  href=\"/"
					+ langType
					+ "/global/3411\"  href=\"/"
					+ langType
					+ "/global/1193\"  href=\"/"
					+ langType
					+ "/global/735\"  href=\"/"
					+ langType
					+ "/global/9057\"  href=\"/"
					+ langType
					+ "/global/3653\"  href=\"/"
					+ langType
					+ "/global/2401\"  href=\"/"
					+ langType
					+ "/global/2403\"  href=\"/"
					+ langType
					+ "/global/2405\"  href=\"/"
					+ langType
					+ "/global/2407\"  href=\"/"
					+ langType
					+ "/global/2409\"  href=\"/"
					+ langType
					+ "/global/1993\"  href=\"/"
					+ langType
					+ "/global/1995\"  href=\"/"
					+ langType
					+ "/global/279\"  href=\"/"
					+ langType
					+ "/global/281\"  href=\"/"
					+ langType
					+ "/global/1565\"  href=\"/"
					+ langType
					+ "/global/1073\"  href=\"/"
					+ langType
					+ "/global/1075\"  href=\"/"
					+ langType
					+ "/global/1077\"  href=\"/"
					+ langType
					+ "/global/2455\"  href=\"/"
					+ langType
					+ "/global/8532\"  href=\"/"
					+ langType
					+ "/global/2459\"  href=\"/"
					+ langType
					+ "/global/2461\"  href=\"/"
					+ langType
					+ "/global/2473\"  href=\"/"
					+ langType
					+ "/global/2475\"  href=\"/"
					+ langType
					+ "/global/2479\"  href=\"/"
					+ langType
					+ "/global/2495\"  href=\"/"
					+ langType
					+ "/global/2509\"  href=\"/"
					+ langType
					+ "/global/2517\"  href=\"/"
					+ langType
					+ "/global/2519\"  href=\"/"
					+ langType
					+ "/global/2521\"  href=\"/"
					+ langType
					+ "/global/2523\"  href=\"/"
					+ langType
					+ "/global/2525\"  href=\"/"
					+ langType
					+ "/global/2465\"  href=\"/"
					+ langType
					+ "/global/2485\"  href=\"/"
					+ langType
					+ "/global/2501\"  href=\"/"
					+ langType
					+ "/global/2531\"  href=\"/"
					+ langType
					+ "/global/8544\"  href=\"/"
					+ langType
					+ "/global/2489\"  href=\"/"
					+ langType
					+ "/global/2505\"  href=\"/"
					+ langType
					+ "/global/2535\"  href=\"/"
					+ langType
					+ "/global/2453\"  href=\"/"
					+ langType
					+ "/global/2457\"  href=\"/"
					+ langType
					+ "/global/2466\"  href=\"/"
					+ langType
					+ "/global/2467\"  href=\"/"
					+ langType
					+ "/global/2483\"  href=\"/"
					+ langType
					+ "/global/2487\"  href=\"/"
					+ langType
					+ "/global/2493\"  href=\"/"
					+ langType
					+ "/global/2503\"  href=\"/"
					+ langType
					+ "/global/2507\"  href=\"/"
					+ langType
					+ "/global/2513\"  href=\"/"
					+ langType
					+ "/global/8535\"  href=\"/"
					+ langType
					+ "/global/8538\"  href=\"/"
					+ langType
					+ "/global/8541\"  href=\"/"
					+ langType
					+ "/global/2539\"  href=\"/"
					+ langType
					+ "/global/2541\"  href=\"/"
					+ langType
					+ "/global/2543\"  href=\"/"
					+ langType
					+ "/global/2549\"  href=\"/"
					+ langType
					+ "/global/2547\"  href=\"/"
					+ langType
					+ "/global/865\"  href=\"/"
					+ langType
					+ "/global/867\"  href=\"/"
					+ langType
					+ "/global/869\"  href=\"/"
					+ langType
					+ "/global/873\"  href=\"/"
					+ langType
					+ "/global/871\"  href=\"/"
					+ langType
					+ "/global/2843\"  href=\"/"
					+ langType
					+ "/global/2845\"  href=\"/"
					+ langType
					+ "/global/2847\"  href=\"/"
					+ langType
					+ "/global/2849\"  href=\"/"
					+ langType
					+ "/global/2853\"  href=\"/"
					+ langType
					+ "/global/2851\"  href=\"/"
					+ langType
					+ "/global/2781\"  href=\"/"
					+ langType
					+ "/global/2955\"  href=\"/"
					+ langType
					+ "/global/2967\"  href=\"/"
					+ langType
					+ "/global/3009\"  href=\"/"
					+ langType
					+ "/global/2987\"  href=\"/"
					+ langType
					+ "/global/2248\"  href=\"/"
					+ langType
					+ "/global/2249\"  href=\"/"
					+ langType
					+ "/global/2251\"  href=\"/"
					+ langType
					+ "/global/2253\"  href=\"/"
					+ langType
					+ "/global/2257\"  href=\"/"
					+ langType
					+ "/global/8970\"  href=\"/"
					+ langType
					+ "/global/2387\"  href=\"/"
					+ langType
					+ "/global/2389\"  href=\"/"
					+ langType
					+ "/global/2393\"  href=\"/"
					+ langType
					+ "/global/1805\"  href=\"/"
					+ langType
					+ "/global/705\"  href=\"/"
					+ langType
					+ "/global/1687\"  href=\"/"
					+ langType
					+ "/global/1671\"  href=\"/"
					+ langType
					+ "/global/1789\"  href=\"/"
					+ langType
					+ "/global/9012\"  href=\"/"
					+ langType
					+ "/global/9015\"  href=\"/"
					+ langType
					+ "/global/617\"  href=\"/"
					+ langType
					+ "/global/9171\"  href=\"/"
					+ langType
					+ "/global/619\"  href=\"/"
					+ langType
					+ "/global/621\"  href=\"/"
					+ langType
					+ "/global/2353\"  href=\"/"
					+ langType
					+ "/global/2015\"  href=\"/"
					+ langType
					+ "/global/3655\"  href=\"/"
					+ langType
					+ "/global/15\"  href=\"/"
					+ langType
					+ "/global/17\"  href=\"/"
					+ langType
					+ "/global/1983\"  href=\"/"
					+ langType
					+ "/global/1987\"  href=\"/"
					+ langType
					+ "/global/1985\"  href=\"/"
					+ langType
					+ "/global/1989\"  href=\"/"
					+ langType
					+ "/global/1991\"  href=\"/"
					+ langType
					+ "/global/1561\"  href=\"/"
					+ langType
					+ "/global/1153\"  href=\"/"
					+ langType
					+ "/global/1155\"  href=\"/"
					+ langType
					+ "/global/1156\"  href=\"/"
					+ langType
					+ "/global/1067\"  href=\"/"
					+ langType
					+ "/global/1069\"  href=\"/"
					+ langType
					+ "/global/1071\"  href=\"/"
					+ langType
					+ "/global/2557\"  href=\"/"
					+ langType
					+ "/global/2559\"  href=\"/"
					+ langType
					+ "/global/2569\"  href=\"/"
					+ langType
					+ "/global/2589\"  href=\"/"
					+ langType
					+ "/global/2595\"  href=\"/"
					+ langType
					+ "/global/2599\"  href=\"/"
					+ langType
					+ "/global/2605\"  href=\"/"
					+ langType
					+ "/global/2607\"  href=\"/"
					+ langType
					+ "/global/3041\"  href=\"/"
					+ langType
					+ "/global/2563\"  href=\"/"
					+ langType
					+ "/global/2573\"  href=\"/"
					+ langType
					+ "/global/2585\"  href=\"/"
					+ langType
					+ "/global/2587\"  href=\"/"
					+ langType
					+ "/global/2567\"  href=\"/"
					+ langType
					+ "/global/2577\"  href=\"/"
					+ langType
					+ "/global/2591\"  href=\"/"
					+ langType
					+ "/global/2561\"  href=\"/"
					+ langType
					+ "/global/2565\"  href=\"/"
					+ langType
					+ "/global/2571\"  href=\"/"
					+ langType
					+ "/global/2575\"  href=\"/"
					+ langType
					+ "/global/2581\"  href=\"/"
					+ langType
					+ "/global/2582\"  href=\"/"
					+ langType
					+ "/global/2593\"  href=\"/"
					+ langType
					+ "/global/2597\"  href=\"/"
					+ langType
					+ "/global/2609\"  href=\"/"
					+ langType
					+ "/global/2611\"  href=\"/"
					+ langType
					+ "/global/2617\"  href=\"/"
					+ langType
					+ "/global/2615\"  href=\"/"
					+ langType
					+ "/global/2621\"  href=\"/"
					+ langType
					+ "/global/855\"  href=\"/"
					+ langType
					+ "/global/857\"  href=\"/"
					+ langType
					+ "/global/859\"  href=\"/"
					+ langType
					+ "/global/889\"  href=\"/"
					+ langType
					+ "/global/9435\"  href=\"/"
					+ langType
					+ "/global/861\"  href=\"/"
					+ langType
					+ "/global/863\"  href=\"/"
					+ langType
					+ "/global/2835\"  href=\"/"
					+ langType
					+ "/global/2837\"  href=\"/"
					+ langType
					+ "/global/2839\"  href=\"/"
					+ langType
					+ "/global/2927\"  href=\"/"
					+ langType
					+ "/global/2841\"  href=\"/"
					+ langType
					+ "/global/2777\"  href=\"/"
					+ langType
					+ "/global/3005\"  href=\"/"
					+ langType
					+ "/global/2141\"  href=\"/"
					+ langType
					+ "/global/2143\"  href=\"/"
					+ langType
					+ "/global/2145\"  href=\"/"
					+ langType
					+ "/global/2149\"  href=\"/"
					+ langType
					+ "/global/2153\"  href=\"/"
					+ langType
					+ "/global/9087\"  href=\"/"
					+ langType
					+ "/global/75\"  href=\"/"
					+ langType
					+ "/global/71\"  href=\"/"
					+ langType
					+ "/global/1901\"  href=\"/"
					+ langType
					+ "/global/2305\"  href=\"/"
					+ langType
					+ "/global/2309\"  href=\"/"
					+ langType
					+ "/global/2311\"  href=\"/"
					+ langType
					+ "/global/2315\"  href=\"/"
					+ langType
					+ "/global/2737\"  href=\"/"
					+ langType
					+ "/global/4313\"  href=\"/"
					+ langType
					+ "/global/687\"  href=\"/"
					+ langType
					+ "/global/697\"  href=\"/"
					+ langType
					+ "/global/777\"  href=\"/"
					+ langType
					+ "/global/1755\"  href=\"/"
					+ langType
					+ "/global/1861\"  href=\"/"
					+ langType
					+ "/global/1418\"  href=\"/"
					+ langType
					+ "/global/1419\"  href=\"/"
					+ langType
					+ "/global/1420\"  href=\"/"
					+ langType
					+ "/global/8682\"  href=\"/"
					+ langType
					+ "/global/611\"  href=\"/"
					+ langType
					+ "/global/613\"  href=\"/"
					+ langType
					+ "/global/615\"  href=\"/"
					+ langType
					+ "/global/3217\"  href=\"/"
					+ langType
					+ "/global/3283\"  href=\"/"
					+ langType
					+ "/global/9141\"  href=\"/"
					+ langType
					+ "/global/1213\"  href=\"/"
					+ langType
					+ "/global/1201\"  href=\"/"
					+ langType
					+ "/global/1214\"  href=\"/"
					+ langType
					+ "/global/3155\"  href=\"/"
					+ langType
					+ "/global/3157\"  href=\"/"
					+ langType
					+ "/global/3175\"  href=\"/"
					+ langType
					+ "/global/3165\"  href=\"/"
					+ langType
					+ "/global/3179\"  href=\"/"
					+ langType
					+ "/global/3159\"  href=\"/"
					+ langType
					+ "/global/3171\"  href=\"/"
					+ langType
					+ "/global/3172\"  href=\"/"
					+ langType
					+ "/global/3169\"  href=\"/"
					+ langType
					+ "/global/3177\"  href=\"/"
					+ langType
					+ "/global/2895\"  href=\"/"
					+ langType
					+ "/global/2819\"  href=\"/"
					+ langType
					+ "/global/2897\"  href=\"/"
					+ langType
					+ "/global/2899\"  href=\"/"
					+ langType
					+ "/global/2793\"  href=\"/"
					+ langType
					+ "/global/8646\"  href=\"/"
					+ langType
					+ "/global/19\"  href=\"/"
					+ langType
					+ "/global/23\"  href=\"/"
					+ langType
					+ "/global/1083\"  href=\"/"
					+ langType
					+ "/global/3193\"  href=\"/"
					+ langType
					+ "/global/3195\"  href=\"/"
					+ langType
					+ "/global/3203\"  href=\"/"
					+ langType
					+ "/global/3197\"  href=\"/"
					+ langType
					+ "/global/3199\"  href=\"/"
					+ langType
					+ "/global/3207\"  href=\"/"
					+ langType
					+ "/global/3201\"  href=\"/"
					+ langType
					+ "/global/3211\"  href=\"/"
					+ langType
					+ "/global/899\"  href=\"/"
					+ langType
					+ "/global/8589\"  href=\"/"
					+ langType
					+ "/global/2367\"  href=\"/"
					+ langType
					+ "/global/1499\"  href=\"/"
					+ langType
					+ "/global/1483\"  href=\"/"
					+ langType
					+ "/global/3471\"  href=\"/"
					+ langType
					+ "/global/8910\"  href=\"/"
					+ langType
					+ "/global/3507\"  href=\"/"
					+ langType
					+ "/global/3509\"  href=\"/"
					+ langType
					+ "/global/3511\"  href=\"/"
					+ langType
					+ "/global/3513\"  href=\"/"
					+ langType
					+ "/global/3515\"  href=\"/"
					+ langType
					+ "/global/29\"  href=\"/"
					+ langType
					+ "/global/31\"  href=\"/"
					+ langType
					+ "/global/33\"  href=\"/"
					+ langType
					+ "/global/369\"  href=\"/"
					+ langType
					+ "/global/391\"  href=\"/"
					+ langType
					+ "/global/407\"  href=\"/"
					+ langType
					+ "/global/418\"  href=\"/"
					+ langType
					+ "/global/373\"  href=\"/"
					+ langType
					+ "/global/375\"  href=\"/"
					+ langType
					+ "/global/379\"  href=\"/"
					+ langType
					+ "/global/381\"  href=\"/"
					+ langType
					+ "/global/393\"  href=\"/"
					+ langType
					+ "/global/409\"  href=\"/"
					+ langType
					+ "/global/411\"  href=\"/"
					+ langType
					+ "/global/413\"  href=\"/"
					+ langType
					+ "/global/415\"  href=\"/"
					+ langType
					+ "/global/417\"  href=\"/"
					+ langType
					+ "/global/420\"  href=\"/"
					+ langType
					+ "/global/424\"  href=\"/"
					+ langType
					+ "/global/426\"  href=\"/"
					+ langType
					+ "/global/433\"  href=\"/"
					+ langType
					+ "/global/529\"  href=\"/"
					+ langType
					+ "/global/539\"  href=\"/"
					+ langType
					+ "/global/541\"  href=\"/"
					+ langType
					+ "/global/547\"  href=\"/"
					+ langType
					+ "/global/567\"  href=\"/"
					+ langType
					+ "/global/383\"  href=\"/"
					+ langType
					+ "/global/387\"  href=\"/"
					+ langType
					+ "/global/395\"  href=\"/"
					+ langType
					+ "/global/422\"  href=\"/"
					+ langType
					+ "/global/401\"  href=\"/"
					+ langType
					+ "/global/371\"  href=\"/"
					+ langType
					+ "/global/377\"  href=\"/"
					+ langType
					+ "/global/385\"  href=\"/"
					+ langType
					+ "/global/388\"  href=\"/"
					+ langType
					+ "/global/389\"  href=\"/"
					+ langType
					+ "/global/390\"  href=\"/"
					+ langType
					+ "/global/397\"  href=\"/"
					+ langType
					+ "/global/2085\"  href=\"/"
					+ langType
					+ "/global/2087\"  href=\"/"
					+ langType
					+ "/global/2089\"  href=\"/"
					+ langType
					+ "/global/2091\"  href=\"/"
					+ langType
					+ "/global/2095\"  href=\"/"
					+ langType
					+ "/global/2103\"  href=\"/"
					+ langType
					+ "/global/1999\"  href=\"/"
					+ langType
					+ "/global/9456\"  href=\"/"
					+ langType
					+ "/global/1013\"  href=\"/"
					+ langType
					+ "/global/9453\"  href=\"/"
					+ langType
					+ "/global/359\"  href=\"/"
					+ langType
					+ "/global/267\"  href=\"/"
					+ langType
					+ "/global/289\"  href=\"/"
					+ langType
					+ "/global/363\"  href=\"/"
					+ langType
					+ "/global/367\"  href=\"/"
					+ langType
					+ "/global/4301\"  href=\"/"
					+ langType
					+ "/global/361\"  href=\"/"
					+ langType
					+ "/global/245\"  href=\"/"
					+ langType
					+ "/global/249\"  href=\"/"
					+ langType
					+ "/global/253\"  href=\"/"
					+ langType
					+ "/global/1569\"  href=\"/"
					+ langType
					+ "/global/1617\"  href=\"/"
					+ langType
					+ "/global/1285\"  href=\"/"
					+ langType
					+ "/global/1287\"  href=\"/"
					+ langType
					+ "/global/1291\"  href=\"/"
					+ langType
					+ "/global/1065\"  href=\"/"
					+ langType
					+ "/global/1087\"  href=\"/"
					+ langType
					+ "/global/3227\"  href=\"/"
					+ langType
					+ "/global/3229\"  href=\"/"
					+ langType
					+ "/global/3215\"  href=\"/"
					+ langType
					+ "/global/3233\"  href=\"/"
					+ langType
					+ "/global/3237\"  href=\"/"
					+ langType
					+ "/global/3241\"  href=\"/"
					+ langType
					+ "/global/3245\"  href=\"/"
					+ langType
					+ "/global/3261\"  href=\"/"
					+ langType
					+ "/global/3265\"  href=\"/"
					+ langType
					+ "/global/3249\"  href=\"/"
					+ langType
					+ "/global/3251\"  href=\"/"
					+ langType
					+ "/global/3253\"  href=\"/"
					+ langType
					+ "/global/3255\"  href=\"/"
					+ langType
					+ "/global/3257\"  href=\"/"
					+ langType
					+ "/global/3267\"  href=\"/"
					+ langType
					+ "/global/3105\"  href=\"/"
					+ langType
					+ "/global/3269\"  href=\"/"
					+ langType
					+ "/global/3271\"  href=\"/"
					+ langType
					+ "/global/3275\"  href=\"/"
					+ langType
					+ "/global/8480\"  href=\"/"
					+ langType
					+ "/global/907\"  href=\"/"
					+ langType
					+ "/global/9330\"  href=\"/"
					+ langType
					+ "/global/2913\"  href=\"/"
					+ langType
					+ "/global/2915\"  href=\"/"
					+ langType
					+ "/global/2917\"  href=\"/"
					+ langType
					+ "/global/8934\"  href=\"/"
					+ langType
					+ "/global/2919\"  href=\"/"
					+ langType
					+ "/global/2811\"  href=\"/"
					+ langType
					+ "/global/3029\"  href=\"/"
					+ langType
					+ "/global/127\"  href=\"/"
					+ langType
					+ "/global/1396\"  href=\"/"
					+ langType
					+ "/global/2377\"  href=\"/"
					+ langType
					+ "/global/2379\"  href=\"/"
					+ langType
					+ "/global/2381\"  href=\"/"
					+ langType
					+ "/global/4309\"  href=\"/"
					+ langType
					+ "/global/795\"  href=\"/"
					+ langType
					+ "/global/1699\"  href=\"/"
					+ langType
					+ "/global/1703\"  href=\"/"
					+ langType
					+ "/global/1445\"  href=\"/"
					+ langType
					+ "/global/651\"  href=\"/"
					+ langType
					+ "/global/537\"  href=\"/"
					+ langType
					+ "/global/545\"  href=\"/"
					+ langType
					+ "/global/543\"  href=\"/"
					+ langType
					+ "/global/549\"  href=\"/"
					+ langType
					+ "/global/985\"  href=\"/"
					+ langType
					+ "/global/9078\"  href=\"/"
					+ langType
					+ "/global/987\"  href=\"/"
					+ langType
					+ "/global/9234\"  href=\"/"
					+ langType
					+ "/global/265\"  href=\"/"
					+ langType
					+ "/global/269\"  href=\"/"
					+ langType
					+ "/global/3637\"  href=\"/"
					+ langType
					+ "/global/563\"  href=\"/"
					+ langType
					+ "/global/591\"  href=\"/"
					+ langType
					+ "/global/565\"  href=\"/"
					+ langType
					+ "/global/570\"  href=\"/"
					+ langType
					+ "/global/593\"  href=\"/"
					+ langType
					+ "/global/595\"  href=\"/"
					+ langType
					+ "/global/569\"  href=\"/"
					+ langType
					+ "/global/1997\"  href=\"/"
					+ langType
					+ "/global/2001\"  href=\"/"
					+ langType
					+ "/global/8477\"  href=\"/"
					+ langType
					+ "/global/8973\"  href=\"/"
					+ langType
					+ "/global/211\"  href=\"/"
					+ langType
					+ "/global/1025\"  href=\"/"
					+ langType
					+ "/global/3097\"  href=\"/"
					+ langType
					+ "/global/3099\"  href=\"/"
					+ langType
					+ "/global/3101\"  href=\"/"
					+ langType
					+ "/global/877\"  href=\"/"
					+ langType
					+ "/global/2971\"  href=\"/"
					+ langType
					+ "/global/3475\"  href=\"/"
					+ langType
					+ "/global/53\"  href=\"/"
					+ langType
					+ "/global/457\"  href=\"/"
					+ langType
					+ "/global/471\"  href=\"/"
					+ langType
					+ "/global/461\"  href=\"/"
					+ langType
					+ "/global/463\"  href=\"/"
					+ langType
					+ "/global/465\"  href=\"/"
					+ langType
					+ "/global/473\"  href=\"/"
					+ langType
					+ "/global/475\"  href=\"/"
					+ langType
					+ "/global/479\"  href=\"/"
					+ langType
					+ "/global/487\"  href=\"/"
					+ langType
					+ "/global/495\"  href=\"/"
					+ langType
					+ "/global/497\"  href=\"/"
					+ langType
					+ "/global/511\"  href=\"/"
					+ langType
					+ "/global/553\"  href=\"/"
					+ langType
					+ "/global/459\"  href=\"/"
					+ langType
					+ "/global/467\"  href=\"/"
					+ langType
					+ "/global/469\"  href=\"/"
					+ langType
					+ "/global/2047\"  href=\"/"
					+ langType
					+ "/global/2045\"  href=\"/"
					+ langType
					+ "/global/2051\"  href=\"/"
					+ langType
					+ "/global/1003\"  href=\"/"
					+ langType
					+ "/global/1007\"  href=\"/"
					+ langType
					+ "/global/1009\"  href=\"/"
					+ langType
					+ "/global/335\"  href=\"/"
					+ langType
					+ "/global/337\"  href=\"/"
					+ langType
					+ "/global/239\"  href=\"/"
					+ langType
					+ "/global/257\"  href=\"/"
					+ langType
					+ "/global/1609\"  href=\"/"
					+ langType
					+ "/global/1221\"  href=\"/"
					+ langType
					+ "/global/1103\"  href=\"/"
					+ langType
					+ "/global/1107\"  href=\"/"
					+ langType
					+ "/global/1223\"  href=\"/"
					+ langType
					+ "/global/1093\"  href=\"/"
					+ langType
					+ "/global/3185\"  href=\"/"
					+ langType
					+ "/global/3187\"  href=\"/"
					+ langType
					+ "/global/3189\"  href=\"/"
					+ langType
					+ "/global/891\"  href=\"/"
					+ langType
					+ "/global/8688\"  href=\"/"
					+ langType
					+ "/global/893\"  href=\"/"
					+ langType
					+ "/global/8604\"  href=\"/"
					+ langType
					+ "/global/1923\"  href=\"/"
					+ langType
					+ "/global/9210\"  href=\"/"
					+ langType
					+ "/global/2359\"  href=\"/"
					+ langType
					+ "/global/2361\"  href=\"/"
					+ langType
					+ "/global/9204\"  href=\"/"
					+ langType
					+ "/global/749\"  href=\"/"
					+ langType
					+ "/global/8883\"  href=\"/"
					+ langType
					+ "/global/3473\"  href=\"/"
					+ langType
					+ "/global/4117\"  href=\"/"
					+ langType
					+ "/global/4091\"  href=\"/"
					+ langType
					+ "/global/25\"  href=\"/"
					+ langType
					+ "/global/27\"  href=\"/"
					+ langType
					+ "/global/435\"  href=\"/"
					+ langType
					+ "/global/451\"  href=\"/"
					+ langType
					+ "/global/437\"  href=\"/"
					+ langType
					+ "/global/439\"  href=\"/"
					+ langType
					+ "/global/443\"  href=\"/"
					+ langType
					+ "/global/453\"  href=\"/"
					+ langType
					+ "/global/441\"  href=\"/"
					+ langType
					+ "/global/447\"  href=\"/"
					+ langType
					+ "/global/445\"  href=\"/"
					+ langType
					+ "/global/449\"  href=\"/"
					+ langType
					+ "/global/2003\"  href=\"/"
					+ langType
					+ "/global/2005\"  href=\"/"
					+ langType
					+ "/global/2007\"  href=\"/"
					+ langType
					+ "/global/2009\"  href=\"/"
					+ langType
					+ "/global/2011\"  href=\"/"
					+ langType
					+ "/global/961\"  href=\"/"
					+ langType
					+ "/global/293\"  href=\"/"
					+ langType
					+ "/global/8802\"  href=\"/"
					+ langType
					+ "/global/215\"  href=\"/"
					+ langType
					+ "/global/219\"  href=\"/"
					+ langType
					+ "/global/1573\"  href=\"/"
					+ langType
					+ "/global/1169\"  href=\"/"
					+ langType
					+ "/global/1171\"  href=\"/"
					+ langType
					+ "/global/1173\"  href=\"/"
					+ langType
					+ "/global/1029\"  href=\"/"
					+ langType
					+ "/global/3107\"  href=\"/"
					+ langType
					+ "/global/3109\"  href=\"/"
					+ langType
					+ "/global/3111\"  href=\"/"
					+ langType
					+ "/global/3112\"  href=\"/"
					+ langType
					+ "/global/3117\"  href=\"/"
					+ langType
					+ "/global/3121\"  href=\"/"
					+ langType
					+ "/global/3113\"  href=\"/"
					+ langType
					+ "/global/3133\"  href=\"/"
					+ langType
					+ "/global/3137\"  href=\"/"
					+ langType
					+ "/global/3125\"  href=\"/"
					+ langType
					+ "/global/880\"  href=\"/"
					+ langType
					+ "/global/881\"  href=\"/"
					+ langType
					+ "/global/882\"  href=\"/"
					+ langType
					+ "/global/9318\"  href=\"/"
					+ langType
					+ "/global/2857\"  href=\"/"
					+ langType
					+ "/global/9123\"  href=\"/"
					+ langType
					+ "/global/2785\"  href=\"/"
					+ langType
					+ "/global/2319\"  href=\"/"
					+ langType
					+ "/global/1503\"  href=\"/"
					+ langType
					+ "/global/1422\"  href=\"/"
					+ langType
					+ "/global/3477\"  href=\"/"
					+ langType
					+ "/global/493\"  href=\"/"
					+ langType
					+ "/global/499\"  href=\"/"
					+ langType
					+ "/global/9138\"  href=\"/"
					+ langType
					+ "/global/2067\"  href=\"/"
					+ langType
					+ "/global/351\"  href=\"/"
					+ langType
					+ "/global/353\"  href=\"/"
					+ langType
					+ "/global/357\"  href=\"/"
					+ langType
					+ "/global/355\"  href=\"/"
					+ langType
					+ "/global/8643\"  href=\"/"
					+ langType
					+ "/global/1251\"  href=\"/"
					+ langType
					+ "/global/1253\"  href=\"/"
					+ langType
					+ "/global/1255\"  href=\"/"
					+ langType
					+ "/global/1256\"  href=\"/"
					+ langType
					+ "/global/1257\"  href=\"/"
					+ langType
					+ "/global/1061\"  href=\"/"
					+ langType
					+ "/global/8748\"  href=\"/"
					+ langType
					+ "/global/8886\"  href=\"/"
					+ langType
					+ "/global/3649\"  href=\"/"
					+ langType
					+ "/global/501\"  href=\"/"
					+ langType
					+ "/global/503\"  href=\"/"
					+ langType
					+ "/global/505\"  href=\"/"
					+ langType
					+ "/global/507\"  href=\"/"
					+ langType
					+ "/global/9066\"  href=\"/"
					+ langType
					+ "/global/8961\"  href=\"/"
					+ langType
					+ "/global/8592\"  href=\"/"
					+ langType
					+ "/global/2975\"  href=\"/"
					+ langType
					+ "/global/2983\"  href=\"/"
					+ langType
					+ "/global/8751\"  href=\"/"
					+ langType
					+ "/global/3651\"  href=\"/"
					+ langType
					+ "/global/3931\"  href=\"/"
					+ langType
					+ "/global/3325\"  href=\"/"
					+ langType
					+ "/global/3387\"  href=\"/"
					+ langType
					+ "/global/5\"  href=\"/"
					+ langType
					+ "/global/7\"  href=\"/"
					+ langType
					+ "/global/513\"  href=\"/"
					+ langType
					+ "/global/525\"  href=\"/"
					+ langType
					+ "/global/519\"  href=\"/"
					+ langType
					+ "/global/521\"  href=\"/"
					+ langType
					+ "/global/522\"  href=\"/"
					+ langType
					+ "/global/2031\"  href=\"/"
					+ langType
					+ "/global/2033\"  href=\"/"
					+ langType
					+ "/global/2037\"  href=\"/"
					+ langType
					+ "/global/995\"  href=\"/"
					+ langType
					+ "/global/9279\"  href=\"/"
					+ langType
					+ "/global/207\"  href=\"/"
					+ langType
					+ "/global/235\"  href=\"/"
					+ langType
					+ "/global/1597\"  href=\"/"
					+ langType
					+ "/global/1163\"  href=\"/"
					+ langType
					+ "/global/1165\"  href=\"/"
					+ langType
					+ "/global/3151\"  href=\"/"
					+ langType
					+ "/global/3153\"  href=\"/"
					+ langType
					+ "/global/2889\"  href=\"/"
					+ langType
					+ "/global/3479\"  href=\"/"
					+ langType
					+ "/global/3693\"  href=\"/"
					+ langType
					+ "/global/527\"  href=\"/"
					+ langType
					+ "/global/531\"  href=\"/"
					+ langType
					+ "/global/533\"  href=\"/"
					+ langType
					+ "/global/535\"  href=\"/"
					+ langType
					+ "/global/9111\"  href=\"/"
					+ langType
					+ "/global/3445\"  href=\"/"
					+ langType
					+ "/global/3915\"  href=\"/"
					+ langType
					+ "/global/4175\"  href=\"/"
					+ langType
					+ "/global/4177\"  href=\"/"
					+ langType
					+ "/global/8931\"  href=\"/"
					+ langType
					+ "/global/551\"  href=\"/"
					+ langType
					+ "/global/555\"  href=\"/"
					+ langType
					+ "/global/561\"  href=\"/"
					+ langType
					+ "/global/2013\"  href=\"/"
					+ langType
					+ "/global/2017\"  href=\"/"
					+ langType
					+ "/global/2021\"  href=\"/"
					+ langType
					+ "/global/2025\"  href=\"/"
					+ langType
					+ "/global/2029\"  href=\"/"
					+ langType
					+ "/global/8976\"  href=\"/"
					+ langType
					+ "/global/8985\"  href=\"/"
					+ langType
					+ "/global/311\"  href=\"/"
					+ langType
					+ "/global/315\"  href=\"/"
					+ langType
					+ "/global/223\"  href=\"/"
					+ langType
					+ "/global/227\"  href=\"/"
					+ langType
					+ "/global/1581\"  href=\"/"
					+ langType
					+ "/global/1242\"  href=\"/"
					+ langType
					+ "/global/1241\"  href=\"/"
					+ langType
					+ "/global/1244\"  href=\"/"
					+ langType
					+ "/global/1037\"  href=\"/"
					+ langType
					+ "/global/3317\"  href=\"/"
					+ langType
					+ "/global/6437\"  href=\"/"
					+ langType
					+ "/global/8712\"  href=\"/"
					+ langType
					+ "/global/9291\"  href=\"/"
					+ langType
					+ "/global/799\"  href=\"/"
					+ langType
					+ "/global/9018\"  href=\"/"
					+ langType
					+ "/global/1426\"  href=\"/"
					+ langType
					+ "/global/3359\"  href=\"/"
					+ langType
					+ "/global/3713\"  href=\"/"
					+ langType
					+ "/global/4239\"  href=\"/"
					+ langType
					+ "/global/49\"  href=\"/"
					+ langType
					+ "/global/51\"  href=\"/"
					+ langType
					+ "/global/2433\"  href=\"/"
					+ langType
					+ "/global/2441\"  href=\"/"
					+ langType
					+ "/global/2435\"  href=\"/"
					+ langType
					+ "/global/2437\"  href=\"/"
					+ langType
					+ "/global/2439\"  href=\"/"
					+ langType
					+ "/global/2443\"  href=\"/"
					+ langType
					+ "/global/2445\"  href=\"/"
					+ langType
					+ "/global/2447\"  href=\"/"
					+ langType
					+ "/global/2451\"  href=\"/"
					+ langType
					+ "/global/297\"  href=\"/"
					+ langType
					+ "/global/195\"  href=\"/"
					+ langType
					+ "/global/1577\"  href=\"/"
					+ langType
					+ "/global/9303\"  href=\"/"
					+ langType
					+ "/global/1175\"  href=\"/"
					+ langType
					+ "/global/1177\"  href=\"/"
					+ langType
					+ "/global/3141\"  href=\"/"
					+ langType
					+ "/global/3145\"  href=\"/"
					+ langType
					+ "/global/9270\"  href=\"/"
					+ langType
					+ "/global/2861\"  href=\"/"
					+ langType
					+ "/global/2865\"  href=\"/"
					+ langType
					+ "/global/2991\"  href=\"/"
					+ langType
					+ "/global/709\"  href=\"/"
					+ langType
					+ "/global/713\"  href=\"/"
					+ langType
					+ "/global/8721\"  href=\"/"
					+ langType
					+ "/global/1727\"  href=\"/"
					+ langType
					+ "/global/1424\"  href=\"/"
					+ langType
					+ "/global/9264\"  href=\"/"
					+ langType
					+ "/global/975\"  href=\"/"
					+ langType
					+ "/global/1763\"  href=\"/"
					+ langType
					+ "/global/3639\"  href=\"/"
					+ langType
					+ "/global/4159\"  href=\"/"
					+ langType
					+ "/global/2415\"  href=\"/"
					+ langType
					+ "/global/2419\"  href=\"/"
					+ langType
					+ "/global/2421\"  href=\"/"
					+ langType
					+ "/global/8661\"  href=\"/"
					+ langType
					+ "/global/1021\"  href=\"/"
					+ langType
					+ "/global/1637\"  href=\"/"
					+ langType
					+ "/global/993\"  href=\"/"
					+ langType
					+ "/global/997\"  href=\"/"
					+ langType
					+ "/global/3567\"  href=\"/"
					+ langType
					+ "/global/3671\"  href=\"/"
					+ langType
					+ "/global/3675\"  href=\"/"
					+ langType
					+ "/global/921\"  href=\"/"
					+ langType
					+ "/global/345\"  href=\"/"
					+ langType
					+ "/global/2351\"  href=\"/"
					+ langType
					+ "/global/6453\"  href=\"/"
					+ langType
					+ "/global/991\"  href=\"/"
					+ langType
					+ "/global/2413\"  href=\"/"
					+ langType
					+ "/global/2079\"  href=\"/"
					+ langType
					+ "/global/8979\"  href=\"/"
					+ langType
					+ "/global/1259\"  href=\"/"
					+ langType
					+ "/global/1281\"  href=\"/"
					+ langType
					+ "/global/1261\"  href=\"/"
					+ langType
					+ "/global/1263\"  href=\"/"
					+ langType
					+ "/global/1265\"  href=\"/"
					+ langType
					+ "/global/1267\"  href=\"/"
					+ langType
					+ "/global/1269\"  href=\"/"
					+ langType
					+ "/global/1275\"  href=\"/"
					+ langType
					+ "/global/1282\"  href=\"/"
					+ langType
					+ "/global/1283\"  href=\"/"
					+ langType
					+ "/global/1057\"  href=\"/"
					+ langType
					+ "/global/9186\"  href=\"/"
					+ langType
					+ "/global/2325\"  href=\"/"
					+ langType
					+ "/global/2327\"  href=\"/"
					+ langType
					+ "/global/2329\"  href=\"/"
					+ langType
					+ "/global/2333\"  href=\"/"
					+ langType
					+ "/global/2335\"  href=\"/"
					+ langType
					+ "/global/2337\"  href=\"/"
					+ langType
					+ "/global/8598\"  href=\"/"
					+ langType
					+ "/global/9378\"  href=\"/"
					+ langType
					+ "/global/8655\"  href=\"/"
					+ langType
					+ "/global/1436\"  href=\"/"
					+ langType
					+ "/global/647\"  href=\"/"
					+ langType
					+ "/global/2427\"  href=\"/"
					+ langType
					+ "/global/2429\"  href=\"/"
					+ langType
					+ "/global/2431\"  href=\"/"
					+ langType
					+ "/global/1195\"  href=\"/"
					+ langType
					+ "/global/1235\"  href=\"/"
					+ langType
					+ "/global/1203\"  href=\"/"
					+ langType
					+ "/global/1205\"  href=\"/"
					+ langType
					+ "/global/1209\"  href=\"/"
					+ langType
					+ "/global/1211\"  href=\"/"
					+ langType
					+ "/global/1215\"  href=\"/"
					+ langType
					+ "/global/1237\"  href=\"/"
					+ langType
					+ "/global/1207\"  href=\"/"
					+ langType
					+ "/global/2877\"  href=\"/"
					+ langType
					+ "/global/59\"  href=\"/"
					+ langType
					+ "/global/1367\"  href=\"/"
					+ langType
					+ "/global/1369\"  href=\"/"
					+ langType
					+ "/global/1371\"  href=\"/"
					+ langType
					+ "/global/1097\"  href=\"/"
					+ langType
					+ "/global/1099\"  href=\"/"
					+ langType
					+ "/global/1101\"  href=\"/"
					+ langType
					+ "/global/1109\"  href=\"/"
					+ langType
					+ "/global/8778\"  href=\"/"
					+ langType
					+ "/global/8781\"  href=\"/"
					+ langType
					+ "/global/8670\"  href=\"/"
					+ langType
					+ "/global/1167\"  href=\"/"
					+ langType
					+ "/global/8772\"  href=\"/"
					+ langType
					+ "/global/1135\"  href=\"/"
					+ langType
					+ "/global/1145\"  href=\"/"
					+ langType
					+ "/global/1147\"  href=\"/"
					+ langType
					+ "/global/1151\"  href=\"/"
					+ langType
					+ "/global/9348\"  href=\"/"
					+ langType
					+ "/global/1797\"  href=\"/"
					+ langType
					+ "/global/1533\"  href=\"/"
					+ langType
					+ "/global/8676\"  href=\"/"
					+ langType
					+ "/global/3035\"  href=\"/"
					+ langType
					+ "/global/8784\"  href=\"/"
					+ langType
					+ "/global/259\"  href=\"/"
					+ langType
					+ "/global/1113\"  href=\"/"
					+ langType
					+ "/global/1115\"  href=\"/"
					+ langType
					+ "/global/9387\"  href=\"/"
					+ langType
					+ "/global/3673\"  href=\"/"
					+ langType
					+ "/global/8658\"  href=\"/"
					+ langType
					+ "/global/937\"  href=\"/"
					+ langType
					+ "/global/1414\"  href=\"/"
					+ langType
					+ "/global/1819\"  href=\"/"
					+ langType
					+ "/global/1821\"  href=\"/"
					+ langType
					+ "/global/941\"  href=\"/"
					+ langType
					+ "/global/3899\"  href=\"/"
					+ langType
					+ "/global/4037\"  href=\"/"
					+ langType
					+ "/global/4041\"  href=\"/"
					+ langType
					+ "/global/4221\"  href=\"/"
					+ langType
					+ "/global/11\"  href=\"/"
					+ langType
					+ "/global/13\"  href=\"/"
					+ langType
					+ "/global/4193\"  href=\"/"
					+ langType
					+ "/global/3563\"  href=\"/"
					+ langType
					+ "/global/3889\"  href=\"/"
					+ langType
					+ "/global/3537\"  href=\"/"
					+ langType
					+ "/global/2295\"  href=\"/"
					+ langType
					+ "/global/2297\"  href=\"/"
					+ langType
					+ "/global/2299\"  href=\"/"
					+ langType
					+ "/global/3587\"  href=\"/"
					+ langType
					+ "/global/4115\"  href=\"/"
					+ langType
					+ "/global/3947\"  href=\"/"
					+ langType
					+ "/global/3929\"  href=\"/"
					+ langType
					+ "/global/8640\"  href=\"/"
					+ langType
					+ "/global/4229\"  href=\"/"
					+ langType
					+ "/global/699\"  href=\"/"
					+ langType
					+ "/global/8715\"  href=\"/"
					+ langType
					+ "/global/8724\"  href=\"/"
					+ langType
					+ "/global/8727\"  href=\"/"
					+ langType
					+ "/global/8492\"  href=\"/"
					+ langType
					+ "/global/9042\"  href=\"/"
					+ langType
					+ "/global/4137\"  href=\"/"
					+ langType
					+ "/global/4107\"  href=\"/"
					+ langType
					+ "/global/4149\"  href=\"/"
					+ langType
					+ "/global/4151\"  href=\"/"
					+ langType
					+ "/global/4157\"  href=\"/"
					+ langType
					+ "/global/9447\"  href=\"/"
					+ langType
					+ "/global/9450\"  href=\"/"
					+ langType
					+ "/global/4155\"  href=\"/"
					+ langType
					+ "/global/3569\"  href=\"/"
					+ langType
					+ "/global/3573\"  href=\"/"
					+ langType
					+ "/global/3961\"  href=\"/"
					+ langType
					+ "/global/9090\"  href=\"/"
					+ langType
					+ "/global/3583\"  href=\"/"
					+ langType
					+ "/global/3455\"  href=\"/"
					+ langType
					+ "/global/3601\"  href=\"/"
					+ langType
					+ "/global/9237\"  href=\"/"
					+ langType
					+ "/global/3711\"  href=\"/"
					+ langType
					+ "/global/3827\"  href=\"/"
					+ langType
					+ "/global/3997\"  href=\"/"
					+ langType
					+ "/global/4029\"  href=\"/"
					+ langType
					+ "/global/1185\"  href=\"/"
					+ langType
					+ "/global/8649\"  href=\"/"
					+ langType
					+ "/global/3017\"  href=\"/"
					+ langType
					+ "/global/303\"  href=\"/"
					+ langType
					+ "/global/3525\"  href=\"/"
					+ langType
					+ "/global/3903\"  href=\"/"
					+ langType
					+ "/global/8787\"  href=\"/"
					+ langType
					+ "/global/9120\"  href=\"/"
					+ langType
					+ "/global/3667\"  href=\"/"
					+ langType
					+ "/global/3463\"  href=\"/"
					+ langType
					+ "/global/3855\"  href=\"/"
					+ langType
					+ "/global/4141\"  href=\"/"
					+ langType
					+ "/global/1585\"  href=\"/"
					+ langType
					+ "/global/3725\"  href=\"/"
					+ langType
					+ "/global/3093\"  href=\"/"
					+ langType
					+ "/global/3095\"  href=\"/"
					+ langType
					+ "/global/2683\"  href=\"/"
					+ langType
					+ "/global/1829\"  href=\"/"
					+ langType
					+ "/global/1807\"  href=\"/"
					+ langType
					+ "/global/1811\"  href=\"/"
					+ langType
					+ "/global/2885\"  href=\"/"
					+ langType
					+ "/global/9150\"  href=\"/"
					+ langType
					+ "/global/9153\"  href=\"/"
					+ langType
					+ "/global/9156\"  href=\"/"
					+ langType
					+ "/global/631\"  href=\"/"
					+ langType
					+ "/global/1589\"  href=\"/"
					+ langType
					+ "/global/1629\"  href=\"/"
					+ langType
					+ "/global/2179\"  href=\"/"
					+ langType
					+ "/global/4235\"  href=\"/"
					+ langType
					+ "/global/9060\"  href=\"/"
					+ langType
					+ "/global/9336\"  href=\"/"
					+ langType
					+ "/global/2685\"  href=\"/"
					+ langType
					+ "/global/2687\"  href=\"/"
					+ langType
					+ "/global/2689\"  href=\"/"
					+ langType
					+ "/global/2691\"  href=\"/"
					+ langType
					+ "/global/3937\"  href=\"/"
					+ langType
					+ "/global/3597\"  href=\"/"
					+ langType
					+ "/global/3447\"  href=\"/"
					+ langType
					+ "/global/8775\"  href=\"/"
					+ langType
					+ "/global/1365\"  href=\"/"
					+ langType
					+ "/global/3383\"  href=\"/"
					+ langType
					+ "/global/1229\"  href=\"/"
					+ langType
					+ "/global/4173\"  href=\"/"
					+ langType
					+ "/global/3991\"  href=\"/"
					+ langType
					+ "/global/2059\"  href=\"/"
					+ langType
					+ "/global/4051\"  href=\"/"
					+ langType
					+ "/global/4057\"  href=\"/"
					+ langType
					+ "/global/773\"  href=\"/"
					+ langType
					+ "/global/3641\"  href=\"/"
					+ langType
					+ "/global/3717\"  href=\"/"
					+ langType
					+ "/global/4207\"  href=\"/"
					+ langType
					+ "/global/3459\"  href=\"/"
					+ langType
					+ "/global/3757\"  href=\"/"
					+ langType
					+ "/global/3531\"  href=\"/"
					+ langType
					+ "/global/3545\"  href=\"/"
					+ langType
					+ "/global/2745\"  href=\"/"
					+ langType
					+ "/global/1745\"  href=\"/"
					+ langType
					+ "/global/1488\"  href=\"/"
					+ langType
					+ "/global/1489\"  href=\"/"
					+ langType
					+ "/global/787\"  href=\"/"
					+ langType
					+ "/global/1751\"  href=\"/"
					+ langType
					+ "/global/1439\"  href=\"/"
					+ langType
					+ "/global/8988\"  href=\"/"
					+ langType
					+ "/global/2071\"  href=\"/"
					+ langType
					+ "/global/1613\"  href=\"/"
					+ langType
					+ "/global/2083\"  href=\"/"
					+ langType
					+ "/global/3487\"  href=\"/"
					+ langType
					+ "/global/3519\"  href=\"/"
					+ langType
					+ "/global/3521\"  href=\"/"
					+ langType
					+ "/global/3225\"  href=\"/"
					+ langType
					+ "/global/3681\"  href=\"/"
					+ langType
					+ "/global/2383\"  href=\"/"
					+ langType
					+ "/global/255\"  href=\"/"
					+ langType
					+ "/global/1507\"  href=\"/"
					+ langType
					+ "/global/1508\"  href=\"/"
					+ langType
					+ "/global/8874\"  href=\"/"
					+ langType
					+ "/global/8871\"  href=\"/"
					+ langType
					+ "/global/8877\"  href=\"/"
					+ langType
					+ "/global/1625\"  href=\"/"
					+ langType
					+ "/global/8832\"  href=\"/"
					+ langType
					+ "/global/3829\"  href=\"/"
					+ langType
					+ "/global/9393\"  href=\"/"
					+ langType
					+ "/global/3759\"  href=\"/"
					+ langType
					+ "/global/9192\"  href=\"/"
					+ langType
					+ "/global/3761\"  href=\"/"
					+ langType
					+ "/global/4205\"  href=\"/"
					+ langType
					+ "/global/3815\"  href=\"/"
					+ langType
					+ "/global/4131\"  href=\"/"
					+ langType
					+ "/global/4209\"  href=\"/"
					+ langType
					+ "/global/3561\"  href=\"/"
					+ langType
					+ "/global/3787\"  href=\"/"
					+ langType
					+ "/global/3747\"  href=\"/"
					+ langType
					+ "/global/3901\"  href=\"/"
					+ langType
					+ "/global/3703\"  href=\"/"
					+ langType
					+ "/global/3707\"  href=\"/"
					+ langType
					+ "/global/3839\"  href=\"/"
					+ langType
					+ "/global/3825\"  href=\"/"
					+ langType
					+ "/global/3843\"  href=\"/"
					+ langType
					+ "/global/3801\"  href=\"/"
					+ langType
					+ "/global/3775\"  href=\"/"
					+ langType
					+ "/global/3543\"  href=\"/"
					+ langType
					+ "/global/3677\"  href=\"/"
					+ langType
					+ "/global/3311\"  href=\"/"
					+ langType
					+ "/global/1885\"  href=\"/"
					+ langType
					+ "/global/3679\"  href=\"/"
					+ langType
					+ "/global/3603\"  href=\"/"
					+ langType
					+ "/global/3535\"  href=\"/"
					+ langType
					+ "/global/823\"  href=\"/"
					+ langType
					+ "/global/827\"  href=\"/"
					+ langType
					+ "/global/9432\"  href=\"/"
					+ langType
					+ "/global/3819\"  href=\"/"
					+ langType
					+ "/global/3847\"  href=\"/"
					+ langType
					+ "/global/4039\"  href=\"/"
					+ langType
					+ "/global/4053\"  href=\"/"
					+ langType
					+ "/global/9363\"  href=\"/"
					+ langType
					+ "/global/9366\"                       >众泰汽车 新能源汽车生产基地 Zotye Automobile New Energy Automobile Production Base</a></td><td>整车</td><td>新能源汽车整车及核心零部件, 新款SUV</td></tr></tbody></table>";
			Pattern ch = Pattern.compile("href=\"(/" + langType + "/global/\\d+)\"");
			Matcher m = ch.matcher(cnt);
			while (m.find()) {
				String code = m.group(1);
				if (!code.equals("")) {
					cfg = new urlCfg();
					cfg.url = "https://www.marklines.com" + code;
					add(cfg);
				}
			}
		}

		// 配件
		//
		// https://www.marklines.com/"+langTyep+"/supplier_db/partDetail?_is=true&containsSub=true&partsName=&isPartial=false&oth.place%5B%5D=a%2C81&oth.del=&oth.isPartial=true
		// https://www.marklines.com/"+langTyep+"/supplier_db/partDetail?page=0&size=10&tb=top&_is=true&containsSub=true&isPartial=false&oth.place[]=a,80&oth.isPartial=true
		// https://www.marklines.com/"+langTyep+"/supplier_db/partDetail?page=1&size=10&tb=top&_is=true&containsSub=true&isPartial=false&oth.place[]=a,80&oth.isPartial=true
		// https://www.marklines.com/"+langTyep+"/supplier_db/partDetail?_is=true&containsSub=true&partsName=&isPartial=false&oth.place%5B%5D=a%2C82&oth.del=&oth.isPartial=true
		if (flags[4]) {
			String cnt = "<table class=\"table table-bordered\"><thead><tr><th>区域</th><th>国家</th><th>地区</th><th>省市</th></tr></thead><tbody>  <tr><td class=\"categoryColor\" rowspan=\"82\"> <span id=\"_select_parts_r__59\">东亚</span></td><td class=\"topColor\" rowspan=\"33\"> <div class=\"checkbox\" id=\"_select_parts_n__2\"> <label for=\"_select_checkbox_parts_n_2\"><input id=\"_select_checkbox_parts_n_2\" data-locale-parts--n=\"2\" data-child-locale-parts--n=\"59\" type=\"checkbox\" value=\"n,2\" name=\"oth.place[]\" data-location=\"parts\">中国 </label></div></td><td class=\"centerColor\" rowspan=\"3\"> <div class=\"checkbox\" id=\"_select_parts_s__8\"> <label for=\"_select_checkbox_parts_s_8\"><input id=\"_select_checkbox_parts_s_8\" data-locale-parts--s=\"8\" data-child-locale-parts--s=\"2\" type=\"checkbox\" value=\"s,8\" name=\"oth.place[]\" data-location=\"parts\">东北地区 </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__71\"> <label for=\"_select_checkbox_parts_a_71\"><input id=\"_select_checkbox_parts_a_71\" data-locale-parts--a=\"71\" data-child-locale-parts--a=\"8\" type=\"checkbox\" value=\"a,71\" name=\"oth.place[]\" data-location=\"parts\">黑龙江省 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__72\"> <label for=\"_select_checkbox_parts_a_72\"><input id=\"_select_checkbox_parts_a_72\" data-locale-parts--a=\"72\" data-child-locale-parts--a=\"8\" type=\"checkbox\" value=\"a,72\" name=\"oth.place[]\" data-location=\"parts\">吉林省 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__73\"> <label for=\"_select_checkbox_parts_a_73\"><input id=\"_select_checkbox_parts_a_73\" data-locale-parts--a=\"73\" data-child-locale-parts--a=\"8\" type=\"checkbox\" value=\"a,73\" name=\"oth.place[]\" data-location=\"parts\">辽宁省 </label></div></td> </tr> <tr><td class=\"centerColor\" rowspan=\"6\"> <div class=\"checkbox\" id=\"_select_parts_s__9\"> <label for=\"_select_checkbox_parts_s_9\"><input id=\"_select_checkbox_parts_s_9\" data-locale-parts--s=\"9\" data-child-locale-parts--s=\"2\" type=\"checkbox\" value=\"s,9\" name=\"oth.place[]\" data-location=\"parts\">华北地区 </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__74\"> <label for=\"_select_checkbox_parts_a_74\"><input id=\"_select_checkbox_parts_a_74\" data-locale-parts--a=\"74\" data-child-locale-parts--a=\"9\" type=\"checkbox\" value=\"a,74\" name=\"oth.place[]\" data-location=\"parts\">北京市 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__75\"> <label for=\"_select_checkbox_parts_a_75\"><input id=\"_select_checkbox_parts_a_75\" data-locale-parts--a=\"75\" data-child-locale-parts--a=\"9\" type=\"checkbox\" value=\"a,75\" name=\"oth.place[]\" data-location=\"parts\">天津市 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__76\"> <label for=\"_select_checkbox_parts_a_76\"><input id=\"_select_checkbox_parts_a_76\" data-locale-parts--a=\"76\" data-child-locale-parts--a=\"9\" type=\"checkbox\" value=\"a,76\" name=\"oth.place[]\" data-location=\"parts\">河北省 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__77\"> <label for=\"_select_checkbox_parts_a_77\"><input id=\"_select_checkbox_parts_a_77\" data-locale-parts--a=\"77\" data-child-locale-parts--a=\"9\" type=\"checkbox\" value=\"a,77\" name=\"oth.place[]\" data-location=\"parts\">河南省 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__78\"> <label for=\"_select_checkbox_parts_a_78\"><input id=\"_select_checkbox_parts_a_78\" data-locale-parts--a=\"78\" data-child-locale-parts--a=\"9\" type=\"checkbox\" value=\"a,78\" name=\"oth.place[]\" data-location=\"parts\">山西省 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__79\"> <label for=\"_select_checkbox_parts_a_79\"><input id=\"_select_checkbox_parts_a_79\" data-locale-parts--a=\"79\" data-child-locale-parts--a=\"9\" type=\"checkbox\" value=\"a,79\" name=\"oth.place[]\" data-location=\"parts\">内蒙古自治区 </label></div></td> </tr> <tr><td class=\"centerColor\" rowspan=\"4\"> <div class=\"checkbox\" id=\"_select_parts_s__10\"> <label for=\"_select_checkbox_parts_s_10\"><input id=\"_select_checkbox_parts_s_10\" data-locale-parts--s=\"10\" data-child-locale-parts--s=\"2\" type=\"checkbox\" value=\"s,10\" name=\"oth.place[]\" data-location=\"parts\">华东地区 </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__80\"> <label for=\"_select_checkbox_parts_a_80\"><input id=\"_select_checkbox_parts_a_80\" data-locale-parts--a=\"80\" data-child-locale-parts--a=\"10\" type=\"checkbox\" value=\"a,80\" name=\"oth.place[]\" data-location=\"parts\">山东省 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__81\"> <label for=\"_select_checkbox_parts_a_81\"><input id=\"_select_checkbox_parts_a_81\" data-locale-parts--a=\"81\" data-child-locale-parts--a=\"10\" type=\"checkbox\" value=\"a,81\" name=\"oth.place[]\" data-location=\"parts\">江苏省 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__82\"> <label for=\"_select_checkbox_parts_a_82\"><input id=\"_select_checkbox_parts_a_82\" data-locale-parts--a=\"82\" data-child-locale-parts--a=\"10\" type=\"checkbox\" value=\"a,82\" name=\"oth.place[]\" data-location=\"parts\">上海市 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__83\"> <label for=\"_select_checkbox_parts_a_83\"><input id=\"_select_checkbox_parts_a_83\" data-locale-parts--a=\"83\" data-child-locale-parts--a=\"10\" type=\"checkbox\" value=\"a,83\" name=\"oth.place[]\" data-location=\"parts\">浙江省 </label></div></td> </tr> <tr><td class=\"centerColor\" rowspan=\"3\"> <div class=\"checkbox\" id=\"_select_parts_s__11\"> <label for=\"_select_checkbox_parts_s_11\"><input id=\"_select_checkbox_parts_s_11\" data-locale-parts--s=\"11\" data-child-locale-parts--s=\"2\" type=\"checkbox\" value=\"s,11\" name=\"oth.place[]\" data-location=\"parts\">华中地区 </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__84\"> <label for=\"_select_checkbox_parts_a_84\"><input id=\"_select_checkbox_parts_a_84\" data-locale-parts--a=\"84\" data-child-locale-parts--a=\"11\" type=\"checkbox\" value=\"a,84\" name=\"oth.place[]\" data-location=\"parts\">湖北省 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__85\"> <label for=\"_select_checkbox_parts_a_85\"><input id=\"_select_checkbox_parts_a_85\" data-locale-parts--a=\"85\" data-child-locale-parts--a=\"11\" type=\"checkbox\" value=\"a,85\" name=\"oth.place[]\" data-location=\"parts\">安徽省 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__86\"> <label for=\"_select_checkbox_parts_a_86\"><input id=\"_select_checkbox_parts_a_86\" data-locale-parts--a=\"86\" data-child-locale-parts--a=\"11\" type=\"checkbox\" value=\"a,86\" name=\"oth.place[]\" data-location=\"parts\">江西省 </label></div></td> </tr> <tr><td class=\"centerColor\" rowspan=\"6\"> <div class=\"checkbox\" id=\"_select_parts_s__12\"> <label for=\"_select_checkbox_parts_s_12\"><input id=\"_select_checkbox_parts_s_12\" data-locale-parts--s=\"12\" data-child-locale-parts--s=\"2\" type=\"checkbox\" value=\"s,12\" name=\"oth.place[]\" data-location=\"parts\">华南地区 </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__87\"> <label for=\"_select_checkbox_parts_a_87\"><input id=\"_select_checkbox_parts_a_87\" data-locale-parts--a=\"87\" data-child-locale-parts--a=\"12\" type=\"checkbox\" value=\"a,87\" name=\"oth.place[]\" data-location=\"parts\">湖南省 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__88\"> <label for=\"_select_checkbox_parts_a_88\"><input id=\"_select_checkbox_parts_a_88\" data-locale-parts--a=\"88\" data-child-locale-parts--a=\"12\" type=\"checkbox\" value=\"a,88\" name=\"oth.place[]\" data-location=\"parts\">福建省 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__89\"> <label for=\"_select_checkbox_parts_a_89\"><input id=\"_select_checkbox_parts_a_89\" data-locale-parts--a=\"89\" data-child-locale-parts--a=\"12\" type=\"checkbox\" value=\"a,89\" name=\"oth.place[]\" data-location=\"parts\">广东省 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__90\"> <label for=\"_select_checkbox_parts_a_90\"><input id=\"_select_checkbox_parts_a_90\" data-locale-parts--a=\"90\" data-child-locale-parts--a=\"12\" type=\"checkbox\" value=\"a,90\" name=\"oth.place[]\" data-location=\"parts\">海南省 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__91\"> <label for=\"_select_checkbox_parts_a_91\"><input id=\"_select_checkbox_parts_a_91\" data-locale-parts--a=\"91\" data-child-locale-parts--a=\"12\" type=\"checkbox\" value=\"a,91\" name=\"oth.place[]\" data-location=\"parts\">香港 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__92\"> <label for=\"_select_checkbox_parts_a_92\"><input id=\"_select_checkbox_parts_a_92\" data-locale-parts--a=\"92\" data-child-locale-parts--a=\"12\" type=\"checkbox\" value=\"a,92\" name=\"oth.place[]\" data-location=\"parts\">澳门 </label></div></td> </tr> <tr><td class=\"centerColor\" rowspan=\"6\"> <div class=\"checkbox\" id=\"_select_parts_s__13\"> <label for=\"_select_checkbox_parts_s_13\"><input id=\"_select_checkbox_parts_s_13\" data-locale-parts--s=\"13\" data-child-locale-parts--s=\"2\" type=\"checkbox\" value=\"s,13\" name=\"oth.place[]\" data-location=\"parts\">西南地区 </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__93\"> <label for=\"_select_checkbox_parts_a_93\"><input id=\"_select_checkbox_parts_a_93\" data-locale-parts--a=\"93\" data-child-locale-parts--a=\"13\" type=\"checkbox\" value=\"a,93\" name=\"oth.place[]\" data-location=\"parts\">西藏自治区 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__94\"> <label for=\"_select_checkbox_parts_a_94\"><input id=\"_select_checkbox_parts_a_94\" data-locale-parts--a=\"94\" data-child-locale-parts--a=\"13\" type=\"checkbox\" value=\"a,94\" name=\"oth.place[]\" data-location=\"parts\">四川省 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__146\"> <label for=\"_select_checkbox_parts_a_146\"><input id=\"_select_checkbox_parts_a_146\" data-locale-parts--a=\"146\" data-child-locale-parts--a=\"13\" type=\"checkbox\" value=\"a,146\" name=\"oth.place[]\" data-location=\"parts\">重庆市 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__147\"> <label for=\"_select_checkbox_parts_a_147\"><input id=\"_select_checkbox_parts_a_147\" data-locale-parts--a=\"147\" data-child-locale-parts--a=\"13\" type=\"checkbox\" value=\"a,147\" name=\"oth.place[]\" data-location=\"parts\">云南省 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__148\"> <label for=\"_select_checkbox_parts_a_148\"><input id=\"_select_checkbox_parts_a_148\" data-locale-parts--a=\"148\" data-child-locale-parts--a=\"13\" type=\"checkbox\" value=\"a,148\" name=\"oth.place[]\" data-location=\"parts\">贵州省 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__149\"> <label for=\"_select_checkbox_parts_a_149\"><input id=\"_select_checkbox_parts_a_149\" data-locale-parts--a=\"149\" data-child-locale-parts--a=\"13\" type=\"checkbox\" value=\"a,149\" name=\"oth.place[]\" data-location=\"parts\">广西壮族自治区 </label></div></td> </tr> <tr><td class=\"centerColor\" rowspan=\"5\"> <div class=\"checkbox\" id=\"_select_parts_s__14\"> <label for=\"_select_checkbox_parts_s_14\"><input id=\"_select_checkbox_parts_s_14\" data-locale-parts--s=\"14\" data-child-locale-parts--s=\"2\" type=\"checkbox\" value=\"s,14\" name=\"oth.place[]\" data-location=\"parts\">西北地区 </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__150\"> <label for=\"_select_checkbox_parts_a_150\"><input id=\"_select_checkbox_parts_a_150\" data-locale-parts--a=\"150\" data-child-locale-parts--a=\"14\" type=\"checkbox\" value=\"a,150\" name=\"oth.place[]\" data-location=\"parts\">陕西省 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__151\"> <label for=\"_select_checkbox_parts_a_151\"><input id=\"_select_checkbox_parts_a_151\" data-locale-parts--a=\"151\" data-child-locale-parts--a=\"14\" type=\"checkbox\" value=\"a,151\" name=\"oth.place[]\" data-location=\"parts\">宁夏回族自治区 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__152\"> <label for=\"_select_checkbox_parts_a_152\"><input id=\"_select_checkbox_parts_a_152\" data-locale-parts--a=\"152\" data-child-locale-parts--a=\"14\" type=\"checkbox\" value=\"a,152\" name=\"oth.place[]\" data-location=\"parts\">甘肃省 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__153\"> <label for=\"_select_checkbox_parts_a_153\"><input id=\"_select_checkbox_parts_a_153\" data-locale-parts--a=\"153\" data-child-locale-parts--a=\"14\" type=\"checkbox\" value=\"a,153\" name=\"oth.place[]\" data-location=\"parts\">青海省 </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__154\"> <label for=\"_select_checkbox_parts_a_154\"><input id=\"_select_checkbox_parts_a_154\" data-locale-parts--a=\"154\" data-child-locale-parts--a=\"14\" type=\"checkbox\" value=\"a,154\" name=\"oth.place[]\" data-location=\"parts\">新疆维吾尔自治区 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__4\"> <label for=\"_select_checkbox_parts_n_4\"><input id=\"_select_checkbox_parts_n_4\" data-locale-parts--n=\"4\" data-child-locale-parts--n=\"59\" type=\"checkbox\" value=\"n,4\" name=\"oth.place[]\" data-location=\"parts\">中国台湾 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"47\"> <div class=\"checkbox\" id=\"_select_parts_n__1\"> <label for=\"_select_checkbox_parts_n_1\"><input id=\"_select_checkbox_parts_n_1\" data-locale-parts--n=\"1\" data-child-locale-parts--n=\"59\" type=\"checkbox\" value=\"n,1\" name=\"oth.place[]\" data-location=\"parts\">日本 </label></div></td><td class=\"centerColor\" rowspan=\"7\"> <div class=\"checkbox\" id=\"_select_parts_s__1\"> <label for=\"_select_checkbox_parts_s_1\"><input id=\"_select_checkbox_parts_s_1\" data-locale-parts--s=\"1\" data-child-locale-parts--s=\"1\" type=\"checkbox\" value=\"s,1\" name=\"oth.place[]\" data-location=\"parts\">Hokkaido/Tohoku </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__1\"> <label for=\"_select_checkbox_parts_a_1\"><input id=\"_select_checkbox_parts_a_1\" data-locale-parts--a=\"1\" data-child-locale-parts--a=\"1\" type=\"checkbox\" value=\"a,1\" name=\"oth.place[]\" data-location=\"parts\">Hokkaido </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__2\"> <label for=\"_select_checkbox_parts_a_2\"><input id=\"_select_checkbox_parts_a_2\" data-locale-parts--a=\"2\" data-child-locale-parts--a=\"1\" type=\"checkbox\" value=\"a,2\" name=\"oth.place[]\" data-location=\"parts\">Aomori </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__3\"> <label for=\"_select_checkbox_parts_a_3\"><input id=\"_select_checkbox_parts_a_3\" data-locale-parts--a=\"3\" data-child-locale-parts--a=\"1\" type=\"checkbox\" value=\"a,3\" name=\"oth.place[]\" data-location=\"parts\">Iwate </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__4\"> <label for=\"_select_checkbox_parts_a_4\"><input id=\"_select_checkbox_parts_a_4\" data-locale-parts--a=\"4\" data-child-locale-parts--a=\"1\" type=\"checkbox\" value=\"a,4\" name=\"oth.place[]\" data-location=\"parts\">Miyagi </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__5\"> <label for=\"_select_checkbox_parts_a_5\"><input id=\"_select_checkbox_parts_a_5\" data-locale-parts--a=\"5\" data-child-locale-parts--a=\"1\" type=\"checkbox\" value=\"a,5\" name=\"oth.place[]\" data-location=\"parts\">Akita </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__6\"> <label for=\"_select_checkbox_parts_a_6\"><input id=\"_select_checkbox_parts_a_6\" data-locale-parts--a=\"6\" data-child-locale-parts--a=\"1\" type=\"checkbox\" value=\"a,6\" name=\"oth.place[]\" data-location=\"parts\">Yamagata </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__7\"> <label for=\"_select_checkbox_parts_a_7\"><input id=\"_select_checkbox_parts_a_7\" data-locale-parts--a=\"7\" data-child-locale-parts--a=\"1\" type=\"checkbox\" value=\"a,7\" name=\"oth.place[]\" data-location=\"parts\">Fukushima </label></div></td> </tr> <tr><td class=\"centerColor\" rowspan=\"7\"> <div class=\"checkbox\" id=\"_select_parts_s__2\"> <label for=\"_select_checkbox_parts_s_2\"><input id=\"_select_checkbox_parts_s_2\" data-locale-parts--s=\"2\" data-child-locale-parts--s=\"1\" type=\"checkbox\" value=\"s,2\" name=\"oth.place[]\" data-location=\"parts\">Kanto </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__8\"> <label for=\"_select_checkbox_parts_a_8\"><input id=\"_select_checkbox_parts_a_8\" data-locale-parts--a=\"8\" data-child-locale-parts--a=\"2\" type=\"checkbox\" value=\"a,8\" name=\"oth.place[]\" data-location=\"parts\">Ibaraki </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__9\"> <label for=\"_select_checkbox_parts_a_9\"><input id=\"_select_checkbox_parts_a_9\" data-locale-parts--a=\"9\" data-child-locale-parts--a=\"2\" type=\"checkbox\" value=\"a,9\" name=\"oth.place[]\" data-location=\"parts\">Tochigi </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__10\"> <label for=\"_select_checkbox_parts_a_10\"><input id=\"_select_checkbox_parts_a_10\" data-locale-parts--a=\"10\" data-child-locale-parts--a=\"2\" type=\"checkbox\" value=\"a,10\" name=\"oth.place[]\" data-location=\"parts\">Gunma </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__11\"> <label for=\"_select_checkbox_parts_a_11\"><input id=\"_select_checkbox_parts_a_11\" data-locale-parts--a=\"11\" data-child-locale-parts--a=\"2\" type=\"checkbox\" value=\"a,11\" name=\"oth.place[]\" data-location=\"parts\">Saitama </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__12\"> <label for=\"_select_checkbox_parts_a_12\"><input id=\"_select_checkbox_parts_a_12\" data-locale-parts--a=\"12\" data-child-locale-parts--a=\"2\" type=\"checkbox\" value=\"a,12\" name=\"oth.place[]\" data-location=\"parts\">Chiba </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__13\"> <label for=\"_select_checkbox_parts_a_13\"><input id=\"_select_checkbox_parts_a_13\" data-locale-parts--a=\"13\" data-child-locale-parts--a=\"2\" type=\"checkbox\" value=\"a,13\" name=\"oth.place[]\" data-location=\"parts\">Tokyo </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__14\"> <label for=\"_select_checkbox_parts_a_14\"><input id=\"_select_checkbox_parts_a_14\" data-locale-parts--a=\"14\" data-child-locale-parts--a=\"2\" type=\"checkbox\" value=\"a,14\" name=\"oth.place[]\" data-location=\"parts\">Kanagawa </label></div></td> </tr> <tr><td class=\"centerColor\" rowspan=\"6\"> <div class=\"checkbox\" id=\"_select_parts_s__3\"> <label for=\"_select_checkbox_parts_s_3\"><input id=\"_select_checkbox_parts_s_3\" data-locale-parts--s=\"3\" data-child-locale-parts--s=\"1\" type=\"checkbox\" value=\"s,3\" name=\"oth.place[]\" data-location=\"parts\">Koushinetsu/hokuriku </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__16\"> <label for=\"_select_checkbox_parts_a_16\"><input id=\"_select_checkbox_parts_a_16\" data-locale-parts--a=\"16\" data-child-locale-parts--a=\"3\" type=\"checkbox\" value=\"a,16\" name=\"oth.place[]\" data-location=\"parts\">Niigata </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__18\"> <label for=\"_select_checkbox_parts_a_18\"><input id=\"_select_checkbox_parts_a_18\" data-locale-parts--a=\"18\" data-child-locale-parts--a=\"3\" type=\"checkbox\" value=\"a,18\" name=\"oth.place[]\" data-location=\"parts\">Toyama </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__19\"> <label for=\"_select_checkbox_parts_a_19\"><input id=\"_select_checkbox_parts_a_19\" data-locale-parts--a=\"19\" data-child-locale-parts--a=\"3\" type=\"checkbox\" value=\"a,19\" name=\"oth.place[]\" data-location=\"parts\">Ishikawa </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__20\"> <label for=\"_select_checkbox_parts_a_20\"><input id=\"_select_checkbox_parts_a_20\" data-locale-parts--a=\"20\" data-child-locale-parts--a=\"3\" type=\"checkbox\" value=\"a,20\" name=\"oth.place[]\" data-location=\"parts\">Fukui </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__15\"> <label for=\"_select_checkbox_parts_a_15\"><input id=\"_select_checkbox_parts_a_15\" data-locale-parts--a=\"15\" data-child-locale-parts--a=\"3\" type=\"checkbox\" value=\"a,15\" name=\"oth.place[]\" data-location=\"parts\">Yamanashi </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__17\"> <label for=\"_select_checkbox_parts_a_17\"><input id=\"_select_checkbox_parts_a_17\" data-locale-parts--a=\"17\" data-child-locale-parts--a=\"3\" type=\"checkbox\" value=\"a,17\" name=\"oth.place[]\" data-location=\"parts\">Nagano </label></div></td> </tr> <tr><td class=\"centerColor\" rowspan=\"4\"> <div class=\"checkbox\" id=\"_select_parts_s__4\"> <label for=\"_select_checkbox_parts_s_4\"><input id=\"_select_checkbox_parts_s_4\" data-locale-parts--s=\"4\" data-child-locale-parts--s=\"1\" type=\"checkbox\" value=\"s,4\" name=\"oth.place[]\" data-location=\"parts\">Tokai </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__23\"> <label for=\"_select_checkbox_parts_a_23\"><input id=\"_select_checkbox_parts_a_23\" data-locale-parts--a=\"23\" data-child-locale-parts--a=\"4\" type=\"checkbox\" value=\"a,23\" name=\"oth.place[]\" data-location=\"parts\">Gifu </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__22\"> <label for=\"_select_checkbox_parts_a_22\"><input id=\"_select_checkbox_parts_a_22\" data-locale-parts--a=\"22\" data-child-locale-parts--a=\"4\" type=\"checkbox\" value=\"a,22\" name=\"oth.place[]\" data-location=\"parts\">Shizuoka </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__21\"> <label for=\"_select_checkbox_parts_a_21\"><input id=\"_select_checkbox_parts_a_21\" data-locale-parts--a=\"21\" data-child-locale-parts--a=\"4\" type=\"checkbox\" value=\"a,21\" name=\"oth.place[]\" data-location=\"parts\">Aichi </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__24\"> <label for=\"_select_checkbox_parts_a_24\"><input id=\"_select_checkbox_parts_a_24\" data-locale-parts--a=\"24\" data-child-locale-parts--a=\"4\" type=\"checkbox\" value=\"a,24\" name=\"oth.place[]\" data-location=\"parts\">Mie </label></div></td> </tr> <tr><td class=\"centerColor\" rowspan=\"6\"> <div class=\"checkbox\" id=\"_select_parts_s__5\"> <label for=\"_select_checkbox_parts_s_5\"><input id=\"_select_checkbox_parts_s_5\" data-locale-parts--s=\"5\" data-child-locale-parts--s=\"1\" type=\"checkbox\" value=\"s,5\" name=\"oth.place[]\" data-location=\"parts\">Kinki </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__25\"> <label for=\"_select_checkbox_parts_a_25\"><input id=\"_select_checkbox_parts_a_25\" data-locale-parts--a=\"25\" data-child-locale-parts--a=\"5\" type=\"checkbox\" value=\"a,25\" name=\"oth.place[]\" data-location=\"parts\">Shiga </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__26\"> <label for=\"_select_checkbox_parts_a_26\"><input id=\"_select_checkbox_parts_a_26\" data-locale-parts--a=\"26\" data-child-locale-parts--a=\"5\" type=\"checkbox\" value=\"a,26\" name=\"oth.place[]\" data-location=\"parts\">Kyoto </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__27\"> <label for=\"_select_checkbox_parts_a_27\"><input id=\"_select_checkbox_parts_a_27\" data-locale-parts--a=\"27\" data-child-locale-parts--a=\"5\" type=\"checkbox\" value=\"a,27\" name=\"oth.place[]\" data-location=\"parts\">Osaka </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__28\"> <label for=\"_select_checkbox_parts_a_28\"><input id=\"_select_checkbox_parts_a_28\" data-locale-parts--a=\"28\" data-child-locale-parts--a=\"5\" type=\"checkbox\" value=\"a,28\" name=\"oth.place[]\" data-location=\"parts\">Hyogo </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__29\"> <label for=\"_select_checkbox_parts_a_29\"><input id=\"_select_checkbox_parts_a_29\" data-locale-parts--a=\"29\" data-child-locale-parts--a=\"5\" type=\"checkbox\" value=\"a,29\" name=\"oth.place[]\" data-location=\"parts\">Nara </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__30\"> <label for=\"_select_checkbox_parts_a_30\"><input id=\"_select_checkbox_parts_a_30\" data-locale-parts--a=\"30\" data-child-locale-parts--a=\"5\" type=\"checkbox\" value=\"a,30\" name=\"oth.place[]\" data-location=\"parts\">Wakayama </label></div></td> </tr> <tr><td class=\"centerColor\" rowspan=\"9\"> <div class=\"checkbox\" id=\"_select_parts_s__6\"> <label for=\"_select_checkbox_parts_s_6\"><input id=\"_select_checkbox_parts_s_6\" data-locale-parts--s=\"6\" data-child-locale-parts--s=\"1\" type=\"checkbox\" value=\"s,6\" name=\"oth.place[]\" data-location=\"parts\">Chugoku/Shikoku </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__31\"> <label for=\"_select_checkbox_parts_a_31\"><input id=\"_select_checkbox_parts_a_31\" data-locale-parts--a=\"31\" data-child-locale-parts--a=\"6\" type=\"checkbox\" value=\"a,31\" name=\"oth.place[]\" data-location=\"parts\">Tottori </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__32\"> <label for=\"_select_checkbox_parts_a_32\"><input id=\"_select_checkbox_parts_a_32\" data-locale-parts--a=\"32\" data-child-locale-parts--a=\"6\" type=\"checkbox\" value=\"a,32\" name=\"oth.place[]\" data-location=\"parts\">Shimane </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__33\"> <label for=\"_select_checkbox_parts_a_33\"><input id=\"_select_checkbox_parts_a_33\" data-locale-parts--a=\"33\" data-child-locale-parts--a=\"6\" type=\"checkbox\" value=\"a,33\" name=\"oth.place[]\" data-location=\"parts\">Okayama </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__34\"> <label for=\"_select_checkbox_parts_a_34\"><input id=\"_select_checkbox_parts_a_34\" data-locale-parts--a=\"34\" data-child-locale-parts--a=\"6\" type=\"checkbox\" value=\"a,34\" name=\"oth.place[]\" data-location=\"parts\">Hiroshima </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__35\"> <label for=\"_select_checkbox_parts_a_35\"><input id=\"_select_checkbox_parts_a_35\" data-locale-parts--a=\"35\" data-child-locale-parts--a=\"6\" type=\"checkbox\" value=\"a,35\" name=\"oth.place[]\" data-location=\"parts\">Yamaguchi </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__36\"> <label for=\"_select_checkbox_parts_a_36\"><input id=\"_select_checkbox_parts_a_36\" data-locale-parts--a=\"36\" data-child-locale-parts--a=\"6\" type=\"checkbox\" value=\"a,36\" name=\"oth.place[]\" data-location=\"parts\">Tokushima </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__37\"> <label for=\"_select_checkbox_parts_a_37\"><input id=\"_select_checkbox_parts_a_37\" data-locale-parts--a=\"37\" data-child-locale-parts--a=\"6\" type=\"checkbox\" value=\"a,37\" name=\"oth.place[]\" data-location=\"parts\">Kagawa </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__38\"> <label for=\"_select_checkbox_parts_a_38\"><input id=\"_select_checkbox_parts_a_38\" data-locale-parts--a=\"38\" data-child-locale-parts--a=\"6\" type=\"checkbox\" value=\"a,38\" name=\"oth.place[]\" data-location=\"parts\">Ehime </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__39\"> <label for=\"_select_checkbox_parts_a_39\"><input id=\"_select_checkbox_parts_a_39\" data-locale-parts--a=\"39\" data-child-locale-parts--a=\"6\" type=\"checkbox\" value=\"a,39\" name=\"oth.place[]\" data-location=\"parts\">Kouchi </label></div></td> </tr> <tr><td class=\"centerColor\" rowspan=\"8\"> <div class=\"checkbox\" id=\"_select_parts_s__7\"> <label for=\"_select_checkbox_parts_s_7\"><input id=\"_select_checkbox_parts_s_7\" data-locale-parts--s=\"7\" data-child-locale-parts--s=\"1\" type=\"checkbox\" value=\"s,7\" name=\"oth.place[]\" data-location=\"parts\">Kyusyu/Okinawa </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__40\"> <label for=\"_select_checkbox_parts_a_40\"><input id=\"_select_checkbox_parts_a_40\" data-locale-parts--a=\"40\" data-child-locale-parts--a=\"7\" type=\"checkbox\" value=\"a,40\" name=\"oth.place[]\" data-location=\"parts\">Fukuoka </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__41\"> <label for=\"_select_checkbox_parts_a_41\"><input id=\"_select_checkbox_parts_a_41\" data-locale-parts--a=\"41\" data-child-locale-parts--a=\"7\" type=\"checkbox\" value=\"a,41\" name=\"oth.place[]\" data-location=\"parts\">Saga </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__42\"> <label for=\"_select_checkbox_parts_a_42\"><input id=\"_select_checkbox_parts_a_42\" data-locale-parts--a=\"42\" data-child-locale-parts--a=\"7\" type=\"checkbox\" value=\"a,42\" name=\"oth.place[]\" data-location=\"parts\">Nagasaki </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__43\"> <label for=\"_select_checkbox_parts_a_43\"><input id=\"_select_checkbox_parts_a_43\" data-locale-parts--a=\"43\" data-child-locale-parts--a=\"7\" type=\"checkbox\" value=\"a,43\" name=\"oth.place[]\" data-location=\"parts\">Kumamoto </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__44\"> <label for=\"_select_checkbox_parts_a_44\"><input id=\"_select_checkbox_parts_a_44\" data-locale-parts--a=\"44\" data-child-locale-parts--a=\"7\" type=\"checkbox\" value=\"a,44\" name=\"oth.place[]\" data-location=\"parts\">Ohita </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__45\"> <label for=\"_select_checkbox_parts_a_45\"><input id=\"_select_checkbox_parts_a_45\" data-locale-parts--a=\"45\" data-child-locale-parts--a=\"7\" type=\"checkbox\" value=\"a,45\" name=\"oth.place[]\" data-location=\"parts\">Miyazaki </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__46\"> <label for=\"_select_checkbox_parts_a_46\"><input id=\"_select_checkbox_parts_a_46\" data-locale-parts--a=\"46\" data-child-locale-parts--a=\"7\" type=\"checkbox\" value=\"a,46\" name=\"oth.place[]\" data-location=\"parts\">Kagoshima </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__47\"> <label for=\"_select_checkbox_parts_a_47\"><input id=\"_select_checkbox_parts_a_47\" data-locale-parts--a=\"47\" data-child-locale-parts--a=\"7\" type=\"checkbox\" value=\"a,47\" name=\"oth.place[]\" data-location=\"parts\">Okinawa </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__3\"> <label for=\"_select_checkbox_parts_n_3\"><input id=\"_select_checkbox_parts_n_3\" data-locale-parts--n=\"3\" data-child-locale-parts--n=\"59\" type=\"checkbox\" value=\"n,3\" name=\"oth.place[]\" data-location=\"parts\">韩国 </label></div></td> </tr> <tr><td class=\"categoryColor\" rowspan=\"9\"> <span id=\"_select_parts_r__60\">东南亚</span></td><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__5\"> <label for=\"_select_checkbox_parts_n_5\"><input id=\"_select_checkbox_parts_n_5\" data-locale-parts--n=\"5\" data-child-locale-parts--n=\"60\" type=\"checkbox\" value=\"n,5\" name=\"oth.place[]\" data-location=\"parts\">泰国 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__6\"> <label for=\"_select_checkbox_parts_n_6\"><input id=\"_select_checkbox_parts_n_6\" data-locale-parts--n=\"6\" data-child-locale-parts--n=\"60\" type=\"checkbox\" value=\"n,6\" name=\"oth.place[]\" data-location=\"parts\">马来西亚 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__7\"> <label for=\"_select_checkbox_parts_n_7\"><input id=\"_select_checkbox_parts_n_7\" data-locale-parts--n=\"7\" data-child-locale-parts--n=\"60\" type=\"checkbox\" value=\"n,7\" name=\"oth.place[]\" data-location=\"parts\">印度尼西亚 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__8\"> <label for=\"_select_checkbox_parts_n_8\"><input id=\"_select_checkbox_parts_n_8\" data-locale-parts--n=\"8\" data-child-locale-parts--n=\"60\" type=\"checkbox\" value=\"n,8\" name=\"oth.place[]\" data-location=\"parts\">菲律宾 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__9\"> <label for=\"_select_checkbox_parts_n_9\"><input id=\"_select_checkbox_parts_n_9\" data-locale-parts--n=\"9\" data-child-locale-parts--n=\"60\" type=\"checkbox\" value=\"n,9\" name=\"oth.place[]\" data-location=\"parts\">越南 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__10\"> <label for=\"_select_checkbox_parts_n_10\"><input id=\"_select_checkbox_parts_n_10\" data-locale-parts--n=\"10\" data-child-locale-parts--n=\"60\" type=\"checkbox\" value=\"n,10\" name=\"oth.place[]\" data-location=\"parts\">新加坡 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__11\"> <label for=\"_select_checkbox_parts_n_11\"><input id=\"_select_checkbox_parts_n_11\" data-locale-parts--n=\"11\" data-child-locale-parts--n=\"60\" type=\"checkbox\" value=\"n,11\" name=\"oth.place[]\" data-location=\"parts\">柬埔寨 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__12\"> <label for=\"_select_checkbox_parts_n_12\"><input id=\"_select_checkbox_parts_n_12\" data-locale-parts--n=\"12\" data-child-locale-parts--n=\"60\" type=\"checkbox\" value=\"n,12\" name=\"oth.place[]\" data-location=\"parts\">缅甸 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__13\"> <label for=\"_select_checkbox_parts_n_13\"><input id=\"_select_checkbox_parts_n_13\" data-locale-parts--n=\"13\" data-child-locale-parts--n=\"60\" type=\"checkbox\" value=\"n,13\" name=\"oth.place[]\" data-location=\"parts\">老挝 </label></div></td> </tr> <tr><td class=\"categoryColor\" rowspan=\"28\"> <span id=\"_select_parts_r__61\">南亚/大洋洲</span></td><td class=\"topColor\" rowspan=\"24\"> <div class=\"checkbox\" id=\"_select_parts_n__14\"> <label for=\"_select_checkbox_parts_n_14\"><input id=\"_select_checkbox_parts_n_14\" data-locale-parts--n=\"14\" data-child-locale-parts--n=\"61\" type=\"checkbox\" value=\"n,14\" name=\"oth.place[]\" data-location=\"parts\">印度 </label></div></td><td class=\"centerColor\" rowspan=\"9\"> <div class=\"checkbox\" id=\"_select_parts_s__15\"> <label for=\"_select_checkbox_parts_s_15\"><input id=\"_select_checkbox_parts_s_15\" data-locale-parts--s=\"15\" data-child-locale-parts--s=\"14\" type=\"checkbox\" value=\"s,15\" name=\"oth.place[]\" data-location=\"parts\">North (Delhi, Gurgaon, etc.) </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__48\"> <label for=\"_select_checkbox_parts_a_48\"><input id=\"_select_checkbox_parts_a_48\" data-locale-parts--a=\"48\" data-child-locale-parts--a=\"15\" type=\"checkbox\" value=\"a,48\" name=\"oth.place[]\" data-location=\"parts\">Delhi </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__49\"> <label for=\"_select_checkbox_parts_a_49\"><input id=\"_select_checkbox_parts_a_49\" data-locale-parts--a=\"49\" data-child-locale-parts--a=\"15\" type=\"checkbox\" value=\"a,49\" name=\"oth.place[]\" data-location=\"parts\">Uttar Pradesh </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__50\"> <label for=\"_select_checkbox_parts_a_50\"><input id=\"_select_checkbox_parts_a_50\" data-locale-parts--a=\"50\" data-child-locale-parts--a=\"15\" type=\"checkbox\" value=\"a,50\" name=\"oth.place[]\" data-location=\"parts\">Uttarakhand </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__51\"> <label for=\"_select_checkbox_parts_a_51\"><input id=\"_select_checkbox_parts_a_51\" data-locale-parts--a=\"51\" data-child-locale-parts--a=\"15\" type=\"checkbox\" value=\"a,51\" name=\"oth.place[]\" data-location=\"parts\">Jammu and Kashmir </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__52\"> <label for=\"_select_checkbox_parts_a_52\"><input id=\"_select_checkbox_parts_a_52\" data-locale-parts--a=\"52\" data-child-locale-parts--a=\"15\" type=\"checkbox\" value=\"a,52\" name=\"oth.place[]\" data-location=\"parts\">Punjab </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__53\"> <label for=\"_select_checkbox_parts_a_53\"><input id=\"_select_checkbox_parts_a_53\" data-locale-parts--a=\"53\" data-child-locale-parts--a=\"15\" type=\"checkbox\" value=\"a,53\" name=\"oth.place[]\" data-location=\"parts\">Haryana </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__54\"> <label for=\"_select_checkbox_parts_a_54\"><input id=\"_select_checkbox_parts_a_54\" data-locale-parts--a=\"54\" data-child-locale-parts--a=\"15\" type=\"checkbox\" value=\"a,54\" name=\"oth.place[]\" data-location=\"parts\">Himachal Pradesh </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__55\"> <label for=\"_select_checkbox_parts_a_55\"><input id=\"_select_checkbox_parts_a_55\" data-locale-parts--a=\"55\" data-child-locale-parts--a=\"15\" type=\"checkbox\" value=\"a,55\" name=\"oth.place[]\" data-location=\"parts\">Chandigarh </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__56\"> <label for=\"_select_checkbox_parts_a_56\"><input id=\"_select_checkbox_parts_a_56\" data-locale-parts--a=\"56\" data-child-locale-parts--a=\"15\" type=\"checkbox\" value=\"a,56\" name=\"oth.place[]\" data-location=\"parts\">Rajasthan </label></div></td> </tr> <tr><td class=\"centerColor\" rowspan=\"4\"> <div class=\"checkbox\" id=\"_select_parts_s__16\"> <label for=\"_select_checkbox_parts_s_16\"><input id=\"_select_checkbox_parts_s_16\" data-locale-parts--s=\"16\" data-child-locale-parts--s=\"14\" type=\"checkbox\" value=\"s,16\" name=\"oth.place[]\" data-location=\"parts\">East (Kolkata, etc.) </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__57\"> <label for=\"_select_checkbox_parts_a_57\"><input id=\"_select_checkbox_parts_a_57\" data-locale-parts--a=\"57\" data-child-locale-parts--a=\"16\" type=\"checkbox\" value=\"a,57\" name=\"oth.place[]\" data-location=\"parts\">Orissa </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__58\"> <label for=\"_select_checkbox_parts_a_58\"><input id=\"_select_checkbox_parts_a_58\" data-locale-parts--a=\"58\" data-child-locale-parts--a=\"16\" type=\"checkbox\" value=\"a,58\" name=\"oth.place[]\" data-location=\"parts\">West Bengal </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__59\"> <label for=\"_select_checkbox_parts_a_59\"><input id=\"_select_checkbox_parts_a_59\" data-locale-parts--a=\"59\" data-child-locale-parts--a=\"16\" type=\"checkbox\" value=\"a,59\" name=\"oth.place[]\" data-location=\"parts\">Bihar </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__60\"> <label for=\"_select_checkbox_parts_a_60\"><input id=\"_select_checkbox_parts_a_60\" data-locale-parts--a=\"60\" data-child-locale-parts--a=\"16\" type=\"checkbox\" value=\"a,60\" name=\"oth.place[]\" data-location=\"parts\">Jharkhand </label></div></td> </tr> <tr><td class=\"centerColor\" rowspan=\"5\"> <div class=\"checkbox\" id=\"_select_parts_s__17\"> <label for=\"_select_checkbox_parts_s_17\"><input id=\"_select_checkbox_parts_s_17\" data-locale-parts--s=\"17\" data-child-locale-parts--s=\"14\" type=\"checkbox\" value=\"s,17\" name=\"oth.place[]\" data-location=\"parts\">West (Mumbai, Pune, etc.) </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__61\"> <label for=\"_select_checkbox_parts_a_61\"><input id=\"_select_checkbox_parts_a_61\" data-locale-parts--a=\"61\" data-child-locale-parts--a=\"17\" type=\"checkbox\" value=\"a,61\" name=\"oth.place[]\" data-location=\"parts\">Madhya Pradesh </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__62\"> <label for=\"_select_checkbox_parts_a_62\"><input id=\"_select_checkbox_parts_a_62\" data-locale-parts--a=\"62\" data-child-locale-parts--a=\"17\" type=\"checkbox\" value=\"a,62\" name=\"oth.place[]\" data-location=\"parts\">Chhattisgarh </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__63\"> <label for=\"_select_checkbox_parts_a_63\"><input id=\"_select_checkbox_parts_a_63\" data-locale-parts--a=\"63\" data-child-locale-parts--a=\"17\" type=\"checkbox\" value=\"a,63\" name=\"oth.place[]\" data-location=\"parts\">Gujarat </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__64\"> <label for=\"_select_checkbox_parts_a_64\"><input id=\"_select_checkbox_parts_a_64\" data-locale-parts--a=\"64\" data-child-locale-parts--a=\"17\" type=\"checkbox\" value=\"a,64\" name=\"oth.place[]\" data-location=\"parts\">Maharashtra </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__65\"> <label for=\"_select_checkbox_parts_a_65\"><input id=\"_select_checkbox_parts_a_65\" data-locale-parts--a=\"65\" data-child-locale-parts--a=\"17\" type=\"checkbox\" value=\"a,65\" name=\"oth.place[]\" data-location=\"parts\">Goa </label></div></td> </tr> <tr><td class=\"centerColor\" rowspan=\"6\"> <div class=\"checkbox\" id=\"_select_parts_s__18\"> <label for=\"_select_checkbox_parts_s_18\"><input id=\"_select_checkbox_parts_s_18\" data-locale-parts--s=\"18\" data-child-locale-parts--s=\"14\" type=\"checkbox\" value=\"s,18\" name=\"oth.place[]\" data-location=\"parts\">South (Bangalore, Chennai, etc.) </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__66\"> <label for=\"_select_checkbox_parts_a_66\"><input id=\"_select_checkbox_parts_a_66\" data-locale-parts--a=\"66\" data-child-locale-parts--a=\"18\" type=\"checkbox\" value=\"a,66\" name=\"oth.place[]\" data-location=\"parts\">Andhra Pradesh </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__67\"> <label for=\"_select_checkbox_parts_a_67\"><input id=\"_select_checkbox_parts_a_67\" data-locale-parts--a=\"67\" data-child-locale-parts--a=\"18\" type=\"checkbox\" value=\"a,67\" name=\"oth.place[]\" data-location=\"parts\">Karnataka </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__68\"> <label for=\"_select_checkbox_parts_a_68\"><input id=\"_select_checkbox_parts_a_68\" data-locale-parts--a=\"68\" data-child-locale-parts--a=\"18\" type=\"checkbox\" value=\"a,68\" name=\"oth.place[]\" data-location=\"parts\">Kerala </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__69\"> <label for=\"_select_checkbox_parts_a_69\"><input id=\"_select_checkbox_parts_a_69\" data-locale-parts--a=\"69\" data-child-locale-parts--a=\"18\" type=\"checkbox\" value=\"a,69\" name=\"oth.place[]\" data-location=\"parts\">Tamil Nadu </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__70\"> <label for=\"_select_checkbox_parts_a_70\"><input id=\"_select_checkbox_parts_a_70\" data-locale-parts--a=\"70\" data-child-locale-parts--a=\"18\" type=\"checkbox\" value=\"a,70\" name=\"oth.place[]\" data-location=\"parts\">Pondicherry </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__155\"> <label for=\"_select_checkbox_parts_a_155\"><input id=\"_select_checkbox_parts_a_155\" data-locale-parts--a=\"155\" data-child-locale-parts--a=\"18\" type=\"checkbox\" value=\"a,155\" name=\"oth.place[]\" data-location=\"parts\">Telangana </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__15\"> <label for=\"_select_checkbox_parts_n_15\"><input id=\"_select_checkbox_parts_n_15\" data-locale-parts--n=\"15\" data-child-locale-parts--n=\"61\" type=\"checkbox\" value=\"n,15\" name=\"oth.place[]\" data-location=\"parts\">巴基斯坦 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__16\"> <label for=\"_select_checkbox_parts_n_16\"><input id=\"_select_checkbox_parts_n_16\" data-locale-parts--n=\"16\" data-child-locale-parts--n=\"61\" type=\"checkbox\" value=\"n,16\" name=\"oth.place[]\" data-location=\"parts\">斯里兰卡 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__17\"> <label for=\"_select_checkbox_parts_n_17\"><input id=\"_select_checkbox_parts_n_17\" data-locale-parts--n=\"17\" data-child-locale-parts--n=\"61\" type=\"checkbox\" value=\"n,17\" name=\"oth.place[]\" data-location=\"parts\">澳大利亚 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__18\"> <label for=\"_select_checkbox_parts_n_18\"><input id=\"_select_checkbox_parts_n_18\" data-locale-parts--n=\"18\" data-child-locale-parts--n=\"61\" type=\"checkbox\" value=\"n,18\" name=\"oth.place[]\" data-location=\"parts\">新西兰 </label></div></td> </tr> <tr><td class=\"categoryColor\" rowspan=\"53\"> <span id=\"_select_parts_r__62\">北美</span></td><td class=\"topColor\" rowspan=\"51\"> <div class=\"checkbox\" id=\"_select_parts_n__19\"> <label for=\"_select_checkbox_parts_n_19\"><input id=\"_select_checkbox_parts_n_19\" data-locale-parts--n=\"19\" data-child-locale-parts--n=\"62\" type=\"checkbox\" value=\"n,19\" name=\"oth.place[]\" data-location=\"parts\">美国 </label></div></td><td class=\"centerColor\" rowspan=\"10\"> <div class=\"checkbox\" id=\"_select_parts_s__19\"> <label for=\"_select_checkbox_parts_s_19\"><input id=\"_select_checkbox_parts_s_19\" data-locale-parts--s=\"19\" data-child-locale-parts--s=\"19\" type=\"checkbox\" value=\"s,19\" name=\"oth.place[]\" data-location=\"parts\">Mid West </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__95\"> <label for=\"_select_checkbox_parts_a_95\"><input id=\"_select_checkbox_parts_a_95\" data-locale-parts--a=\"95\" data-child-locale-parts--a=\"19\" type=\"checkbox\" value=\"a,95\" name=\"oth.place[]\" data-location=\"parts\">Illinois </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__96\"> <label for=\"_select_checkbox_parts_a_96\"><input id=\"_select_checkbox_parts_a_96\" data-locale-parts--a=\"96\" data-child-locale-parts--a=\"19\" type=\"checkbox\" value=\"a,96\" name=\"oth.place[]\" data-location=\"parts\">Indiana </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__97\"> <label for=\"_select_checkbox_parts_a_97\"><input id=\"_select_checkbox_parts_a_97\" data-locale-parts--a=\"97\" data-child-locale-parts--a=\"19\" type=\"checkbox\" value=\"a,97\" name=\"oth.place[]\" data-location=\"parts\">Iowa </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__98\"> <label for=\"_select_checkbox_parts_a_98\"><input id=\"_select_checkbox_parts_a_98\" data-locale-parts--a=\"98\" data-child-locale-parts--a=\"19\" type=\"checkbox\" value=\"a,98\" name=\"oth.place[]\" data-location=\"parts\">Kentucky </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__99\"> <label for=\"_select_checkbox_parts_a_99\"><input id=\"_select_checkbox_parts_a_99\" data-locale-parts--a=\"99\" data-child-locale-parts--a=\"19\" type=\"checkbox\" value=\"a,99\" name=\"oth.place[]\" data-location=\"parts\">Michigan </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__100\"> <label for=\"_select_checkbox_parts_a_100\"><input id=\"_select_checkbox_parts_a_100\" data-locale-parts--a=\"100\" data-child-locale-parts--a=\"19\" type=\"checkbox\" value=\"a,100\" name=\"oth.place[]\" data-location=\"parts\">Minnesota </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__101\"> <label for=\"_select_checkbox_parts_a_101\"><input id=\"_select_checkbox_parts_a_101\" data-locale-parts--a=\"101\" data-child-locale-parts--a=\"19\" type=\"checkbox\" value=\"a,101\" name=\"oth.place[]\" data-location=\"parts\">Missouri </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__102\"> <label for=\"_select_checkbox_parts_a_102\"><input id=\"_select_checkbox_parts_a_102\" data-locale-parts--a=\"102\" data-child-locale-parts--a=\"19\" type=\"checkbox\" value=\"a,102\" name=\"oth.place[]\" data-location=\"parts\">Ohio </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__103\"> <label for=\"_select_checkbox_parts_a_103\"><input id=\"_select_checkbox_parts_a_103\" data-locale-parts--a=\"103\" data-child-locale-parts--a=\"19\" type=\"checkbox\" value=\"a,103\" name=\"oth.place[]\" data-location=\"parts\">Pennsylvania </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__104\"> <label for=\"_select_checkbox_parts_a_104\"><input id=\"_select_checkbox_parts_a_104\" data-locale-parts--a=\"104\" data-child-locale-parts--a=\"19\" type=\"checkbox\" value=\"a,104\" name=\"oth.place[]\" data-location=\"parts\">Wisconsin </label></div></td> </tr> <tr><td class=\"centerColor\" rowspan=\"10\"> <div class=\"checkbox\" id=\"_select_parts_s__20\"> <label for=\"_select_checkbox_parts_s_20\"><input id=\"_select_checkbox_parts_s_20\" data-locale-parts--s=\"20\" data-child-locale-parts--s=\"19\" type=\"checkbox\" value=\"s,20\" name=\"oth.place[]\" data-location=\"parts\">South </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__105\"> <label for=\"_select_checkbox_parts_a_105\"><input id=\"_select_checkbox_parts_a_105\" data-locale-parts--a=\"105\" data-child-locale-parts--a=\"20\" type=\"checkbox\" value=\"a,105\" name=\"oth.place[]\" data-location=\"parts\">Alabama </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__106\"> <label for=\"_select_checkbox_parts_a_106\"><input id=\"_select_checkbox_parts_a_106\" data-locale-parts--a=\"106\" data-child-locale-parts--a=\"20\" type=\"checkbox\" value=\"a,106\" name=\"oth.place[]\" data-location=\"parts\">Arkansas </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__107\"> <label for=\"_select_checkbox_parts_a_107\"><input id=\"_select_checkbox_parts_a_107\" data-locale-parts--a=\"107\" data-child-locale-parts--a=\"20\" type=\"checkbox\" value=\"a,107\" name=\"oth.place[]\" data-location=\"parts\">Florida </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__108\"> <label for=\"_select_checkbox_parts_a_108\"><input id=\"_select_checkbox_parts_a_108\" data-locale-parts--a=\"108\" data-child-locale-parts--a=\"20\" type=\"checkbox\" value=\"a,108\" name=\"oth.place[]\" data-location=\"parts\">Georgia </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__109\"> <label for=\"_select_checkbox_parts_a_109\"><input id=\"_select_checkbox_parts_a_109\" data-locale-parts--a=\"109\" data-child-locale-parts--a=\"20\" type=\"checkbox\" value=\"a,109\" name=\"oth.place[]\" data-location=\"parts\">Louisiana </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__110\"> <label for=\"_select_checkbox_parts_a_110\"><input id=\"_select_checkbox_parts_a_110\" data-locale-parts--a=\"110\" data-child-locale-parts--a=\"20\" type=\"checkbox\" value=\"a,110\" name=\"oth.place[]\" data-location=\"parts\">Mississippi </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__111\"> <label for=\"_select_checkbox_parts_a_111\"><input id=\"_select_checkbox_parts_a_111\" data-locale-parts--a=\"111\" data-child-locale-parts--a=\"20\" type=\"checkbox\" value=\"a,111\" name=\"oth.place[]\" data-location=\"parts\">North Carolina </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__112\"> <label for=\"_select_checkbox_parts_a_112\"><input id=\"_select_checkbox_parts_a_112\" data-locale-parts--a=\"112\" data-child-locale-parts--a=\"20\" type=\"checkbox\" value=\"a,112\" name=\"oth.place[]\" data-location=\"parts\">Oklahoma </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__113\"> <label for=\"_select_checkbox_parts_a_113\"><input id=\"_select_checkbox_parts_a_113\" data-locale-parts--a=\"113\" data-child-locale-parts--a=\"20\" type=\"checkbox\" value=\"a,113\" name=\"oth.place[]\" data-location=\"parts\">South Carolina </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__114\"> <label for=\"_select_checkbox_parts_a_114\"><input id=\"_select_checkbox_parts_a_114\" data-locale-parts--a=\"114\" data-child-locale-parts--a=\"20\" type=\"checkbox\" value=\"a,114\" name=\"oth.place[]\" data-location=\"parts\">Tennessee </label></div></td> </tr> <tr><td class=\"centerColor\" rowspan=\"13\"> <div class=\"checkbox\" id=\"_select_parts_s__21\"> <label for=\"_select_checkbox_parts_s_21\"><input id=\"_select_checkbox_parts_s_21\" data-locale-parts--s=\"21\" data-child-locale-parts--s=\"19\" type=\"checkbox\" value=\"s,21\" name=\"oth.place[]\" data-location=\"parts\">East </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__115\"> <label for=\"_select_checkbox_parts_a_115\"><input id=\"_select_checkbox_parts_a_115\" data-locale-parts--a=\"115\" data-child-locale-parts--a=\"21\" type=\"checkbox\" value=\"a,115\" name=\"oth.place[]\" data-location=\"parts\">Connecticut </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__116\"> <label for=\"_select_checkbox_parts_a_116\"><input id=\"_select_checkbox_parts_a_116\" data-locale-parts--a=\"116\" data-child-locale-parts--a=\"21\" type=\"checkbox\" value=\"a,116\" name=\"oth.place[]\" data-location=\"parts\">Delaware </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__117\"> <label for=\"_select_checkbox_parts_a_117\"><input id=\"_select_checkbox_parts_a_117\" data-locale-parts--a=\"117\" data-child-locale-parts--a=\"21\" type=\"checkbox\" value=\"a,117\" name=\"oth.place[]\" data-location=\"parts\">Washington, DC </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__118\"> <label for=\"_select_checkbox_parts_a_118\"><input id=\"_select_checkbox_parts_a_118\" data-locale-parts--a=\"118\" data-child-locale-parts--a=\"21\" type=\"checkbox\" value=\"a,118\" name=\"oth.place[]\" data-location=\"parts\">Maine </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__119\"> <label for=\"_select_checkbox_parts_a_119\"><input id=\"_select_checkbox_parts_a_119\" data-locale-parts--a=\"119\" data-child-locale-parts--a=\"21\" type=\"checkbox\" value=\"a,119\" name=\"oth.place[]\" data-location=\"parts\">Maryland </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__120\"> <label for=\"_select_checkbox_parts_a_120\"><input id=\"_select_checkbox_parts_a_120\" data-locale-parts--a=\"120\" data-child-locale-parts--a=\"21\" type=\"checkbox\" value=\"a,120\" name=\"oth.place[]\" data-location=\"parts\">Massachusetts </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__121\"> <label for=\"_select_checkbox_parts_a_121\"><input id=\"_select_checkbox_parts_a_121\" data-locale-parts--a=\"121\" data-child-locale-parts--a=\"21\" type=\"checkbox\" value=\"a,121\" name=\"oth.place[]\" data-location=\"parts\">New Hampshire </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__122\"> <label for=\"_select_checkbox_parts_a_122\"><input id=\"_select_checkbox_parts_a_122\" data-locale-parts--a=\"122\" data-child-locale-parts--a=\"21\" type=\"checkbox\" value=\"a,122\" name=\"oth.place[]\" data-location=\"parts\">New Jersey </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__123\"> <label for=\"_select_checkbox_parts_a_123\"><input id=\"_select_checkbox_parts_a_123\" data-locale-parts--a=\"123\" data-child-locale-parts--a=\"21\" type=\"checkbox\" value=\"a,123\" name=\"oth.place[]\" data-location=\"parts\">New York </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__124\"> <label for=\"_select_checkbox_parts_a_124\"><input id=\"_select_checkbox_parts_a_124\" data-locale-parts--a=\"124\" data-child-locale-parts--a=\"21\" type=\"checkbox\" value=\"a,124\" name=\"oth.place[]\" data-location=\"parts\">Rhode Island </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__125\"> <label for=\"_select_checkbox_parts_a_125\"><input id=\"_select_checkbox_parts_a_125\" data-locale-parts--a=\"125\" data-child-locale-parts--a=\"21\" type=\"checkbox\" value=\"a,125\" name=\"oth.place[]\" data-location=\"parts\">Vermont </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__126\"> <label for=\"_select_checkbox_parts_a_126\"><input id=\"_select_checkbox_parts_a_126\" data-locale-parts--a=\"126\" data-child-locale-parts--a=\"21\" type=\"checkbox\" value=\"a,126\" name=\"oth.place[]\" data-location=\"parts\">Virginia </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__127\"> <label for=\"_select_checkbox_parts_a_127\"><input id=\"_select_checkbox_parts_a_127\" data-locale-parts--a=\"127\" data-child-locale-parts--a=\"21\" type=\"checkbox\" value=\"a,127\" name=\"oth.place[]\" data-location=\"parts\">West Virginia </label></div></td> </tr> <tr><td class=\"centerColor\" rowspan=\"18\"> <div class=\"checkbox\" id=\"_select_parts_s__22\"> <label for=\"_select_checkbox_parts_s_22\"><input id=\"_select_checkbox_parts_s_22\" data-locale-parts--s=\"22\" data-child-locale-parts--s=\"19\" type=\"checkbox\" value=\"s,22\" name=\"oth.place[]\" data-location=\"parts\">West </label></div></td><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__128\"> <label for=\"_select_checkbox_parts_a_128\"><input id=\"_select_checkbox_parts_a_128\" data-locale-parts--a=\"128\" data-child-locale-parts--a=\"22\" type=\"checkbox\" value=\"a,128\" name=\"oth.place[]\" data-location=\"parts\">Alaska </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__129\"> <label for=\"_select_checkbox_parts_a_129\"><input id=\"_select_checkbox_parts_a_129\" data-locale-parts--a=\"129\" data-child-locale-parts--a=\"22\" type=\"checkbox\" value=\"a,129\" name=\"oth.place[]\" data-location=\"parts\">Arizona </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__130\"> <label for=\"_select_checkbox_parts_a_130\"><input id=\"_select_checkbox_parts_a_130\" data-locale-parts--a=\"130\" data-child-locale-parts--a=\"22\" type=\"checkbox\" value=\"a,130\" name=\"oth.place[]\" data-location=\"parts\">California </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__131\"> <label for=\"_select_checkbox_parts_a_131\"><input id=\"_select_checkbox_parts_a_131\" data-locale-parts--a=\"131\" data-child-locale-parts--a=\"22\" type=\"checkbox\" value=\"a,131\" name=\"oth.place[]\" data-location=\"parts\">Colorado </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__132\"> <label for=\"_select_checkbox_parts_a_132\"><input id=\"_select_checkbox_parts_a_132\" data-locale-parts--a=\"132\" data-child-locale-parts--a=\"22\" type=\"checkbox\" value=\"a,132\" name=\"oth.place[]\" data-location=\"parts\">Hawaii </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__133\"> <label for=\"_select_checkbox_parts_a_133\"><input id=\"_select_checkbox_parts_a_133\" data-locale-parts--a=\"133\" data-child-locale-parts--a=\"22\" type=\"checkbox\" value=\"a,133\" name=\"oth.place[]\" data-location=\"parts\">Idaho </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__134\"> <label for=\"_select_checkbox_parts_a_134\"><input id=\"_select_checkbox_parts_a_134\" data-locale-parts--a=\"134\" data-child-locale-parts--a=\"22\" type=\"checkbox\" value=\"a,134\" name=\"oth.place[]\" data-location=\"parts\">Kansas </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__135\"> <label for=\"_select_checkbox_parts_a_135\"><input id=\"_select_checkbox_parts_a_135\" data-locale-parts--a=\"135\" data-child-locale-parts--a=\"22\" type=\"checkbox\" value=\"a,135\" name=\"oth.place[]\" data-location=\"parts\">Montana </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__136\"> <label for=\"_select_checkbox_parts_a_136\"><input id=\"_select_checkbox_parts_a_136\" data-locale-parts--a=\"136\" data-child-locale-parts--a=\"22\" type=\"checkbox\" value=\"a,136\" name=\"oth.place[]\" data-location=\"parts\">Nebraska </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__137\"> <label for=\"_select_checkbox_parts_a_137\"><input id=\"_select_checkbox_parts_a_137\" data-locale-parts--a=\"137\" data-child-locale-parts--a=\"22\" type=\"checkbox\" value=\"a,137\" name=\"oth.place[]\" data-location=\"parts\">Nevada </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__138\"> <label for=\"_select_checkbox_parts_a_138\"><input id=\"_select_checkbox_parts_a_138\" data-locale-parts--a=\"138\" data-child-locale-parts--a=\"22\" type=\"checkbox\" value=\"a,138\" name=\"oth.place[]\" data-location=\"parts\">New Mexico </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__139\"> <label for=\"_select_checkbox_parts_a_139\"><input id=\"_select_checkbox_parts_a_139\" data-locale-parts--a=\"139\" data-child-locale-parts--a=\"22\" type=\"checkbox\" value=\"a,139\" name=\"oth.place[]\" data-location=\"parts\">North Dakota </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__140\"> <label for=\"_select_checkbox_parts_a_140\"><input id=\"_select_checkbox_parts_a_140\" data-locale-parts--a=\"140\" data-child-locale-parts--a=\"22\" type=\"checkbox\" value=\"a,140\" name=\"oth.place[]\" data-location=\"parts\">Oregon </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__141\"> <label for=\"_select_checkbox_parts_a_141\"><input id=\"_select_checkbox_parts_a_141\" data-locale-parts--a=\"141\" data-child-locale-parts--a=\"22\" type=\"checkbox\" value=\"a,141\" name=\"oth.place[]\" data-location=\"parts\">South Dakota </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__142\"> <label for=\"_select_checkbox_parts_a_142\"><input id=\"_select_checkbox_parts_a_142\" data-locale-parts--a=\"142\" data-child-locale-parts--a=\"22\" type=\"checkbox\" value=\"a,142\" name=\"oth.place[]\" data-location=\"parts\">Texas </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__143\"> <label for=\"_select_checkbox_parts_a_143\"><input id=\"_select_checkbox_parts_a_143\" data-locale-parts--a=\"143\" data-child-locale-parts--a=\"22\" type=\"checkbox\" value=\"a,143\" name=\"oth.place[]\" data-location=\"parts\">Utah </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__144\"> <label for=\"_select_checkbox_parts_a_144\"><input id=\"_select_checkbox_parts_a_144\" data-locale-parts--a=\"144\" data-child-locale-parts--a=\"22\" type=\"checkbox\" value=\"a,144\" name=\"oth.place[]\" data-location=\"parts\">Washington </label></div></td> </tr> <tr><td class=\"\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_a__145\"> <label for=\"_select_checkbox_parts_a_145\"><input id=\"_select_checkbox_parts_a_145\" data-locale-parts--a=\"145\" data-child-locale-parts--a=\"22\" type=\"checkbox\" value=\"a,145\" name=\"oth.place[]\" data-location=\"parts\">Wyoming </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__20\"> <label for=\"_select_checkbox_parts_n_20\"><input id=\"_select_checkbox_parts_n_20\" data-locale-parts--n=\"20\" data-child-locale-parts--n=\"62\" type=\"checkbox\" value=\"n,20\" name=\"oth.place[]\" data-location=\"parts\">加拿大 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__21\"> <label for=\"_select_checkbox_parts_n_21\"><input id=\"_select_checkbox_parts_n_21\" data-locale-parts--n=\"21\" data-child-locale-parts--n=\"62\" type=\"checkbox\" value=\"n,21\" name=\"oth.place[]\" data-location=\"parts\">墨西哥 </label></div></td> </tr> <tr><td class=\"categoryColor\" rowspan=\"10\"> <span id=\"_select_parts_r__63\">中南美</span></td><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__23\"> <label for=\"_select_checkbox_parts_n_23\"><input id=\"_select_checkbox_parts_n_23\" data-locale-parts--n=\"23\" data-child-locale-parts--n=\"63\" type=\"checkbox\" value=\"n,23\" name=\"oth.place[]\" data-location=\"parts\">巴西 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__22\"> <label for=\"_select_checkbox_parts_n_22\"><input id=\"_select_checkbox_parts_n_22\" data-locale-parts--n=\"22\" data-child-locale-parts--n=\"63\" type=\"checkbox\" value=\"n,22\" name=\"oth.place[]\" data-location=\"parts\">阿根廷 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__27\"> <label for=\"_select_checkbox_parts_n_27\"><input id=\"_select_checkbox_parts_n_27\" data-locale-parts--n=\"27\" data-child-locale-parts--n=\"63\" type=\"checkbox\" value=\"n,27\" name=\"oth.place[]\" data-location=\"parts\">智利 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__26\"> <label for=\"_select_checkbox_parts_n_26\"><input id=\"_select_checkbox_parts_n_26\" data-locale-parts--n=\"26\" data-child-locale-parts--n=\"63\" type=\"checkbox\" value=\"n,26\" name=\"oth.place[]\" data-location=\"parts\">哥伦比亚 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__71\"> <label for=\"_select_checkbox_parts_n_71\"><input id=\"_select_checkbox_parts_n_71\" data-locale-parts--n=\"71\" data-child-locale-parts--n=\"63\" type=\"checkbox\" value=\"n,71\" name=\"oth.place[]\" data-location=\"parts\">乌拉圭 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__24\"> <label for=\"_select_checkbox_parts_n_24\"><input id=\"_select_checkbox_parts_n_24\" data-locale-parts--n=\"24\" data-child-locale-parts--n=\"63\" type=\"checkbox\" value=\"n,24\" name=\"oth.place[]\" data-location=\"parts\">委内瑞拉 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__25\"> <label for=\"_select_checkbox_parts_n_25\"><input id=\"_select_checkbox_parts_n_25\" data-locale-parts--n=\"25\" data-child-locale-parts--n=\"63\" type=\"checkbox\" value=\"n,25\" name=\"oth.place[]\" data-location=\"parts\">厄瓜多尔 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__69\"> <label for=\"_select_checkbox_parts_n_69\"><input id=\"_select_checkbox_parts_n_69\" data-locale-parts--n=\"69\" data-child-locale-parts--n=\"63\" type=\"checkbox\" value=\"n,69\" name=\"oth.place[]\" data-location=\"parts\">洪都拉斯 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__70\"> <label for=\"_select_checkbox_parts_n_70\"><input id=\"_select_checkbox_parts_n_70\" data-locale-parts--n=\"70\" data-child-locale-parts--n=\"63\" type=\"checkbox\" value=\"n,70\" name=\"oth.place[]\" data-location=\"parts\">尼加拉瓜 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__76\"> <label for=\"_select_checkbox_parts_n_76\"><input id=\"_select_checkbox_parts_n_76\" data-locale-parts--n=\"76\" data-child-locale-parts--n=\"63\" type=\"checkbox\" value=\"n,76\" name=\"oth.place[]\" data-location=\"parts\">巴拉圭 </label></div></td> </tr> <tr><td class=\"categoryColor\" rowspan=\"21\"> <span id=\"_select_parts_r__64\">西欧</span></td><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__28\"> <label for=\"_select_checkbox_parts_n_28\"><input id=\"_select_checkbox_parts_n_28\" data-locale-parts--n=\"28\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,28\" name=\"oth.place[]\" data-location=\"parts\">德国 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__29\"> <label for=\"_select_checkbox_parts_n_29\"><input id=\"_select_checkbox_parts_n_29\" data-locale-parts--n=\"29\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,29\" name=\"oth.place[]\" data-location=\"parts\">法国 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__30\"> <label for=\"_select_checkbox_parts_n_30\"><input id=\"_select_checkbox_parts_n_30\" data-locale-parts--n=\"30\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,30\" name=\"oth.place[]\" data-location=\"parts\">西班牙 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__31\"> <label for=\"_select_checkbox_parts_n_31\"><input id=\"_select_checkbox_parts_n_31\" data-locale-parts--n=\"31\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,31\" name=\"oth.place[]\" data-location=\"parts\">意大利 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__32\"> <label for=\"_select_checkbox_parts_n_32\"><input id=\"_select_checkbox_parts_n_32\" data-locale-parts--n=\"32\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,32\" name=\"oth.place[]\" data-location=\"parts\">葡萄牙 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__33\"> <label for=\"_select_checkbox_parts_n_33\"><input id=\"_select_checkbox_parts_n_33\" data-locale-parts--n=\"33\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,33\" name=\"oth.place[]\" data-location=\"parts\">英国 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__34\"> <label for=\"_select_checkbox_parts_n_34\"><input id=\"_select_checkbox_parts_n_34\" data-locale-parts--n=\"34\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,34\" name=\"oth.place[]\" data-location=\"parts\">爱尔兰 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__35\"> <label for=\"_select_checkbox_parts_n_35\"><input id=\"_select_checkbox_parts_n_35\" data-locale-parts--n=\"35\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,35\" name=\"oth.place[]\" data-location=\"parts\">比利时 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__36\"> <label for=\"_select_checkbox_parts_n_36\"><input id=\"_select_checkbox_parts_n_36\" data-locale-parts--n=\"36\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,36\" name=\"oth.place[]\" data-location=\"parts\">荷兰 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__37\"> <label for=\"_select_checkbox_parts_n_37\"><input id=\"_select_checkbox_parts_n_37\" data-locale-parts--n=\"37\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,37\" name=\"oth.place[]\" data-location=\"parts\">奥地利 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__38\"> <label for=\"_select_checkbox_parts_n_38\"><input id=\"_select_checkbox_parts_n_38\" data-locale-parts--n=\"38\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,38\" name=\"oth.place[]\" data-location=\"parts\">瑞士 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__39\"> <label for=\"_select_checkbox_parts_n_39\"><input id=\"_select_checkbox_parts_n_39\" data-locale-parts--n=\"39\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,39\" name=\"oth.place[]\" data-location=\"parts\">卢森堡 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__40\"> <label for=\"_select_checkbox_parts_n_40\"><input id=\"_select_checkbox_parts_n_40\" data-locale-parts--n=\"40\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,40\" name=\"oth.place[]\" data-location=\"parts\">瑞典 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__41\"> <label for=\"_select_checkbox_parts_n_41\"><input id=\"_select_checkbox_parts_n_41\" data-locale-parts--n=\"41\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,41\" name=\"oth.place[]\" data-location=\"parts\">挪威 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__42\"> <label for=\"_select_checkbox_parts_n_42\"><input id=\"_select_checkbox_parts_n_42\" data-locale-parts--n=\"42\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,42\" name=\"oth.place[]\" data-location=\"parts\">芬兰 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__43\"> <label for=\"_select_checkbox_parts_n_43\"><input id=\"_select_checkbox_parts_n_43\" data-locale-parts--n=\"43\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,43\" name=\"oth.place[]\" data-location=\"parts\">丹麦 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__44\"> <label for=\"_select_checkbox_parts_n_44\"><input id=\"_select_checkbox_parts_n_44\" data-locale-parts--n=\"44\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,44\" name=\"oth.place[]\" data-location=\"parts\">希腊 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__45\"> <label for=\"_select_checkbox_parts_n_45\"><input id=\"_select_checkbox_parts_n_45\" data-locale-parts--n=\"45\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,45\" name=\"oth.place[]\" data-location=\"parts\">马其顿 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__46\"> <label for=\"_select_checkbox_parts_n_46\"><input id=\"_select_checkbox_parts_n_46\" data-locale-parts--n=\"46\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,46\" name=\"oth.place[]\" data-location=\"parts\">马耳他 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__47\"> <label for=\"_select_checkbox_parts_n_47\"><input id=\"_select_checkbox_parts_n_47\" data-locale-parts--n=\"47\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,47\" name=\"oth.place[]\" data-location=\"parts\">摩洛哥 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__72\"> <label for=\"_select_checkbox_parts_n_72\"><input id=\"_select_checkbox_parts_n_72\" data-locale-parts--n=\"72\" data-child-locale-parts--n=\"64\" type=\"checkbox\" value=\"n,72\" name=\"oth.place[]\" data-location=\"parts\">列支敦士登 </label></div></td> </tr> <tr><td class=\"categoryColor\" rowspan=\"16\"> <span id=\"_select_parts_r__65\">东欧/俄罗斯CIS</span></td><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__57\"> <label for=\"_select_checkbox_parts_n_57\"><input id=\"_select_checkbox_parts_n_57\" data-locale-parts--n=\"57\" data-child-locale-parts--n=\"65\" type=\"checkbox\" value=\"n,57\" name=\"oth.place[]\" data-location=\"parts\">俄罗斯 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__48\"> <label for=\"_select_checkbox_parts_n_48\"><input id=\"_select_checkbox_parts_n_48\" data-locale-parts--n=\"48\" data-child-locale-parts--n=\"65\" type=\"checkbox\" value=\"n,48\" name=\"oth.place[]\" data-location=\"parts\">波兰 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__49\"> <label for=\"_select_checkbox_parts_n_49\"><input id=\"_select_checkbox_parts_n_49\" data-locale-parts--n=\"49\" data-child-locale-parts--n=\"65\" type=\"checkbox\" value=\"n,49\" name=\"oth.place[]\" data-location=\"parts\">捷克 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__50\"> <label for=\"_select_checkbox_parts_n_50\"><input id=\"_select_checkbox_parts_n_50\" data-locale-parts--n=\"50\" data-child-locale-parts--n=\"65\" type=\"checkbox\" value=\"n,50\" name=\"oth.place[]\" data-location=\"parts\">斯洛伐克 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__53\"> <label for=\"_select_checkbox_parts_n_53\"><input id=\"_select_checkbox_parts_n_53\" data-locale-parts--n=\"53\" data-child-locale-parts--n=\"65\" type=\"checkbox\" value=\"n,53\" name=\"oth.place[]\" data-location=\"parts\">匈牙利 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__51\"> <label for=\"_select_checkbox_parts_n_51\"><input id=\"_select_checkbox_parts_n_51\" data-locale-parts--n=\"51\" data-child-locale-parts--n=\"65\" type=\"checkbox\" value=\"n,51\" name=\"oth.place[]\" data-location=\"parts\">罗马尼亚 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__56\"> <label for=\"_select_checkbox_parts_n_56\"><input id=\"_select_checkbox_parts_n_56\" data-locale-parts--n=\"56\" data-child-locale-parts--n=\"65\" type=\"checkbox\" value=\"n,56\" name=\"oth.place[]\" data-location=\"parts\">克罗地亚 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__55\"> <label for=\"_select_checkbox_parts_n_55\"><input id=\"_select_checkbox_parts_n_55\" data-locale-parts--n=\"55\" data-child-locale-parts--n=\"65\" type=\"checkbox\" value=\"n,55\" name=\"oth.place[]\" data-location=\"parts\">斯洛文尼亚 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__52\"> <label for=\"_select_checkbox_parts_n_52\"><input id=\"_select_checkbox_parts_n_52\" data-locale-parts--n=\"52\" data-child-locale-parts--n=\"65\" type=\"checkbox\" value=\"n,52\" name=\"oth.place[]\" data-location=\"parts\">保加利亚 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__54\"> <label for=\"_select_checkbox_parts_n_54\"><input id=\"_select_checkbox_parts_n_54\" data-locale-parts--n=\"54\" data-child-locale-parts--n=\"65\" type=\"checkbox\" value=\"n,54\" name=\"oth.place[]\" data-location=\"parts\">塞尔维亚 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__58\"> <label for=\"_select_checkbox_parts_n_58\"><input id=\"_select_checkbox_parts_n_58\" data-locale-parts--n=\"58\" data-child-locale-parts--n=\"65\" type=\"checkbox\" value=\"n,58\" name=\"oth.place[]\" data-location=\"parts\">乌克兰 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__68\"> <label for=\"_select_checkbox_parts_n_68\"><input id=\"_select_checkbox_parts_n_68\" data-locale-parts--n=\"68\" data-child-locale-parts--n=\"65\" type=\"checkbox\" value=\"n,68\" name=\"oth.place[]\" data-location=\"parts\">乌兹别克斯坦 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__73\"> <label for=\"_select_checkbox_parts_n_73\"><input id=\"_select_checkbox_parts_n_73\" data-locale-parts--n=\"73\" data-child-locale-parts--n=\"65\" type=\"checkbox\" value=\"n,73\" name=\"oth.place[]\" data-location=\"parts\">爱沙尼亚 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__74\"> <label for=\"_select_checkbox_parts_n_74\"><input id=\"_select_checkbox_parts_n_74\" data-locale-parts--n=\"74\" data-child-locale-parts--n=\"65\" type=\"checkbox\" value=\"n,74\" name=\"oth.place[]\" data-location=\"parts\">立陶宛 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__75\"> <label for=\"_select_checkbox_parts_n_75\"><input id=\"_select_checkbox_parts_n_75\" data-locale-parts--n=\"75\" data-child-locale-parts--n=\"65\" type=\"checkbox\" value=\"n,75\" name=\"oth.place[]\" data-location=\"parts\">黑山 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__77\"> <label for=\"_select_checkbox_parts_n_77\"><input id=\"_select_checkbox_parts_n_77\" data-locale-parts--n=\"77\" data-child-locale-parts--n=\"65\" type=\"checkbox\" value=\"n,77\" name=\"oth.place[]\" data-location=\"parts\">波斯尼亚-黑塞哥维那 </label></div></td> </tr> <tr><td class=\"categoryColor\" rowspan=\"5\"> <span id=\"_select_parts_r__66\">中东</span></td><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__59\"> <label for=\"_select_checkbox_parts_n_59\"><input id=\"_select_checkbox_parts_n_59\" data-locale-parts--n=\"59\" data-child-locale-parts--n=\"66\" type=\"checkbox\" value=\"n,59\" name=\"oth.place[]\" data-location=\"parts\">土耳其 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__60\"> <label for=\"_select_checkbox_parts_n_60\"><input id=\"_select_checkbox_parts_n_60\" data-locale-parts--n=\"60\" data-child-locale-parts--n=\"66\" type=\"checkbox\" value=\"n,60\" name=\"oth.place[]\" data-location=\"parts\">伊朗 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__63\"> <label for=\"_select_checkbox_parts_n_63\"><input id=\"_select_checkbox_parts_n_63\" data-locale-parts--n=\"63\" data-child-locale-parts--n=\"66\" type=\"checkbox\" value=\"n,63\" name=\"oth.place[]\" data-location=\"parts\">以色列 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__61\"> <label for=\"_select_checkbox_parts_n_61\"><input id=\"_select_checkbox_parts_n_61\" data-locale-parts--n=\"61\" data-child-locale-parts--n=\"66\" type=\"checkbox\" value=\"n,61\" name=\"oth.place[]\" data-location=\"parts\">沙特阿拉伯 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__62\"> <label for=\"_select_checkbox_parts_n_62\"><input id=\"_select_checkbox_parts_n_62\" data-locale-parts--n=\"62\" data-child-locale-parts--n=\"66\" type=\"checkbox\" value=\"n,62\" name=\"oth.place[]\" data-location=\"parts\">阿拉伯酋长国 </label></div></td> </tr> <tr><td class=\"categoryColor\" rowspan=\"4\"> <span id=\"_select_parts_r__67\">非洲</span></td><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__65\"> <label for=\"_select_checkbox_parts_n_65\"><input id=\"_select_checkbox_parts_n_65\" data-locale-parts--n=\"65\" data-child-locale-parts--n=\"67\" type=\"checkbox\" value=\"n,65\" name=\"oth.place[]\" data-location=\"parts\">埃及 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__64\"> <label for=\"_select_checkbox_parts_n_64\"><input id=\"_select_checkbox_parts_n_64\" data-locale-parts--n=\"64\" data-child-locale-parts--n=\"67\" type=\"checkbox\" value=\"n,64\" name=\"oth.place[]\" data-location=\"parts\">南非 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__66\"> <label for=\"_select_checkbox_parts_n_66\"><input id=\"_select_checkbox_parts_n_66\" data-locale-parts--n=\"66\" data-child-locale-parts--n=\"67\" type=\"checkbox\" value=\"n,66\" name=\"oth.place[]\" data-location=\"parts\">突尼斯 </label></div></td> </tr> <tr><td class=\"topColor\" rowspan=\"1\"> <div class=\"checkbox\" id=\"_select_parts_n__67\"> <label for=\"_select_checkbox_parts_n_67\"><input id=\"_select_checkbox_parts_n_67\" data-locale-parts--n=\"67\" data-child-locale-parts--n=\"67\" type=\"checkbox\" value=\"n,67\" name=\"oth.place[]\" data-location=\"parts\">摩洛哥 </label></div></td> </tr> </tbody></table>";
			Pattern tr = Pattern.compile("<tr.*?>(.*?)</tr>");
			Matcher m = tr.matcher(cnt);
			while (m.find()) {
				int rowNum = m.groupCount();
				Pattern ch = Pattern.compile(".*type=\"checkbox\" value=\"(.*?)\"");
				String row = m.group(1);
				Matcher td = ch.matcher(row);
				String code = "";
				while (td.find()) {
					code = td.group(1);
				}
				if (!code.equals("")) {
					cfg = new urlCfg();
					cfg.url = "https://www.marklines.com/" + langType + "/supplier_db/partDetail"
							+ "?page=0&size=10&tb=top&_is=true&containsSub=true&isPartial=false&oth.place%5B%5D="
							+ code.replace(",", "%2C") + "&oth.isPartial=true";
					cfg.regUrl = Pattern.compile("(.*/" + langType + "/top\\d+/\\w\\d+_\\d+)|(.*/" + langType
							+ "/supplier_db/partDetail\\?page=\\d.*)");
					cfg.subregUrl = Pattern.compile("(.*/" + langType + "/supplier_db/partDetail\\?page=\\d.*)");
					add(cfg);
				}
			}
		}
		// 真实列表页面
		// https://www.marklines.com/"+langTyep+"/supplier_db/partDetail/top?_is=true&containsSub=true&isPartial=false&oth.place[]=a,71&oth.isPartial=true&page=1&size=10
		// 简介地址
		// https://www.marklines.com//"+langTyep+"/supplier_db/000000000019882/?no_frame=true&_is=true&isPartial=false&
		// 详细报告
		// https://www.marklines.com/"+langTyep+"/top500/s500_018
		// https://www.marklines.com/"+langTyep+"/top500/s500_515
		LogUtils.info("================================================================\n" + pages);
	}

	public static String getStartKey(int index, int step) {
		if (step == 2) {
			return "https://www.marklines.com/" + langType + "/supplier_db/partDetail/top?_is=trud";
		} else if (step == 3) {
			return "https://www.marklines.com/" + langType + "/supplier_db/00";
		} else {
			return "https://www.marklines.com/" + langType + "/top500/";
		}
	}

	public static String getEndKey(int index, int step) {
		if (step == 2) {
			return "https://www.marklines.com/" + langType + "/supplier_db/partDetail/top?_is=truf";
		} else if (step == 3) {
			return "https://www.marklines.com/" + langType + "/supplier_db/N9";
		} else {
			return "https://www.marklines.com/" + langType + "/top500/z";
		}
	}

	public static void setCookies(DefaultHttpClient httpClient) {
		CookieStore cookieStore = httpClient.getCookieStore();
		cookieStore.clear();
		BasicClientCookie cook = new BasicClientCookie(
				"PLATFORM_SESSION",
				"52c26906672fae93c7bfd70ba365bc5042d381e8-csrfToken=be8ae01d0c837b427d5a51418c4f03f867502233-1464318293929-0a21582adebfff7361de455e&_lsi=1499739&_lsh=49580faaa648f9aa6f03de9afc13228af3b8178e");
		cookieStore.addCookie(cook);
		cook.setDomain("www.marklines.com");
		cook.setPath("/");
		cook.setExpiryDate(new Date(2019, 5, 30, 8, 17, 11));
		cook = new BasicClientCookie("PLAY_LANG", "" + langType + "");
		cook.setDomain("www.marklines.com");
		cook.setPath("/");
		cookieStore.addCookie(cook);
		httpClient.setCookieStore(cookieStore);
	}

	public static boolean next() {
		return !pages.isEmpty();
	}

	public static WebPage get() {
		cfg = pages.pollLast();
		page = new WebPage();
		page.setBaseUrl(new Utf8(cfg.url));
		return page;
	}

	public static void readd() {
		pages.push(cfg);
	}

	public static Pattern getUrlReg() {
		return cfg.regUrl;
	}

	public static Pattern getUrlNameReg() {
		return cfg.regUrlName;
	}

	// url
	public static String getKey() throws MalformedURLException {
		return TableUtil.reverseUrl(cfg.url);
	}

	public static String getStartKey(int index) {
		return groupCfg.get(index).startKey;
	}

	public static String getEndKey(int index) {
		return groupCfg.get(index).endKey;
	}

}
