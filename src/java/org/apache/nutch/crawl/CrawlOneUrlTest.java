package org.apache.nutch.crawl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.util.Utf8;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.ParserJob;
import org.apache.nutch.parse.element.SegMentParsers;
import org.apache.nutch.parse.element.SegParserReducer;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.Protocol;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.ProtocolOutput;
import org.apache.nutch.protocol.ProtocolStatusCodes;
import org.apache.nutch.protocol.ProtocolStatusUtils;
import org.apache.nutch.protocol.httpclient.HttpComponent;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.ProtocolStatus;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;
import org.apache.nutch.urlfilter.SubURLFilters;
import org.apache.nutch.urlfilter.UrlPathMatch;
import org.apache.nutch.urlfilter.UrlPathMatch.UrlNodeConfig;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrawlOneUrlTest {
	public static final Logger LOG = LoggerFactory.getLogger(CrawlOneUrlTest.class);
	static WebPage page;
	static String url;
	private static ParseUtil parseUtil;
	private static boolean skipTruncated;
	static URLFilters filters = null;
	static URLNormalizers normalizers = null;
	static FetchSchedule schedule;
	static ScoringFilters scoringFilters;
	static long rowCount = 0;
	static long scanRowCount = 0;
	static String batchID;
	static String batchTime;
	static String reprUrl;
	static String key;

	/**
	 * @param args
	 * @throws Exception
	 * @throws NumberFormatException
	 */
	public static void main(String[] args) throws NumberFormatException, Exception {

		// System.err.println(Pattern.compile("(?i)http://www.scjst.gov.cn/WebSite/main/pageDetail\\.aspx\\?fid=.*&fcol=\\d+")
		// .matcher("http://www.scjst.gov.cn/WebSite/main/pagedetail.aspx?fid=ee453d4d-9ef6-4f49-81ec-e6e29e2abfa4&fcol=160007")
		// .matches());
		// System.out.println(System.currentTimeMillis() + "");
		// if (System.currentTimeMillis() > 0)
		// return;
		// // TODO Auto-generated method stub
		// // Pattern macroPattern = Pattern.compile("\\{\\w+:?\\w+\\}");
		// // System.err.println(macroPattern.matcher("{name}").find());
		// // System.err.println(macroPattern.matcher("{cf:mame}").find());
		// // System.err.println(macroPattern.matcher("{f1:a}").find());
		// // System.err.println(macroPattern.matcher("{1:1}").find());
		// // ZooKeeperServer zk = new ZooKeeperServer();
		// //
		// // System.out.println(zk.connect("hadoop01:2181,hadoop02:2181,hadoop03:2181"));
		// // byte[] data = zk.getData("/storm/supervisors/b057cd5a-4fa6-4f96-abae-caf7620b7d0b");
		// // System.out.println(zk.getString("/storm/supervisorsb057cd5a-4fa6-4f96-abae-caf7620b7d0b"));
		// //
		// Map<String, Object> env = new HashMap<String, Object>();
		// env.put("EXP", "{a}>=1");
		// env.put("DURATION", 1);
		// env.put("TYPE", "call_cds");
		// env.put("ORI_CHARGE", 300);
		// System.out.println("sdadgd_{asgfdas}_ass{saf}_s".replaceAll("\\{|\\}", ""));
		// AviatorEvaluator.setOptimize(AviatorEvaluator.EVAL);
		// // Expression exp = AviatorEvaluator.compile("email=~/([\\w0-8]+)@\\w+[\\.\\w+]+/ ? $1:'unknow'");
		// Expression exp = AviatorEvaluator.compile("string.substring('adsbgsdd',0,2)+TYPE+ORI_CHARGE+'_'+ORI_CHARGE+'-'+ORI_CHARGE");
		// System.out.println(exp.execute(env));
		// System.out.println(exp.toString());
		// long lt = System.currentTimeMillis();
		// if (lt > 0)
		// return;
		// env.put("email", "hans@tydic.com");
		// for (int i = 0; i < 1000000; i++) {
		// // String result = (String) AviatorEvaluator.execute("a>0? 'yes':'no'", env);
		// String name = (String) exp.execute(env);
		// // AviatorEvaluator.execute("1000+100.0*99-(600-3*15)/(((68-9)-3)*2-100)+10000%7*71");
		// // String username = (String) AviatorEvaluator.execute("email=~/([\\w0-8]+)@\\w+[\\.\\w+]+/ ? $1:'unknow'", env);
		// if (i % 100000 == 0)
		// System.out.println((System.currentTimeMillis() - lt) + " ms " + i + "  name:" + name);
		// }
		// System.out.println((System.currentTimeMillis() - lt) + " ms");
		// if (env != null)
		// return;
		// lt = System.currentTimeMillis();
		// for (int i = 0; i < 1000000; i++) {
		// Pattern pt = Pattern.compile("([\\w0-8]+)@\\w+[\\.\\w+]+");
		// Matcher m = pt.matcher("hans@tydic.com");
		// String name = "";
		// if (m.find()) {
		// m.group(1);
		// }
		// // String result = (String) AviatorEvaluator.execute("a>0? 'yes':'no'", env);
		// // exp.execute(env);
		// // AviatorEvaluator.execute("1000+100.0*99-(600-3*15)/(((68-9)-3)*2-100)+10000%7*71");
		// // String username = (String) AviatorEvaluator.execute("email=~/([\\w0-8]+)@\\w+[\\.\\w+]+/ ? $1:'unknow'", env);
		// if (i % 10000 == 0) {
		// System.out.println((System.currentTimeMillis() - lt) + " ms " + i + "  name:" + name);
		// }
		// }
		// //
		// if (env != null)
		// return;
		Configuration conf = NutchConfiguration.create();
		conf.set(Nutch.CRAWL_ID_KEY, "mopt");
		//
		// HBaseAdmin admin = new HBaseAdmin(conf);
		// List<HRegionInfo> regs = admin.getTableRegions("ea_webpage".getBytes());
		// int regIndex = 0;
		// HTable metaTable = new HTable(conf, HConstants.META_TABLE_NAME);
		// Scan scan = new Scan();
		// scan.addFamily("info".getBytes());
		// scan.setStartRow("ea_webpage".getBytes());
		// scan.setStopRow("ea_webpagf".getBytes());
		// ResultScanner rc = metaTable.getScanner(scan);
		// org.apache.hadoop.hbase.client.Result rv = null;
		// while ((rv = rc.next()) != null) {
		// String row = new String(rv.getRow());
		// KeyValue kv = rv.getColumn("info".getBytes(), "regioninfo".getBytes()).get(0);
		// String regioninfo = new String(kv.getValue());
		// System.out.println("row=" + row + "  regioninfo:" + regioninfo);
		// }
		// rc.close();
		// if (rc != null)
		// return;
		// HRegionInfo begion = new HRegionInfo(TableName.valueOf("ea_webpage"), null, regs.get(0).getEndKey());
		// Path rootdir = FSUtils.getRootDir(conf);
		//
		// for (HRegionInfo reg : regs) {
		// if (regIndex == 0) {
		// // reg.
		// }
		// System.out.println("getRegionNameAsString=" + reg.getRegionNameAsString());
		// System.out.println(new String(reg.getStartKey()) + "   ==>  " + new String(reg.getEndKey()));
		//
		// Path regioninfofile = new Path(rootdir.getName() + "/ea_webpage/" + reg.getEncodedName() + "/.regioninfo");
		// FileSystem fs = FileSystem.get(conf);
		// fs.delete(regioninfofile, true);
		// DataOutputStream out = fs.create(regioninfofile);
		// reg.write(out);
		// out.close();
		// }
		//
		// if (regs != null)
		// return;
		String betchId = "";
		DataStore<String, WebPage> store = StorageUtils.createWebStore(conf, String.class, WebPage.class);
		if (store == null)
			throw new RuntimeException("Could not create datastore");
		Query<String, WebPage> query = store.newQuery();
		if ((query instanceof Configurable)) {
			((Configurable) query).setConf(conf);
		}

		// List<PartitionQuery<String, WebPage>> parts = store.getPartitions(query);
		// for (PartitionQuery<String, WebPage> part : parts) {
		// System.out.println(part.getStartKey() + "   ==>  " + part.getEndKey());
		// System.out.println("getKey=" + part.getKey());
		// }
		Collection<WebPage.Field> fields = Arrays.asList(WebPage.Field.values());
		query.setFields(StorageUtils.toStringArray(fields));
		String startUrlKey = "http://www.sccin.com.cn/InvestmentInfo/";// http://weibo.com/u/
		String endUrlKey = "http://www.sccin.com.cn/InvestmentInfu";// startUrlKey;// "http://tech.qq.com/a/20130824/004772.htm";
		if (startUrlKey != null && startUrlKey.equals("") == false)
			query.setStartKey(betchId + TableUtil.reverseUrl(startUrlKey));
		if (endUrlKey != null && endUrlKey.equals("") == false)
			query.setEndKey(betchId + TableUtil.reverseUrl(endUrlKey));
		NutchConstant.setUrlConfig(conf, 3);
		NutchConstant.setSegmentParseRules(conf);
		NutchConstant.getSegmentParseRules(conf);

		SegMentParsers parses = new SegMentParsers(conf);
		Result<String, WebPage> rs = query.execute();
		long curTime = System.currentTimeMillis();
		UrlPathMatch urlcfg = NutchConstant.getUrlConfig(conf);
		boolean filter = conf.getBoolean(GeneratorJob.GENERATOR_FILTER, true);
		boolean normalise = conf.getBoolean(GeneratorJob.GENERATOR_NORMALISE, true);
		long limit = conf.getLong(GeneratorJob.GENERATOR_TOP_N, Long.MAX_VALUE);
		if (limit < 5) {
			limit = Long.MAX_VALUE;
		}
		int retryMax = conf.getInt("db.fetch.retry.max", 3);

		limit = Integer.MAX_VALUE;
		if (filter) {
			filters = new URLFilters(conf);
		}
		if (normalise) {
			normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
		}
		urlFilters = new URLFilters(conf);
		curTime = conf.getLong(GeneratorJob.GENERATOR_CUR_TIME, System.currentTimeMillis());
		schedule = FetchScheduleFactory.getFetchSchedule(conf);
		scoringFilters = new ScoringFilters(conf);
		batchID = NutchConstant.getBatchId(conf);
		batchTime = "";
		ProtocolFactory protocolFactory = new ProtocolFactory(conf);
		skipTruncated = conf.getBoolean(ParserJob.SKIP_TRUNCATED, true);
		parseUtil = new ParseUtil(conf);
		subUrlFilter = new SubURLFilters(conf);
		System.err.println("批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
		int rowCount = 0;
		HttpComponent httpComponent = new HttpComponent();
		httpComponent.setConf(conf);
		long l = System.currentTimeMillis();
		// System.out.println(AviatorEvaluator.execute("\"2013-10-17 16:48:00\"").toString());
		// if (l > 0)
		// return;

		// try {
		// while (rs.next()) {
		// key = rs.getKey();
		// startUrlKey = url = TableUtil.unreverseUrl(key.substring(betchId.length()));
		// page = rs.get();
		// System.out.println();
		// System.out.println("Status:" + page.getStatus() + "key:" + key + "  rul:" + url);
		// System.out.println(page);
		// System.out.println(new String(page.getContent().array(), "utf-8"));
		// }
		// rs.close();
		// } catch (Exception exx) {
		// rs.close();
		// query.setStartKey(TableUtil.reverseUrl(startUrlKey));
		// rs = query.execute();
		// }
		//
		// if (rs != null)
		// return;
		while (true) {// rs.next()
			if (rowCount++ > 0) {
				break;
			}
			// page = rs.get();
			// key = rs.getKey();
			// url = TableUtil.unreverseUrl(key);

			url = "http://finance.qq.com/a/20120912/006325.htm";// 设置要抓取的URL
			url = "http://cs.house.qq.com/a/20121018/000078.htm";// 设置要抓取的URL
			// url = "http://db.house.qq.com/index.php?mod=search&city=xian";
			// url = "http://xian.house.qq.com/l/xbtf/fcxbtf.htm";
			// url = "http://xian.house.qq.com/l/northcity/fcsbdg.htm";
			// url = "http://xian.house.qq.com/";
			// url = "http://db.house.qq.com/xian";
			url = "http://db.house.qq.com/anshan_100856/";
			url = "http://yangguangxinyezhongxin.soufun.com/photo/list_900_3211050454_2.htm";

			url = "http://db.house.qq.com/cd_111828/";
			url = "http://db.house.qq.com/cd_111828/info.shtml";
			url = "http://db.house.qq.com/cd_111828/price/";
			url = "http://db.house.qq.com/cd_111828/photo/1_1/";
			url = "http://cd.house.qq.com/a/20130917/008297.htm";

			url = "http://nanwaitanjnw.soufun.com/";
			url = "http://newhouse.cd.soufun.com/2009-09-17/2792504_5.htm";
			// url = "http://nanwaitanjnw.soufun.com/house/3210869592/housedetail.htm";
			// url = "http://news.cd.soufun.com/2013-10-10/11188927.htm";
			// url = "http://nanwaitanjnw.soufun.com/house/3210869592/dongtai/1342701.htm";
			// url = "http://newhouse.cd.soufun.com/2013-10-16/11235788.htm";
			// url = "http://nanwaitanjnw.soufun.com/house/3210869592/fangjia.htm";

			url = "http://cd.ganji.com/fang1/tuiguang-31854953.htm";
			url = "http://cd.ganji.com/fang1/692785818x.htm";
			url = "http://cd.ganji.com/fang2/704252518x.htm";
			url = "http://cd.ganji.com/fang2/670041497x.htm";

			url = "http://wap.ganji.com/cd/fang1/651602770x?pos=8&url=fang1&agent=2&pageSize=10&page=1&tg=3";
			url = "http://wap.ganji.com/cd/fang1/704720564x?pos=8&url=fang1&pageSize=10&page=1&tg=1001";
			url = "http://wap.ganji.com/cd/fang3/695852655x?pos=12&url=fang3&pageSize=10&page=1&tg=1001";
			url = "http://wap.ganji.com/cd/fang5/696177626x?pos=1&url=fang5&pageSize=10&page=1&tg=3";
			url = "http://wap.ganji.com/cd/fang5/702030015x?pos=4&url=fang5&pageSize=10&page=1&tg=2.1";
			url = "http://wap.ganji.com/cd/fang2/583363262x?pos=10&url=fang2&pageSize=10&page=1&tg=1001";
			url = "http://wap.ganji.com/cd/fang2/704744779x?pos=1&url=fang2&pageSize=10&page=1&tg=1001";
			url = "http://wap.ganji.com/cd/fang4/704536458x?pos=2&url=fang4&pageSize=10&page=1&tg=1001";
			// url = "http://wap.ganji.com/cd/fang4/704436345x?pos=4&url=fang4&pageSize=10&page=1&tg=1001";
			url = "http://wap.ganji.com/cd/fang1/?domain=cd&url=fang1&page=1";
			url = "http://wap.ganji.com/cd/fang1/";

			url = "http://m.58.com/cd/zufang/";
			url = "http://i.m.58.com/cd/zufang/15538653692039x.shtml";
			url = "http://i.m.58.com/cd/zufang/15403127032966x.shtml";
			url = "http://m.58.com/cd/qiuzu/";
			url = "http://m.58.com/cd/qiuzu/pn2/";

			url = "http://i.m.58.com/cd/qiuzu/15691078326151x.shtml";
			url = "http://i.m.58.com/cd/qiuzu/15563372183690x.shtml";
			// url = "http://i.m.58.com/cd/qiuzu/15691649268353x.shtml";
			// url = "http://i.m.58.com/cd/qiuzu/15580564633477x.shtml";
			// url = "http://i.m.58.com/cd/qiuzu/15691792568835x.shtml";
			// url = "http://i.m.58.com/cd/qiuzu/15514728510981x.shtml";

			url = "http://m.58.com/cd/ershoufang/";
			url = "http://i.m.58.com/cd/ershoufang/15660173611521x.shtml";
			url = "http://i.m.58.com/cd/ershoufang/15692610703237x.shtml";
			url = "http://i.m.58.com/cd/ershoufang/15646523265417x.shtml";
			url = "http://m.58.com/local/cd";
			url = "http://i.m.58.com/cd/hezu/11632175277065x.shtml";
			url = "http://i.m.58.com/cd/hezu/15568727765129x.shtml";
			url = "http://zu.cd.soufun.com/chuzu/7_10907071_1.htm";
			url = "http://www.scjst.gov.cn/WebSite/main/pagedetail.aspx?fid=ee453d4d-9ef6-4f49-81ec-e6e29e2abfa4&fcol=160007";
			url = "http://www.scjst.gov.cn/WebSite/main/pagedetail.aspx?fid=bf4d5efa-025c-4ada-b9a6-8a4b53e16247&fcol=160001";
			url = "http://www.scjst.gov.cn/WebSite/main/pagedetail.aspx?fid=c79694ac-0312-4814-89d0-24457c2b01ee&fcol=137003001";
			url = "http://www.scjst.gov.cn/WebSite/main/";
			url = "http://www.scjst.gov.cn/WebSite/main/pageDetail.aspx?fid=12d77820-0d37-47fb-ad01-21aa4d0c97cb&fcol=160004";
			url = "http://www.sccin.com.cn/InvestmentInfo/ZhaoBiao/InviteNoticeDetail.aspx?id=112449";
			url = "http://www.sccin.com.cn/InvestmentInfo/ZhaoBiao/InviteNoticeDetail.aspx?id=112428";
			url = "http://www.sccin.com.cn/InvestmentInfo/ZhaoBiao/InviteNoticeDetail.aspx?id=112417";
			url = "http://www.sccin.com.cn/InvestmentInfo/ZhaoBiao/InviteNoticeDetail.aspx?id=112465";
			url = "http://www.sccin.com.cn/InvestmentInfo/ZhaoBiao/InviteNoticeDetail.aspx?id=112468";
			url = "http://www.sccin.com.cn/InvestmentInfo/ZhaoBiao/InviteNoticeDetail.aspx?id=112450";

			key = TableUtil.reverseUrl(url);
			page = new WebPage();
			page.setCrawlType(1);// 设置服务端抓取
			page.setFetchInterval(0);

			page.setContent(null);
			page.getOutlinks().clear();
			System.out.println("流程测试地址：" + url + " config:" + page.getConfigUrl());
			page.setFetchInterval(0);

			System.out.println("url:" + url + " key:" + key);
			System.err.println("///////////////////////////begin generator////////////////////////////////////////");
			Utf8 mark = Mark.BETCH_MARK.checkMark(page);
			if (mark != null) {
				String[] bm = mark.toString().split(":");
				if (bm.length == 2) {
					if (curTime - Long.parseLong(bm[1]) < DbConfigFetchSchedule.SECONDS_PER_DAY * 1000 && page.getFetchInterval() > 3600) {
						System.err.println("Skipping " + url + "; already generated");
						continue;
					}
				}
			}
			// String baseUrl = page.getBaseUrl().toString();
			UrlPathMatch urlSegment = urlcfg.match(url);// 获取配置项
			if (urlSegment == null) {
				// 不满足配置要求
				System.err.println("url:" + url + " is not in urlconfig rowkey:" + key);
				continue;
				// } else {
				// System.err.println("url:" + url + " is config in urlconfig rowCount=" + rowCount);
			}
			UrlNodeConfig value = (UrlNodeConfig) urlSegment.getNodeValue();
			String configUrl = urlSegment.getConfigPath();
			page.setConfigUrl(new Utf8(configUrl));
			String curl = NutchConstant.getUrlPath(url);
			System.out.println("url:" + url + "     configUrl:" + configUrl + " curl=" + curl);
			if (curl == null) {
				return;
			}
			String tmpCu = url.substring(0, url.length() - 1);
			System.out.println(" configUrl:" + configUrl + " " + tmpCu);
			System.out.println(configUrl.equals(tmpCu));
			if (configUrl.equals(url) || configUrl.equals(curl)
					|| (url.endsWith("/") && configUrl.equals(url.substring(0, url.length() - 1)))) {// 应用新设置的属性
				System.err.println("url:" + url + " is config url, set [sorce,FetchInterval]" + " to [" + value.customScore + ","
						+ value.customInterval + "]");
				page.setScore(value.customScore);
				page.setFetchInterval(value.customInterval);
				page.putToMarkers(DbUpdaterJob.DISTANCE, new Utf8(Integer.toString(1)));
			} else if (page.getFetchInterval() < value.subFetchInterval * 0.5) {
				if (value.subFilters != null) {
					if (SubURLFilters.filter(url, value.subFilters) == false) {
						GeneratorJob.LOG.info("规则：" + value.subFilters + " 过滤URL:" + url);
						return;
					}
				}
				if (GeneratorJob.LOG.isWarnEnabled())
					GeneratorJob.LOG.warn("url:" + url + " FetchInterval is too small, set to " + value.customInterval);
				page.setFetchInterval(value.subFetchInterval);
			}
			page.setCrawlType(value.crawlType);
			// filter on distance
			if (value.fetchDepth > -1) {
				Utf8 distanceUtf8 = page.getFromMarkers(DbUpdaterJob.DISTANCE);
				if (distanceUtf8 != null) {
					int distance = Integer.parseInt(distanceUtf8.toString());
					if (distance > value.fetchDepth) {
						GeneratorJob.LOG.warn("url:" + url + " depth:" + distance + " config fetchDepth:" + value.fetchDepth + " rowKey:"
								+ key);
						continue;
					}
				}
			}
			// If filtering is on don't generate URLs that don't pass URLFilters
			try {
				url = normalizers.normalize(url, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
				System.out.println("规整后的URL：" + url);
				// if (url != null)
				// return;
				// 允许配置文件中配置禁止抓取的地址
				if (filter && filters.filter(url) == null) {
					GeneratorJob.LOG.warn("filter url:" + url + " rowkey:" + key);
					continue;
				}
			} catch (URLFilterException e) {
				if (GeneratorJob.LOG.isWarnEnabled()) {
					GeneratorJob.LOG.warn("Couldn't filter url: " + url + " (" + e.getMessage() + ")");
					continue;
				}
			} catch (MalformedURLException e) {
				if (GeneratorJob.LOG.isWarnEnabled()) {
					GeneratorJob.LOG.warn("Couldn't filter url: " + url + " (" + e.getMessage() + ")");
					continue;
				}
			}
			page.nodeConfig = value;
			page.setFetchTime(0);
			page.setPrevFetchTime(0);
			// check fetch schedule
			if (!schedule.shouldFetch(url, page, curTime)) {
				if (GeneratorJob.LOG.isDebugEnabled()) {
					System.err.println("-shouldFetch rejected '" + url + "', fetchTime=" + page.getFetchTime() + ", curTime=" + curTime
							+ " FetchInterval=" + page.getFetchInterval());
				}
				continue;
			}
			// page.nodeConfig = null;
			float score = page.getScore();
			try {
				score = scoringFilters.generatorSortValue(url, page, score);
			} catch (ScoringFilterException e) {
				// ignore
			}
			System.out.println("write url:" + url + " rowkey:" + key);
			rowCount++;
			// key = NutchConstant.getWebPageUrl("1", key);
			// if (key == null)
			// continue;
			System.err.println("// /////////////////////////end generator////////////////////////////////////////");
			page.setCrawlType(1);

			System.err.println("// /////////////////////////begin fetch/////////////////////////////////////////");
			if (!page.isReadable(WebPage.Field.REPR_URL.getIndex())) {
				reprUrl = url;
			} else {
				reprUrl = TableUtil.toString(page.getReprUrl());
			}
			final Protocol protocol = protocolFactory.getProtocol(url);
			final ProtocolOutput output = protocol.getProtocolOutput(url, page);
			final ProtocolStatus status = output.getStatus();
			final Content content = output.getContent();

			int length = 0;
			if (content != null && content.getContent() != null)
				length = content.getContent().length;
			System.out.println("url:" + url + " fetch content length:" + length);

			switch (status.getCode()) {
			case ProtocolStatusCodes.WOULDBLOCK:
				// retry ?
				System.err.println("need to refetch ");
				continue;
			case ProtocolStatusCodes.SUCCESS: // got a page
				output(content, status, CrawlStatus.STATUS_FETCHED);
				break;
			case ProtocolStatusCodes.MOVED: // redirect
			case ProtocolStatusCodes.TEMP_MOVED:
				byte code;
				boolean temp;
				if (status.getCode() == ProtocolStatusCodes.MOVED) {
					code = CrawlStatus.STATUS_REDIR_PERM;
					temp = false;
				} else {
					code = CrawlStatus.STATUS_REDIR_TEMP;
					temp = true;
				}
				final String newUrl = ProtocolStatusUtils.getMessage(status);
				// System.err.println("need to refetch ");
				// return;
				handleRedirect(url, newUrl, temp, FetcherJob.PROTOCOL_REDIR, page);
				output(content, status, code);
				break;
			case ProtocolStatusCodes.EXCEPTION:
				LOG.warn("fetch of " + url + " failed with: " + ProtocolStatusUtils.getMessage(status));
				/* FALLTHROUGH */
			case ProtocolStatusCodes.RETRY: // retry
			case ProtocolStatusCodes.BLOCKED:
				output(null, status, CrawlStatus.STATUS_RETRY);
				break;

			case ProtocolStatusCodes.GONE: // gone
			case ProtocolStatusCodes.NOTFOUND:
			case ProtocolStatusCodes.ACCESS_DENIED:
			case ProtocolStatusCodes.ROBOTS_DENIED:
				LOG.warn("FetcherReducer 572: set URL:" + url + " fetchinterval from " + page.getFetchInterval() + " to "
						+ (page.getFetchInterval() * 5 + 30 * FetchSchedule.SECONDS_PER_DAY));
				page.setFetchInterval(page.getFetchInterval() * 5 + 30 * FetchSchedule.SECONDS_PER_DAY);
				output(null, status, CrawlStatus.STATUS_GONE);
				break;
			case ProtocolStatusCodes.NOTMODIFIED:
				output(null, status, CrawlStatus.STATUS_NOTMODIFIED);
				break;
			default:
				if (LOG.isWarnEnabled()) {
					LOG.warn("Unknown ProtocolStatus: " + status.getCode());
				}
				output(null, status, CrawlStatus.STATUS_RETRY);
			}
			System.err.println("// /////////////////////////end fetch////////////////////////////////////////");
			if (content == null) {
				System.err.println("url:" + url + " fetch content is null");
				continue;
			}
			System.err.println("// /////////////////////////begin parse////////////////////////////////////////");
			if (!skipTruncated || (skipTruncated && !ParserJob.isTruncated(url, page))) {
				parseUtil.process(key, page);
			}
			ParseStatus pstatus = page.getParseStatus();
			if (pstatus != null) {
				System.out.println(("ParserStatus:" + ParseStatusCodes.majorCodes[pstatus.getMajorCode()]));
			} else {
				System.out.println(("ParserStatus:" + "failed"));
				continue;
			}
			System.err.println("// /////////////////////////end parse////////////////////////////////////////");
			System.err.println("// /////////////////////////begin dbupdate////////////////////////////////////////");
			Map<Utf8, Utf8> outlinks = page.getOutlinks();
			if (outlinks != null) {
				int depth = 0;
				Utf8 depthUtf8 = page.getFromMarkers(DbUpdaterJob.DISTANCE);
				if (depthUtf8 != null)
					depth = Integer.parseInt(depthUtf8.toString());
				for (Entry<Utf8, Utf8> e : outlinks.entrySet()) {
					String olurl = e.getKey().toString();
					int spindex = olurl.indexOf(' ');
					if (spindex > 0) {
						olurl = olurl.substring(0, spindex);
					}
					spindex = olurl.indexOf('>');
					if (spindex > 0) {
						olurl = olurl.substring(0, spindex);
					}
					spindex = olurl.indexOf('<');
					if (spindex > 0) {
						olurl = olurl.substring(0, spindex);
					}
					olurl = normalizers.normalize(olurl, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);

					UrlPathMatch segment = NutchConstant.urlcfg.match(olurl);// 获取配置项
					if (segment == null) {
						System.err.println("未匹配到配置地址 过滤URL:" + olurl);
						page.removeFromOutlinks(e.getKey());
						continue;
					}
					UrlNodeConfig val = (UrlNodeConfig) segment.getNodeValue();
					if (val.regFormat != null) {
						olurl = val.regFormat.replace(olurl);
					}
					System.out.println("config:" + urlSegment.getConfigPath());
					if (!SubURLFilters.filter(olurl, value.subFilters)) {
						page.removeFromOutlinks(e.getKey());
						continue;
					}
					try {
						URI u = new URI(olurl);
						InetAddress.getByName(u.getHost());
					} catch (Exception ex) {
						ex.printStackTrace();
						LOG.warn("outlink:" + olurl + " author:" + e.getKey() + " error:" + ex.getMessage());
						page.removeFromOutlinks(e.getKey());
						continue;
					}

					WebPage subpage = new WebPage();
					schedule.initializeSchedule(url, subpage);
					subpage.putToMarkers(DbUpdaterJob.DISTANCE, new Utf8());
					subpage.setStatus(CrawlStatus.STATUS_UNFETCHED);
					try {
						scoringFilters.initialScore(url, subpage);
					} catch (ScoringFilterException ex) {
						System.out.println(ex.getMessage());
						ex.printStackTrace();
						subpage.setScore(1.0f);
					}
					subpage.putToMarkers(DbUpdaterJob.DISTANCE, new Utf8(Integer.toString(depth)));
					// store.put(key, subpage);//不真实写入数据库
					System.out.println("WebPage outlink==" + e.getValue() + " " + olurl);
				}
				System.out.println("outlinks size=" + outlinks.size());
			}
			// update self

			byte _status = (byte) page.getStatus();
			switch (_status) {
			case CrawlStatus.STATUS_FETCHED: // succesful fetch
			case CrawlStatus.STATUS_REDIR_TEMP: // successful fetch, redirected
			case CrawlStatus.STATUS_REDIR_PERM:
			case CrawlStatus.STATUS_NOTMODIFIED: // successful fetch, notmodified
				int modified = FetchSchedule.STATUS_UNKNOWN;
				if (_status == CrawlStatus.STATUS_NOTMODIFIED) {
					modified = FetchSchedule.STATUS_NOTMODIFIED;
				}
				ByteBuffer prevSig = page.getPrevSignature();
				ByteBuffer signature = page.getSignature();
				if (prevSig == null)
					page.setModifiedTime(System.currentTimeMillis());
				if (prevSig != null && signature != null) {
					if (SignatureComparator.compare(prevSig.array(), signature.array()) != 0) {
						modified = FetchSchedule.STATUS_MODIFIED;
						page.setModifiedTime(System.currentTimeMillis());
					} else {
						modified = FetchSchedule.STATUS_NOTMODIFIED;
					}
				}
				long fetchTime = page.getFetchTime();
				long prevFetchTime = page.getPrevFetchTime();
				long modifiedTime = page.getModifiedTime();
				schedule.setFetchSchedule(url, page, prevFetchTime, 0L, fetchTime, modifiedTime, modified);
				break;
			case CrawlStatus.STATUS_RETRY:
				schedule.setPageRetrySchedule(url, page, 0L, 0L, page.getFetchTime());
				if (page.getRetriesSinceFetch() < retryMax) {
					page.setStatus(CrawlStatus.STATUS_UNFETCHED);
				} else {
					page.setStatus(CrawlStatus.STATUS_GONE);
				}
				break;
			case CrawlStatus.STATUS_GONE:
				schedule.setPageGoneSchedule(url, page, 0L, 0L, page.getFetchTime());
				break;
			}
			if (pstatus == null || !ParseStatusUtils.isSuccess(pstatus)) {
				page.setFetchInterval(0);
				page.setFetchTime(0);
			}
			// store.put(key, page);//不真实写入数据库
			System.out.println("WebPage==" + page);
			System.err.println("// /////////////////////////end dbupdate////////////////////////////////////////");
			// System.out.println(new String(page.getContent().array()));
			System.out.println(new String(page.getContent().array(), "utf-8"));

			System.err.println("// /////////////////////////begin segmentparse////////////////////////////////////////");
			WebPageSegment wps = SegParserReducer.parseSegMent(parses, url, page);// 有解析成功的数据返回
			System.out.println("WebPageSegment==" + wps);
			System.err.println("// /////////////////////////end segmentparse////////////////////////////////////////");
			break;
		}
		// rs.close();
	}

	private static URLFilters urlFilters;
	static SubURLFilters subUrlFilter;

	private static void handleRedirect(String url, String newUrl, boolean temp, String redirType, WebPage page) throws URLFilterException,
			IOException, InterruptedException {
		newUrl = normalizers.normalize(newUrl, URLNormalizers.SCOPE_FETCHER);
		newUrl = urlFilters.filter(newUrl);
		if (newUrl == null || newUrl.equals(url)) {
			return;
		}
		page.putToOutlinks(new Utf8(newUrl), new Utf8());
		page.putToMetadata(FetcherJob.REDIRECT_DISCOVERED, TableUtil.YES_VAL);
		reprUrl = URLUtil.chooseRepr(reprUrl, newUrl, temp);
		if (reprUrl == null) {
			LOG.warn("url:" + url + " reprUrl==null");
		} else {
			page.setReprUrl(new Utf8(reprUrl));
			if (LOG.isDebugEnabled()) {
				LOG.debug("url:" + url + "  - " + redirType + " redirect to " + reprUrl + " (fetching later)");
			}
		}
	}

	private static void output(Content content, ProtocolStatus pstatus, byte status) throws IOException, InterruptedException {
		page.setStatus(status);
		// final long prevFetchTime = page.getFetchTime();
		long curTime = System.currentTimeMillis();
		page.setPrevFetchTime(curTime);
		page.setFetchTime(curTime);
		if (pstatus != null) {
			page.setProtocolStatus(pstatus);
		}
		if (content != null) {
			page.setContent(ByteBuffer.wrap(content.getContent()));
			page.setContentType(new Utf8(content.getContentType()));
			page.setBaseUrl(new Utf8(content.getBaseUrl()));
		}
		if (content != null) {
			Metadata data = content.getMetadata();
			String[] names = data.names();
			for (String name : names) {
				page.putToHeaders(new Utf8(name), new Utf8(data.get(name)));
			}
		}
		// remove content if storingContent is false. Content is added to page above
		// for ParseUtil be able to parse it.
		if (content == null) {
			page.setContent(ByteBuffer.wrap(new byte[0]));
		}
	}
}
