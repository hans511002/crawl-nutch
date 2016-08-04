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
import org.apache.nutch.urlfilter.ExpFilter;
import org.apache.nutch.urlfilter.SubURLFilters;
import org.apache.nutch.urlfilter.UrlPathMatch;
import org.apache.nutch.urlfilter.UrlPathMatch.UrlNodeConfig;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CrawlSingleClient {
	public static final Logger LOG = LoggerFactory.getLogger(CrawlSingleClient.class);
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
		String usage = "Usage: CrawlSingleClient [-crawlId <id>] [-topN N] [-cfgs <ids>]\n";
		if (args.length == 0) {
			System.err.println(usage);
			System.exit(-1);
			return;
		}
		Configuration conf = NutchConfiguration.create();
		long curTime = System.currentTimeMillis(), topN = Long.MAX_VALUE;
		for (int i = 0; i < args.length; i++) {
			if ("-topN".equals(args[i])) {
				topN = Long.parseLong(args[++i]);
			} else if ("-crawlId".equals(args[i])) {
				conf.set(Nutch.CRAWL_ID_KEY, args[++i]);
			} else if ("-cfgs".equals(args[i])) {
				conf.set(NutchConstant.configIdsKey, args[++i]);
			} else if ("-batch".equals(args[i])) {
				String batchId = org.apache.commons.lang.StringUtils.lowerCase(args[i + 1]);
				if (batchId != null && !batchId.equals("")) {
					conf.set(NutchConstant.BATCH_ID_KEY, batchId);
				}
				i++;
			}
		}

		DataStore<String, WebPage> store = StorageUtils.createWebStore(conf, String.class, WebPage.class);
		if (store == null)
			throw new RuntimeException("Could not create datastore");
		Query<String, WebPage> query = store.newQuery();
		if ((query instanceof Configurable)) {
			((Configurable) query).setConf(conf);
		}
		Collection<WebPage.Field> fields = Arrays.asList(WebPage.Field.values());
		query.setFields(StorageUtils.toStringArray(fields));
		String startUrlKey = "http://cd.58.com/";// http://weibo.com/u/
		String endUrlKey = "";// startUrlKey;// "http://tech.qq.com/a/20130824/004772.htm";
		if (startUrlKey != null && startUrlKey.equals("") == false)
			query.setStartKey(TableUtil.reverseUrl(startUrlKey));
		if (endUrlKey != null && endUrlKey.equals("") == false)
			query.setEndKey(TableUtil.reverseUrl(endUrlKey));
		NutchConstant.setUrlConfig(conf, 3);
		NutchConstant.setSegmentParseRules(conf);
		NutchConstant.getSegmentParseRules(conf);

		SegMentParsers parses = new SegMentParsers(conf);
		Result<String, WebPage> rs = query.execute();
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
		// while (true) {
		// try {
		// while (rs.next()) {
		// key = rs.getKey();
		// startUrlKey = url = TableUtil.unreverseUrl(key);
		// if (key.endsWith("/") == false)
		// continue;
		// Response response = null;
		// rowCount++;
		// try {
		// l = System.currentTimeMillis();
		// response = new HttpComponentResponse(httpComponent, new URL(url), page, true);
		// int code = response.getCode();
		// System.out.println((new Date().toLocaleString()) + " num:" + rowCount + " code:" + code + " time:"
		// + (System.currentTimeMillis() - l) + "  url:" + url);
		// l = System.currentTimeMillis();
		// } catch (Exception e) {
		// e.printStackTrace(System.out);
		// }
		//
		// }
		// rs.close();
		// break;
		// } catch (Exception exx) {
		// rs.close();
		// query.setStartKey(TableUtil.reverseUrl(startUrlKey));
		// rs = query.execute();
		// }
		// }
		//
		// if (rs != null)
		// return;
		while (rs.next()) {// rs.next()
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
			url = "http://cd.58.com/zufang/14345683634435x.shtml";
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
			if (curl == null) {
				GeneratorJob.LOG.error("非法url:" + url);
				return;
			}
			if (configUrl.equals(url) || configUrl.equals(curl)) {// 应用新设置的属性
				System.err.println("url:" + url + " is config url, set [sorce,FetchInterval]" + " to [" + value.customScore + ","
						+ value.customInterval + "]");
				page.setScore(value.customScore);
				page.setFetchInterval(value.customInterval);
				page.putToMarkers(DbUpdaterJob.DISTANCE, new Utf8(Integer.toString(1)));
			} else if (page.getFetchInterval() < value.subFetchInterval * 0.5) {
				if (value.subFilters != null) {
					for (ExpFilter fter : value.subFilters) {
						if (!fter.filter(url)) {
							GeneratorJob.LOG.info("规则：" + fter + " 过滤URL:" + url);
							return;
						}
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
					if (!checkUrl(olurl, e.getValue().toString())) {
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
			// System.out.println(new String(page.getContent().array(), "utf-8"));

			System.err.println("// /////////////////////////begin segmentparse////////////////////////////////////////");
			WebPageSegment wps = SegParserReducer.parseSegMent(parses, url, page);// 有解析成功的数据返回
			System.out.println("WebPageSegment==" + wps);
			System.err.println("// /////////////////////////end segmentparse////////////////////////////////////////");
		}
		rs.close();
	}

	private static URLFilters urlFilters;
	static SubURLFilters subUrlFilter;

	public static boolean checkUrl(String olurl, String name) {
		try {
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
			URI u = new URI(olurl);
			if (!subUrlFilter.filter(olurl))
				return false;
			InetAddress address = InetAddress.getByName(u.getHost());
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			LOG.warn("outlink:" + olurl + " author:" + name + " error:" + ex.getMessage());
			return false;
		}
	}

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
