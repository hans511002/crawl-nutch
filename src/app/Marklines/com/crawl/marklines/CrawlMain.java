package com.crawl.marklines;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.avro.util.Utf8;
import org.apache.gora.mongo.store.MongoStore;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.CookieStore;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.DbUpdaterJob;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.crawl.FetchScheduleFactory;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.crawl.SignatureComparator;
import org.apache.nutch.fetcher.FetcherJob;
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
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.ProtocolStatus;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;
import org.apache.nutch.urlfilter.SubURLFilters;
import org.apache.nutch.urlfilter.UrlPathMatch;
import org.apache.nutch.urlfilter.UrlPathMatch.UrlNodeConfig;
import org.apache.nutch.util.HasThread;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TableUtil;

import com.crawl.App;
import com.mongodb.BasicDBObject;
import com.sobey.jcg.support.log4j.LogUtils;
import com.sobey.jcg.support.utils.Utils;

public class CrawlMain extends App {

	static String langType = "cn";

	public void setCookies(DefaultHttpClient httpClient) {
		CookieStore cookieStore = httpClient.getCookieStore();
		cookieStore.clear();
		BasicClientCookie cook = new BasicClientCookie(
				"PLATFORM_SESSION",
				"69e2f6b07b1c415cf9306b678fa50b65a55714d8-csrfToken=be8ae01d0c837b427d5a51418c4f03f867502233-1464318293929-0a21582adebfff7361de455e&_lsi=1500061&_lsh=bb870bbf4425dda0136239d73471d32b7cd93ac4");
		cookieStore.addCookie(cook);
		cook.setDomain("www.marklines.com");
		cook.setPath("/");
		// cook.setExpiryDate(new Date(2019, 5, 30, 8, 17, 11));
		cook = new BasicClientCookie("PLAY_LANG", langType);
		cook.setDomain("www.marklines.com");
		cook.setPath("/");
		cookieStore.addCookie(cook);
		httpClient.setCookieStore(cookieStore);
	}

	static boolean stoped = false;
	static int activeThread = 0;

	public static void main(String[] args) throws Exception {
		System.out.println(System.currentTimeMillis() - 7200000);

		WebPage page = null;
		String url;
		CrawlMain app = new CrawlMain();
		Configuration conf = NutchConfiguration.create();
		schedule = FetchScheduleFactory.getFetchSchedule(conf);
		final int index = 4;
		int step = 3;// 1 为抓取搜索入口 2为解析列表地址，抓取列表内容 3为解析简介及详细地址，抓取简介 4抓取详细
		boolean parseSubUrled = false;
		int threadNum = 5;
		boolean threadInited = false;
		boolean reCrawl = false;

		String crawlKey[] = { "ml_prod", "ml_sale", "ml_engine", "ml_holcar", "ml_part" };
		boolean flags[] = new boolean[] { false, false, false, false, false };
		flags[index] = true;
		conf.set(Nutch.CRAWL_ID_KEY, UrlList.langType + "_" + crawlKey[index]);
		UrlList.addUrls(flags);// boolean prods, boolean sales, boolean engine, boolean holcar, boolean part

		final DataStore<String, WebPage> store = StorageUtils.createWebStore(conf, String.class, WebPage.class);
		if (store == null)
			throw new RuntimeException("Could not create datastore");
		Query<String, WebPage> query = store.newQuery();
		if ((query instanceof Configurable)) {
			((Configurable) query).setConf(conf);
		}
		// {
		// MongoUtil mon = ParseSegment.getMongo("tml_part_webpage");
		// // mon.docCell.deleteMany(new BasicDBObject().append("fetchTime", new BasicDBObject("$gt",
		// // 1464749746642l)));
		// query.setStartKey(null);
		// query.setEndKey(null);
		// Result<String, WebPage> rs = query.execute();
		// try {
		// int rows = 0;
		// int brows = 0;
		// String key;
		// while (rs.next()) {
		// key = rs.getKey();
		// page = rs.get();
		// rows++;
		// if (key.equals(TableUtil.reverseUrl(page.getBaseUrl().toString()))) {
		// if (page.getContent() != null && page.getContent().array().length > 1024) {
		// BasicDBObject term = new BasicDBObject("key", key);
		// BasicDBObject doc = new BasicDBObject();
		// doc.put("status", page.getStatus());
		// doc.put("content", page.getContent().array().toString());
		// doc.put("configUrl", page.getConfigUrl().toString());
		// doc.put("fetchTime", page.getFetchTime());
		// doc.put("code", page.getProtocolStatus().getCode());
		// mon.put(term, doc);
		// brows++;
		// }
		// }
		// }
		// LogUtils.info("brows=" + brows + " allrows=" + rows);
		// rs.close();
		// return;
		// } catch (Exception exx) {
		// rs.close();
		// LogUtils.error("解析查询异常： ", exx);
		// if (rs != null)
		// return;
		// }
		// }

		Collection<WebPage.Field> fields = Arrays.asList(WebPage.Field.values());
		query.setFields(StorageUtils.toStringArray(fields));
		String startUrlKey = UrlList.getStartKey(index);// "https://www.marklines.com/cn/vehicle_production/";
		String endUrlKey = UrlList.getEndKey(index);// "https://www.marklines.com/cn/vehicle_production/yeaz";

		// startUrlKey;//
		// "http://tech.qq.com/a/20130824/004772.htm";
		query.setStartKey(TableUtil.reverseUrl(startUrlKey));
		query.setEndKey(TableUtil.reverseUrl(endUrlKey));

		UrlList.setUrlConfig(conf);
		UrlList.setSegmentParseRules(conf);

		final SegMentParsers parses = new SegMentParsers(conf);
		long _curTime = System.currentTimeMillis();
		final UrlPathMatch urlcfg = NutchConstant.getUrlConfig(conf);
		final boolean filter = conf.getBoolean(GeneratorJob.GENERATOR_FILTER, true);
		boolean normalise = conf.getBoolean(GeneratorJob.GENERATOR_NORMALISE, true);
		if (filter) {
			filters = new URLFilters(conf);
		}
		if (normalise) {
			normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
		}
		urlFilters = new URLFilters(conf);
		_curTime = conf.getLong(GeneratorJob.GENERATOR_CUR_TIME, System.currentTimeMillis());
		final long curTime = _curTime;
		scoringFilters = new ScoringFilters(conf);
		batchID = NutchConstant.getBatchId(conf);
		batchTime = "";
		final ProtocolFactory protocolFactory = new ProtocolFactory(conf);
		skipTruncated = conf.getBoolean(ParserJob.SKIP_TRUNCATED, true);
		parseUtil = new ParseUtil(conf);
		subUrlFilter = new SubURLFilters(conf);
		final Map<String, WebPage> panding = new HashMap<String, WebPage>();

		while (UrlList.next()) {
			WebPage subpage = UrlList.get();
			schedule.initializeSchedule(UrlList.cfg.url, subpage);
			String key = TableUtil.reverseUrl(UrlList.cfg.url);
			WebPage wp = store.get(key);
			if (wp != null) {
				continue;
			}
			if (UrlList.groupCfg.get(index).needAddList) {
				panding.put(key, subpage);
			}
			subpage.setBaseUrl(new Utf8(UrlList.cfg.url));
			subpage.putToMarkers(DbUpdaterJob.DISTANCE, new Utf8());
			subpage.setStatus(CrawlStatus.STATUS_UNFETCHED);
			try {
				scoringFilters.initialScore(UrlList.cfg.url, subpage);
			} catch (ScoringFilterException ex) {
				System.out.println(ex.getMessage());
				ex.printStackTrace();
				subpage.setScore(1.0f);
			}
			subpage.setConfigUrl(new Utf8(UrlList.rooturl));
			subpage.putToMarkers(DbUpdaterJob.DISTANCE, new Utf8(Integer.toString(Integer.MAX_VALUE)));
			subpage.setTitle(new Utf8("init"));
			store.put(TableUtil.reverseUrl(UrlList.cfg.url), subpage);
		}
		System.out.println("批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
		if (parseSubUrled && index == 4 && (step == 2 || step == 3)) {// 完成搜索查询，解析分页数
			if (step == 3) {
				startUrlKey = (UrlList.getStartKey(index, step - 1));
				endUrlKey = (UrlList.getEndKey(index, step - 1));
			}
			query.setStartKey(TableUtil.reverseUrl(startUrlKey));
			query.setEndKey(TableUtil.reverseUrl(endUrlKey));
			Result<String, WebPage> rs = query.execute();
			try {
				String key;
				while (rs.next()) {
					key = rs.getKey();
					page = rs.get();
					UrlList.parsePartPages(store, key, page, step);
				}
				rs.close();
				// Utils.sleep(Integer.MAX_VALUE);
			} catch (Exception exx) {
				rs.close();
				LogUtils.error("解析查询异常： ", exx);
			}
		}
		if (index == 4 && step > 1) {
			startUrlKey = UrlList.getStartKey(index, step);
			endUrlKey = UrlList.getEndKey(index, step);
			query.setStartKey(TableUtil.reverseUrl(startUrlKey));
			query.setEndKey(TableUtil.reverseUrl(endUrlKey));
		}
		final int retryMax = 3;

		BasicDBObject othTerm = new BasicDBObject();
		if (index == 4) {
			if (step == 2) {
				othTerm.put("title", "init");
			} else if (step == 3) {
				othTerm.put("title", "info");
			} else if (step == 4) {
				othTerm.put("title", "detail");
			}
		}

		((MongoStore) store).setOthterm(othTerm);
		Result<String, WebPage> rs = query.execute();
		HasThread thread[] = new HasThread[threadNum];
		int readTime = 0;
		boolean rsnext = rs.next();
		while (rsnext || panding.size() > 0) {
			if (rsnext) {
				String key = null;
				page = rs.get();
				key = rs.getKey();
				rsnext = rs.next();
				// page.setBaseUrl(new Utf8("https://www.marklines.com/cn/top500/s500_018"));
				// key = TableUtil.reverseUrl("https://www.marklines.com/cn/top500/s500_018");
				if (page.getStatus() == 2 && page.getContent().array().length > 1024) {
					if (!reCrawl && key.equals(TableUtil.reverseUrl(page.getBaseUrl().toString()))) {
						if (index == 4) {
							page.setText(new Utf8(page.getText().toString() + "ed"));
							LogUtils.info("修复状态: " + key);
							store.put(key, page);
						}
						continue;
					} else {
						page.setBaseUrl(new Utf8(TableUtil.unreverseUrl(key)));
					}
				}
				synchronized (panding) {
					panding.put(key, page);
				}
				url = page.getBaseUrl().toString();
				System.out.println("流程测试地址：" + url + " config:" + page.getConfigUrl());
			}
			while (panding.size() > 1000 && !stoped) {
				Utils.sleep(2000);
			}
			if (stoped) {
				break;
			}

			if (!threadInited) {
				for (int i = 0; i < threadNum; i++) {
					threadInited = true;
					Utils.sleep(1000);
					thread[i] = new HasThread() {
						@Override
						public void run() {
							while (true) {
								WebPage page = null;
								String key = null;
								int nullTimes = 0;
								while (page == null) {
									if (stoped) {
										break;
									}
									synchronized (panding) {
										if (panding.size() > 0) {
											Set<String> set = panding.keySet();
											if (set.iterator().hasNext()) {
												key = set.iterator().next();
												page = panding.remove(key);
											}
											panding.notify();
											break;
										}
									}
									nullTimes++;
									Utils.sleep(1000);
									if (nullTimes > 10) {
										break;
									}
								}
								if (page == null) {
									break;
								}
								String url = page.getBaseUrl().toString();
								System.err
										.println("///////////////////////////begin generator////////////////////////////////////////");
								// Utf8 mark = Mark.BETCH_MARK.checkMark(page);
								// if (mark != null) {
								// String[] bm = mark.toString().split(":");
								// if (bm.length == 2) {
								// if (curTime - Long.parseLong(bm[1]) < DbConfigFetchSchedule.SECONDS_PER_DAY * 1000
								// && page.getFetchInterval() > 3600) {
								// System.err.println("Skipping " + url + "; already generated");
								// continue;
								// }
								// }
								// }
								String baseUrl = page.getBaseUrl().toString();

								UrlPathMatch urlSegment = urlcfg.match(url);// 获取配置项
								if (urlSegment == null) {
									// 不满足配置要求
									System.err.println("url:" + url + " is not in urlconfig rowkey:" + key);
									continue;
									// } else {
									// System.err.println("url:" + url + " is config in urlconfig rowCount=" +
									// rowCount);
								}
								UrlNodeConfig value = (UrlNodeConfig) urlSegment.getNodeValue();
								String configUrl = urlSegment.getConfigPath();
								page.setConfigUrl(new Utf8(configUrl));
								if (configUrl.equals(url) || url.equals(configUrl + "/")) {// 应用新设置的属性
									System.err.println("url:" + url + " is config url, set [sorce,FetchInterval]"
											+ " to [" + value.customScore + "," + value.customInterval + "]");
									page.setScore(value.customScore);
									page.setFetchInterval(value.customInterval);
									page.putToMarkers(DbUpdaterJob.DISTANCE, new Utf8(Integer.toString(1)));
								} else if (page.getFetchInterval() < value.subFetchInterval * 0.5) {
									if (GeneratorJob.LOG.isWarnEnabled())
										GeneratorJob.LOG.warn("url:" + url + " FetchInterval is too small, set to "
												+ value.customInterval);
									page.setFetchInterval(value.subFetchInterval);
								}
								page.setCrawlType(value.crawlType);
								// filter on distance
								// if (value.fetchDepth > -1) {
								// Utf8 distanceUtf8 = page.getFromMarkers(DbUpdaterJob.DISTANCE);
								// if (distanceUtf8 != null) {
								// int distance = Integer.parseInt(distanceUtf8.toString());
								// if (distance > value.fetchDepth) {
								// GeneratorJob.LOG.warn("url:" + url + " depth:" + distance
								// + " config fetchDepth:" + value.fetchDepth + " rowKey:" + key);
								// continue;
								// }
								// }
								// }
								page.nodeConfig = value;
								// If filtering is on don't generate URLs that don't pass URLFilters
								try {
									url = normalizers.normalize(url, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
									// 允许配置文件中配置禁止抓取的地址
									if (filter && filters.filter(url) == null) {
										GeneratorJob.LOG.warn("filter url:" + url + " rowkey:" + key);
										continue;
									}
								} catch (URLFilterException e) {
									if (GeneratorJob.LOG.isWarnEnabled()) {
										GeneratorJob.LOG.warn("Couldn't filter url: " + url + " (" + e.getMessage()
												+ ")");
									}
									continue;
								} catch (MalformedURLException e) {
									if (GeneratorJob.LOG.isWarnEnabled()) {
										GeneratorJob.LOG.warn("Couldn't filter url: " + url + " (" + e.getMessage()
												+ ")");
									}
									continue;
								}

								page.setFetchTime(0);
								page.setPrevFetchTime(0);
								// check fetch schedule
								if (!schedule.shouldFetch(url, page, curTime)) {
									System.err.println("-shouldFetch rejected '" + url + "', fetchTime="
											+ page.getFetchTime() + ", curTime=" + curTime + " FetchInterval="
											+ page.getFetchInterval());
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
								System.err
										.println("// /////////////////////////end generator////////////////////////////////////////");
								page.setCrawlType(1);
								int retryTimes = 0;
								while (retryTimes < retryMax) {
									retryTimes++;
									try {
										System.err
												.println("// /////////////////////////begin fetch/////////////////////////////////////////");
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
											output(page, content, status, CrawlStatus.STATUS_FETCHED);
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
											output(page, content, status, code);
											break;
										case ProtocolStatusCodes.EXCEPTION:
											LOG.warn("fetch of " + url + " failed with: "
													+ ProtocolStatusUtils.getMessage(status));
											throw new IOException(ProtocolStatusUtils.getMessage(status));
											/* FALLTHROUGH */
										case ProtocolStatusCodes.RETRY: // retry
										case ProtocolStatusCodes.BLOCKED:
											output(page, null, status, CrawlStatus.STATUS_RETRY);
											break;

										case ProtocolStatusCodes.GONE: // gone
										case ProtocolStatusCodes.NOTFOUND:
										case ProtocolStatusCodes.ACCESS_DENIED:
										case ProtocolStatusCodes.ROBOTS_DENIED:
											LOG.warn("FetcherReducer 572: set URL:"
													+ url
													+ " fetchinterval from "
													+ page.getFetchInterval()
													+ " to "
													+ (page.getFetchInterval() * 5 + 30 * FetchSchedule.SECONDS_PER_DAY));
											page.setFetchInterval(page.getFetchInterval() * 5 + 30
													* FetchSchedule.SECONDS_PER_DAY);
											output(page, null, status, CrawlStatus.STATUS_GONE);
											break;
										case ProtocolStatusCodes.NOTMODIFIED:
											output(page, null, status, CrawlStatus.STATUS_NOTMODIFIED);
											break;
										default:
											if (LOG.isWarnEnabled()) {
												LOG.warn("Unknown ProtocolStatus: " + status.getCode());
											}
											output(page, null, status, CrawlStatus.STATUS_RETRY);
										}
										System.err
												.println("// /////////////////////////end fetch////////////////////////////////////////");
										if (content == null) {
											System.err.println("url:" + url + " fetch content is null");
											continue;
										}
									} catch (Exception e) {
										LogUtils.error("fetch failed " + e.getMessage());
										Utils.sleep(1000);
										continue;
									}
								}
								System.err
										.println("// /////////////////////////begin parse////////////////////////////////////////");
								if (!skipTruncated || (skipTruncated && !ParserJob.isTruncated(url, page))) {
									parseUtil.process(key, page);
								}
								ParseStatus pstatus = page.getParseStatus();
								if (pstatus != null) {
									System.out.println(("ParserStatus:" + ParseStatusCodes.majorCodes[pstatus
											.getMajorCode()]));
								} else {
									System.out.println(("ParserStatus:" + "failed"));
									continue;
								}
								if (UrlList.langType.equals("cn")
										&& page.getTitle().toString().startsWith("登录 - 全球领先B2B汽车门户网")) {
									LogUtils.error("退出登录： " + page.getBaseUrl());
									stoped = true;
									break;
								} else if (UrlList.langType.equals("en")
										&& page.getTitle().toString().startsWith("Login - Automotive")) {
									LogUtils.error("退出登录： " + page.getBaseUrl());
									stoped = true;
									break;
								}
								System.err
										.println("// /////////////////////////end parse////////////////////////////////////////");
								System.err
										.println("// /////////////////////////begin dbupdate////////////////////////////////////////");
								Map<Utf8, Utf8> outlinks = page.getOutlinks();
								if (outlinks != null) {
									int depth = 100000;
									Utf8 depthUtf8 = page.getFromMarkers(DbUpdaterJob.DISTANCE);
									if (depthUtf8 != null)
										depth = Integer.parseInt(depthUtf8.toString());
									for (Entry<Utf8, Utf8> e : outlinks.entrySet()) {
										String olurl = e.getKey().toString();
										olurl = olurl.replaceAll("\\.\\./", "").replaceAll("&amp;", "&");
										if (!checkUrl(olurl, e.getValue().toString())) {
											page.removeFromOutlinks(e.getKey());
										}
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
										// System.out.println("config:" + urlSegment.getConfigPath());
										if (!SubURLFilters.filter(olurl, value.subFilters)) {
											page.removeFromOutlinks(e.getKey());
											continue;
										}
										try {
											URI u = new URI(olurl);
											InetAddress.getByName(u.getHost());
											if (UrlList.groupCfg.get(index).addFetchListRule.matcher(olurl).matches()) {
												if (index == 0 || index == 1) {
													// https://www.marklines.com/cn/vehicle_production/year?nationCode=ZAF&fromYear=2000&toYear=2016
													olurl = olurl.replaceAll("fromYear=(\\d+)", "fromYear=2002");
													olurl = olurl.replaceAll("toYear=(\\d+)", "toYear=2015");
													olurl = olurl.replaceAll("toYear=2015&toYear=2015", "toYear=2015");
													olurl = olurl.replaceAll("fromYear=2002&fromYear=2002",
															"fromYear=2002");
												}
												String olkey = TableUtil.reverseUrl(olurl);
												WebPage wp = store.get(olkey);
												if (wp != null) {
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
												subpage.putToMarkers(DbUpdaterJob.DISTANCE,
														new Utf8(Integer.toString(depth)));
												subpage.setTitle(new Utf8("init"));
												subpage.setBaseUrl(new Utf8(olurl));
												store.put(olkey, subpage);
											} else {
												page.removeFromOutlinks(e.getKey());
												continue;
											}
										} catch (Exception ex) {
											ex.printStackTrace();
											LOG.warn("outlink:" + olurl + " author:" + e.getKey() + " error:"
													+ ex.getMessage());
											page.removeFromOutlinks(e.getKey());
											continue;
										}
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
									schedule.setFetchSchedule(url, page, prevFetchTime, 0L, fetchTime, modifiedTime,
											modified);
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
								System.out.println("WebPage==" + page);
								store.put(key, page);
								System.err
										.println("// /////////////////////////end dbupdate////////////////////////////////////////");
								// System.out.println(new String(page.getContent().array()));
								// page.getOutlinks()
								System.err
										.println("// /////////////////begin segmentparse////////////////////////////////////////");
								WebPageSegment wps = SegParserReducer.parseSegMent(parses, url, page);// 有解析成功的数据返回
								System.out.println("WebPageSegment==" + wps);
								System.err
										.println("// /////////////////////////end segmentparse////////////////////////////////////////");
								Utils.sleep(1000);
							}
						}
					}.setName("crawl_" + (i + 1)).start();
				}
			}
		}
		synchronized (panding) {
			panding.notifyAll();
		}
		if (threadInited) {
			for (HasThread hasThread : thread) {
				hasThread.join();
			}
		}
		stoped = true;
		System.exit(0);
	}
}
