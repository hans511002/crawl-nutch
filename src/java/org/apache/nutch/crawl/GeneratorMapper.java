package org.apache.nutch.crawl;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraInputSplit;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.GeneratorJob.SelectorEntry;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.urlfilter.SubURLFilters;
import org.apache.nutch.urlfilter.UrlPathMatch;
import org.apache.nutch.urlfilter.UrlPathMatch.UrlNodeConfig;
import org.apache.nutch.util.TableUtil;

public class GeneratorMapper extends GoraMapper<String, WebPage, SelectorEntry, WebPage> {

	private URLFilters filters;
	private URLNormalizers normalizers;
	private boolean filter;
	private boolean normalise;
	private FetchSchedule schedule;
	private ScoringFilters scoringFilters;
	private long curTime;
	private long rowCount = 0;
	private long scanRowCount = 0;
	private SelectorEntry entry = new SelectorEntry();
	private String batchID;
	private String batchTime;
	public static UrlPathMatch urlcfg = null;
	private long limit = Integer.MAX_VALUE;
	int retryMax = 3;
	int deleteRowCount = 0;

	DataStore<String, WebPage> store = null;

	@Override
	public void map(String reversedUrl, WebPage page, Context context) throws IOException, InterruptedException {
		scanRowCount++;
		if ((scanRowCount % 1000) == 0)
			context.setStatus(getGenStatus(context));
		if (rowCount >= limit) {
			context.getCounter("mapRead", "limit").increment(1);
			return;
		}
		if (retryMax > 0 && page.getRetriesSinceFetch() >= retryMax) {
			context.getCounter("mapRead", "retryMax").increment(1);
			return;
		}
		if (page.getStatus() == CrawlStatus.STATUS_GONE) {
			context.getCounter("mapRead", "STATUS_GONE").increment(1);
			return;
		}
		String url = TableUtil.unreverseUrl(reversedUrl);
		if (page.getReprUrl() != null) {
			try {
				String rurl = page.getReprUrl().toString();
				if (page.getRetriesSinceFetch() > 1) {
					context.getCounter("mapRead", "redirect").increment(1);
					System.err.println("url:" + url + " is redirect  " + rurl);
					return;
				}
				URL ru = new URL(rurl);
				URL u = new URL(url);
				if (ru.getHost().equals(u.getHost())) {
					url = rurl;
					reversedUrl = TableUtil.reverseUrl(rurl);
				} else {
					context.getCounter("mapRead", "redirect").increment(1);
					System.err.println("url:" + url + " is redirect  " + rurl);
					return;
				}
			} catch (Exception e) {
				context.getCounter("mapRead", "urlException").increment(1);
				return;
			}

		}
		// String baseUrl = page.getBaseUrl().toString();
		UrlPathMatch urlSegment = urlcfg.match(url);// 获取配置项
		if (urlSegment == null) {
			// 不满足配置要求
			context.getCounter("mapRead", "not_in_config").increment(1);
			GeneratorJob.LOG.warn("url:" + url + " is not in urlconfig rowkey:" + reversedUrl);
			return;
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
		if (configUrl.equals(url) || configUrl.equals(curl)
				|| (url.endsWith("/") && configUrl.equals(url.substring(0, url.length() - 1)))) {// 应用新设置的属性
			GeneratorJob.LOG.info("url:" + url + " is config url, set [sorce,FetchInterval]" + " to ["
					+ value.customScore + "," + value.customInterval + "]");
			page.setScore(value.customScore);
			page.setFetchInterval(value.customInterval);
			page.putToMarkers(DbUpdaterJob.DISTANCE, new Utf8(Integer.toString(1)));
		} else {
			if (value.subFilters != null) {
				if (SubURLFilters.filter(url, value.subFilters) == false) {
					GeneratorJob.LOG.info("规则：" + filter + " 过滤URL:" + url);
					return;
				}
			}
			boolean isSubCfgUrl = false;
			if (value.subCfgRules != null) {
				isSubCfgUrl = SubURLFilters.filter(url, value.subCfgRules);
			}
			if (isSubCfgUrl) {
				GeneratorJob.LOG.info("规则：" + filter + " 重置URL属性:" + url);
				page.setScore(value.customScore);
				page.setFetchInterval(value.customInterval);
				page.putToMarkers(DbUpdaterJob.DISTANCE, new Utf8(Integer.toString(2)));
			} else if (page.getFetchInterval() < value.subFetchInterval * 0.5) {
				// GeneratorJob.LOG.warn("url:" + url + " FetchInterval is too small, set to " +
				// value.subFetchInterval);
				page.setFetchInterval(value.subFetchInterval);
			}
		}
		Utf8 mark = Mark.BETCH_MARK.checkMark(page);
		if (mark != null) {
			String[] bm = mark.toString().split(":");
			if (bm.length == 2) {
				if (curTime - Long.parseLong(bm[1]) < page.getFetchInterval() && !bm[0].equals(batchID)) {
					context.getCounter("mapRead", "already_generated").increment(1);
					GeneratorJob.LOG.warn("Skipping " + url + "; already generated");
					return;
				}
			}
		}
		page.setCrawlType(value.crawlType);
		// filter on distance
		if (value.fetchDepth > -1) {
			Utf8 distanceUtf8 = page.getFromMarkers(DbUpdaterJob.DISTANCE);
			if (distanceUtf8 != null) {
				int distance = Integer.parseInt(distanceUtf8.toString());
				if (distance > value.fetchDepth) {
					context.getCounter("mapRead", "high_depth").increment(1);
					GeneratorJob.LOG.warn("url:" + url + " depth:" + distance + " config fetchDepth:"
							+ value.fetchDepth + " rowKey:" + reversedUrl);
					return;
				}
			}
		}
		Mark.ROOTID_MARK.putMark(page, value.rootSiteId + "");// 配置根地址ID
		Mark.MEDIATYPEID_MARK.putMark(page, value.mediaTypeId + "");// 媒体类型ID
		Mark.MEDIALEVELID_MARK.putMark(page, value.mediaLevelId + "");// 媒体级别ID
		Mark.TOPICID_MARK.putMark(page, value.topicTypeId + "");// 栏目类型ID
		Mark.AREAID_MARK.putMark(page, value.areaId + "");// 地域ID

		// If filtering is on don't generate URLs that don't pass URLFilters
		try {
			if (normalise) {
				url = normalizers.normalize(url, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
			}
			// 允许配置文件中配置禁止抓取的地址
			if (filter && filters.filter(url) == null) {
				GeneratorJob.LOG.warn("filter url:" + url + " rowkey:" + reversedUrl);
				return;
			}
		} catch (URLFilterException e) {
			context.getCounter("mapRead", "filter_err").increment(1);
			if (GeneratorJob.LOG.isWarnEnabled()) {
				GeneratorJob.LOG.warn("Couldn't filter url: " + url + " (" + e.getMessage() + ")");
				return;
			}
		} catch (MalformedURLException e) {
			context.getCounter("mapRead", "filter_err").increment(1);
			if (GeneratorJob.LOG.isWarnEnabled()) {
				GeneratorJob.LOG.warn("Couldn't filter url: " + url + " (" + e.getMessage() + ")");
				return;
			}
		}
		page.nodeConfig = value;
		// check fetch schedule
		if (!schedule.shouldFetch(url, page, curTime)) {
			context.getCounter("mapRead", "shouldFetch_rejected").increment(1);
			if (GeneratorJob.LOG.isDebugEnabled()) {
				System.out.println("-shouldFetch rejected '" + url + "', fetchTime=" + page.getFetchTime()
						+ ", curTime=" + curTime + " FetchInterval=" + page.getFetchInterval());
			}
			return;
		}
		page.nodeConfig = null;
		float score = page.getScore();
		try {
			score = scoringFilters.generatorSortValue(url, page, score);
			page.setScore(score);
		} catch (ScoringFilterException e) {
		}
		if (value != null && value.regFormat != null) {
			String tmpurl = value.regFormat.replace(url);
			if (!tmpurl.equals(url)) {
				System.out.println("delete:" + reversedUrl + "  add:" + tmpurl);
				url = tmpurl;
				store.delete(reversedUrl);
				deleteRowCount++;
				if (deleteRowCount % 1000 == 0) {
					store.flush();
				}
			}
		}
		context.getCounter("mapRead", "SuccessWrite").increment(1);
		System.out.println("write url:" + url + " rowkey:" + reversedUrl);
		GeneratorJob.LOG.info("write url:" + url + " rowkey:" + reversedUrl);
		entry.set(url, score);
		entry.cfgId = value.cfgUrlId;
		entry.onceCount = value.onceCount;
		rowCount++;
		context.write(entry, page);
	}

	String getGenStatus(Context context) {
		String res = "write " + rowCount + " pages scanRowCount:" + scanRowCount;
		for (String code : new String[] { "SuccessWrite", "already_generated", "high_depth", "filter_err",
				"shouldFetch_rejected", "limit", "retryMax", "redirect", "urlException", "not_in_config" }) {
			org.apache.hadoop.mapreduce.Counter counter = context.getCounter("mapRead", code);
			res += (" " + counter.getName() + "=" + counter.getValue()) + "\n";
		}
		return res + " <br/>更新时间：" + (new Date().toLocaleString());
	}

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		filter = conf.getBoolean(GeneratorJob.GENERATOR_FILTER, true);
		normalise = conf.getBoolean(GeneratorJob.GENERATOR_NORMALISE, true);
		limit = conf.getLong(GeneratorJob.GENERATOR_TOP_N, Long.MAX_VALUE);
		if (limit < 5) {
			limit = Long.MAX_VALUE;
		}
		retryMax = conf.getInt("db.fetch.retry.max", 3);
		if (filter) {
			filters = new URLFilters(conf);
		}
		if (normalise) {
			normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
		}
		try {
			store = StorageUtils.createWebStore(conf, String.class, WebPage.class);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new IOException(e);
		}
		curTime = conf.getLong(GeneratorJob.GENERATOR_CUR_TIME, System.currentTimeMillis());
		schedule = FetchScheduleFactory.getFetchSchedule(conf);
		scoringFilters = new ScoringFilters(conf);
		batchID = NutchConstant.getBatchId(conf);
		batchTime = NutchConstant.getBatchTime(conf);
		GeneratorJob.LOG.info("limit=" + limit);
		GeneratorJob.LOG.info("generatorMap-批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间："
				+ batchTime);
		GoraInputSplit split = (GoraInputSplit) context.getInputSplit();
		context.setStatus(split.getQuery().getStartKey() + "==>" + split.getQuery().getEndKey());
		GeneratorJob.LOG.info("start_rowkey: " + split.getQuery().getStartKey() + "  end_rowkey:"
				+ split.getQuery().getEndKey());
		urlcfg = NutchConstant.getUrlConfig(conf);

		NutchConstant.setupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.generatNode, context,
				true);
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		if (store != null) {
			store.flush();
			store.close();
		}
		GeneratorJob.LOG.info("generatorMap-批次ID：" + batchID + "  退出时间:" + (new Date().toLocaleString()) + " 批次时间："
				+ batchTime + " rowCount=" + rowCount);
		GoraInputSplit split = (GoraInputSplit) context.getInputSplit();
		context.setStatus(getGenStatus(context) + " from  " + split.getQuery().getStartKey() + "==>"
				+ split.getQuery().getEndKey());
		NutchConstant.cleanupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.generatNode, context
				.getTaskAttemptID().getTaskID().toString(), true);
	}
}
