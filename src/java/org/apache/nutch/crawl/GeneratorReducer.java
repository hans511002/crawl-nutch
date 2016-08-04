package org.apache.nutch.crawl;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraReducer;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.GeneratorJob.SelectorEntry;
import org.apache.nutch.fetcher.FetcherJob.FetcherMapper;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageIndex;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;

/**
 * Reduce class for generate
 * 
 * The #reduce() method write a random integer to all generated URLs. This random number is then used by {@link FetcherMapper}.
 * 
 */
public class GeneratorReducer extends GoraReducer<SelectorEntry, WebPage, String, WebPageIndex> {

	private long limit;
	private long maxCount;
	private long count = 0;
	private int byMode = 0;
	private Map<String, Integer> hostCountMap = new HashMap<String, Integer>();
	private Utf8 batchId;
	private String batchID;
	private String batchTime;
	DataStore<String, WebPage> store = null;
	String batchMarkData = null;
	long curTime = 0;

	@Override
	protected void reduce(SelectorEntry key, Iterable<WebPage> values, Context context) throws IOException, InterruptedException {
		context.setStatus("读取：" + count + " limit：" + limit + " <br/>更新时间：" + (new Date().toLocaleString()));
		for (WebPage page : values) {
			if (limit > 0 && count >= limit) {
				return;
			}
			if (byMode == 4 && key.onceCount > 0) {
				String hostordomain = key.cfgId + "";
				Integer hostCount = hostCountMap.get(hostordomain);
				if (hostCount == null) {
					hostCountMap.put(hostordomain, 0);
					hostCount = 0;
				}
				if (hostCount >= key.onceCount) {
					return;
				}
				System.out.println("cfgId:" + hostordomain + "  limit:" + key.onceCount + " curCount:" + hostCount + "  " + key.url);
				hostCountMap.put(hostordomain, hostCount + 1);
			} else if (maxCount > 0 && (byMode == 1 || byMode == 2)) {
				String hostordomain;
				if (byMode == 1) {
					hostordomain = URLUtil.getDomainName(key.url);
				} else {
					hostordomain = URLUtil.getHost(key.url);
				}
				Integer hostCount = hostCountMap.get(hostordomain);
				if (hostCount == null) {
					hostCountMap.put(hostordomain, 0);
					hostCount = 0;
				}
				if (hostCount >= maxCount) {
					return;
				}
				hostCountMap.put(hostordomain, hostCount + 1);
			}
			Mark.GENERATE_MARK.putMark(page, batchId);
			try {
				String reversedUrl = TableUtil.reverseUrl(key.url);
				// reversedUrl = batchID + reversedUrl;
				String wpIndexReversedUrl = NutchConstant.getWebPageIndexUrl(batchID, reversedUrl);
				WebPageIndex wpi = new WebPageIndex(page);
				GeneratorJob.LOG.info("write url:" + key.url + " key:" + reversedUrl);
				context.write(wpIndexReversedUrl, wpi);

				WebPage wp = new WebPage();
				wp.setFetchInterval(page.getFetchInterval());
				wp.setMarkers(page.getMarkers());// 写入配置项
				Mark.BETCH_MARK.putMark(wp, batchMarkData);
				store.put(reversedUrl, wp);
				// System.err.println("WebPageIndex getSchema=" + wpi.getSchema());
				// System.err.println("reversedUrl=" + reversedUrl + "  \n page=" + wpi);
			} catch (MalformedURLException e) {
				context.getCounter("Generator", "MALFORMED_URL").increment(1);
				continue;
			}
			context.getCounter("Generator", "GENERATE_MARK").increment(1);
			count++;
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		long totalLimit = conf.getLong(GeneratorJob.GENERATOR_TOP_N, Long.MAX_VALUE);
		if (totalLimit == Long.MAX_VALUE) {
			limit = Long.MAX_VALUE;
		} else {
			int r = context.getNumReduceTasks();
			if (r > 2)
				r = r / 2;
			limit = totalLimit / r;
		}
		maxCount = conf.getLong(GeneratorJob.GENERATOR_MAX_COUNT, -2);
		batchId = new Utf8(conf.get(GeneratorJob.BATCH_ID));
		String countMode = conf.get(GeneratorJob.GENERATOR_COUNT_MODE, GeneratorJob.GENERATOR_COUNT_VALUE_HOST);
		if (countMode.equals(GeneratorJob.GENERATOR_COUNT_VALUE_DOMAIN)) {
			byMode = 1;
		} else if (countMode.equals(GeneratorJob.GENERATOR_COUNT_VALUE_HOST)) {
			byMode = 2;
		} else if (countMode.equals(GeneratorJob.GENERATOR_COUNT_VALUE_CFG)) {
			byMode = 4;
		}

		batchID = NutchConstant.getBatchId(conf);
		batchTime = NutchConstant.getBatchTime(conf);
		curTime = conf.getLong(GeneratorJob.GENERATOR_CUR_TIME, System.currentTimeMillis());
		batchMarkData = batchID + ":" + curTime;
		try {
			store = StorageUtils.createWebStore(conf, String.class, WebPage.class);
			if (store == null)
				throw new IOException("Could not create datastore");
		} catch (ClassNotFoundException e) {
			throw new IOException("ERROR:" + e.getMessage());
		}
		GeneratorJob.LOG.info("generatorReducer-批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
		NutchConstant.setupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.generatNode, context, false);
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		if (store != null) {
			store.close();
			store = null;
		}
		context.setStatus(" 添加地址数：" + count + " <br/>更新时间：" + (new Date().toLocaleString()));
		GeneratorJob.LOG.info("generatorReducer-批次ID：" + batchID + "  退出时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime
				+ " 添加地址数：" + count);
		NutchConstant.cleanupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.generatNode, context.getTaskAttemptID()
				.getTaskID().toString(), false, true, count);
	}
}
