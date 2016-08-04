/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.apache.nutch.crawl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.urlfilter.SubURLFilters;
import org.apache.nutch.urlfilter.UrlPathMatch;
import org.apache.nutch.urlfilter.UrlPathMatch.UrlNodeConfig;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.WebPageWritable;
import org.slf4j.Logger;

public class DbUpdateReducer extends GoraReducer<UrlWithScore, NutchWritable, String, WebPage> {

	public static final String CRAWLDB_ADDITIONS_ALLOWED = "db.update.additions.allowed";

	public static final Logger LOG = DbUpdaterJob.LOG;

	private int retryMax;
	private boolean additionsAllowed;
	private int maxInterval;
	private FetchSchedule schedule;
	private ScoringFilters scoringFilters;
	private List<ScoreDatum> inlinkedScoreData = new ArrayList<ScoreDatum>();
	private int maxLinks;
	private String batchID;
	private String batchTime;
	int rowCount = 0;
	int failCount = 0;
	int acrCount = 0;
	SubURLFilters subUrlFilter;
	private URLNormalizers normalizers;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		subUrlFilter = new SubURLFilters(conf);
		normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
		retryMax = conf.getInt("db.fetch.retry.max", 3);
		additionsAllowed = conf.getBoolean(CRAWLDB_ADDITIONS_ALLOWED, true);
		maxInterval = conf.getInt("db.fetch.interval.max", 0);
		schedule = FetchScheduleFactory.getFetchSchedule(conf);
		scoringFilters = new ScoringFilters(conf);
		maxLinks = conf.getInt("db.update.max.inlinks", 10000);
		batchID = NutchConstant.getBatchId(conf);
		batchTime = NutchConstant.getBatchTime(conf);
		DbUpdaterJob.LOG.info("dbUpdateReduce-批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
		NutchConstant.setupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.dbUpdateNode, context, false);
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		setStatus(context);
		DbUpdaterJob.LOG.info("dbUpdateReduce-批次ID：" + batchID + "  退出时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
		NutchConstant.cleanupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.dbUpdateNode, context.getTaskAttemptID()
				.getTaskID().toString(), false, true, rowCount);
	}

	void setStatus(Context context) {
		String res = "";
		for (String code : new String[] { "更新数", "新增数", "冗余数", "无配置", "规则过滤数", "异常数", "retryMax" }) {
			org.apache.hadoop.mapreduce.Counter counter = context.getCounter("reduceWrite", code);
			res += (" " + counter.getName() + "=" + counter.getValue()) + "\n";
		}
		context.setStatus(res += " <br/>更新时间：" + (new Date().toLocaleString()));
	}

	@Override
	protected void reduce(UrlWithScore key, Iterable<NutchWritable> values, Context context) throws IOException, InterruptedException {
		String keyUrl = key.getUrl().toString();
		WebPage page = null;
		inlinkedScoreData.clear();
		boolean isPage = false;
		String url;
		try {
			url = TableUtil.unreverseUrl(keyUrl);
			url = normalizers.normalize(url, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
			UrlPathMatch urlSegment = NutchConstant.urlcfg.match(url);// 获取配置项
			if (urlSegment == null) {
				System.err.println("未匹配到配置地址 过滤URL:" + url);
				context.getCounter("reduceWrite", "无配置").increment(1);
				return;
			}
			UrlNodeConfig value = (UrlNodeConfig) urlSegment.getNodeValue();
			if (value.regFormat != null) {
				url = value.regFormat.replace(url);
			}
			if (!SubURLFilters.filter(url, value.subFilters)) {
				System.err.println("过滤URL:" + url);
				context.getCounter("reduceWrite", "规则过滤数").increment(1);
				return;
			}
			URI u = new URI(url);
			InetAddress.getByName(u.getHost());
			keyUrl = TableUtil.reverseUrl(url);
		} catch (Exception e) {
			// this can happen because a newly discovered malformed link
			// may slip by url filters
			failCount++;
			context.getCounter("reduceWrite", "异常数").increment(1);
			System.err.println(keyUrl + " unreverseUrl error:" + e.getMessage());
			return;
		}
		for (NutchWritable nutchWritable : values) {
			Writable val = nutchWritable.get();
			if (val instanceof WebPageWritable) {
				page = ((WebPageWritable) val).getWebPage();
				isPage = true;
			} else {
				inlinkedScoreData.add((ScoreDatum) val);
				if (inlinkedScoreData.size() >= maxLinks) {
					System.out.println("Limit reached, skipping further inlinks for " + keyUrl);
					break;
				}
			}
		}

		if (page == null) { // new row
			acrCount += inlinkedScoreData.size() - 1;
			context.getCounter("reduceWrite", "新增数").increment(1);
			context.getCounter("reduceWrite", "冗余数").increment(inlinkedScoreData.size() - 1);
			if (!additionsAllowed) {
				return;
			}
			page = new WebPage();// parts update ,
			// schedule.initializeSchedule(url, page);
			// page.putToMarkers(DbUpdaterJob.DISTANCE, new Utf8());
			// page.setStatus(CrawlStatus.STATUS_UNFETCHED);
			try {
				scoringFilters.initialScore(url, page);
			} catch (ScoringFilterException e) {
				System.out.println(e.getMessage());
				e.printStackTrace();
				page.setScore(1.0f);
			}
		}
		if (isPage) {
			if (page.getRetriesSinceFetch() >= retryMax) {
				page.setStatus(CrawlStatus.STATUS_GONE);
			}
			byte status = (byte) page.getStatus();
			switch (status) {
			case CrawlStatus.STATUS_REDIR_TEMP: // successful fetch, redirected
			case CrawlStatus.STATUS_REDIR_PERM:
				page.setRetriesSinceFetch(page.getRetriesSinceFetch() + 1);
			case CrawlStatus.STATUS_FETCHED: // succesful fetch
			case CrawlStatus.STATUS_NOTMODIFIED: // successful fetch, notmodified
				int modified = FetchSchedule.STATUS_UNKNOWN;
				if (status == CrawlStatus.STATUS_NOTMODIFIED) {
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
				// if (maxInterval < page.getFetchInterval())
				// schedule.forceRefetch(url, page, false);
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
			org.apache.nutch.storage.ParseStatus pstatus = page.getParseStatus();
			if (pstatus == null || !ParseStatusUtils.isSuccess(pstatus)) {
				page.setFetchInterval(0);
				page.setFetchTime(0);
			}
		}

		// if (page.getInlinks() != null) {//未全表读取，不清空
		// page.getInlinks().clear();
		// }
		// for (ScoreDatum inlink : inlinkedScoreData) {
		// page.putToInlinks(new Utf8(inlink.getUrl()), new Utf8(inlink.getAnchor()));
		// }

		// Distance calculation.
		// Retrieve smallest distance from all inlinks distances
		// Calculate new distance for current page: smallest inlink distance plus 1.
		// If the new distance is smaller than old one (or if old did not exist yet),
		// write it to the page.
		int smallestDist = Integer.MAX_VALUE;
		for (ScoreDatum inlink : inlinkedScoreData) {
			int inlinkDist = inlink.getDistance();
			if (inlinkDist < smallestDist) {
				smallestDist = inlinkDist;
			}
			page.putToInlinks(new Utf8(inlink.getUrl()), new Utf8(inlink.getAnchor()));
		}
		int oldDistance = Integer.MAX_VALUE;
		Utf8 oldDistUtf8 = page.getFromMarkers(DbUpdaterJob.DISTANCE);
		if (oldDistUtf8 != null && !oldDistUtf8.toString().equals("")) {
			oldDistance = Integer.parseInt(oldDistUtf8.toString());
		}
		if (smallestDist < oldDistance) {
			page.putToMarkers(DbUpdaterJob.DISTANCE, new Utf8(Integer.toString(smallestDist)));
		}
		try {
			scoringFilters.updateScore(url, page, inlinkedScoreData);
		} catch (ScoringFilterException e) {
			System.err.println("Scoring filters failed with exception " + StringUtils.stringifyException(e));
		}
		// clear markers
		// But only delete when they exist. This is much faster for the underlying
		// store. The markers are on the input anyway.
		if (page.getFromMetadata(FetcherJob.REDIRECT_DISCOVERED) != null) {
			page.removeFromMetadata(FetcherJob.REDIRECT_DISCOVERED);
		}
		Mark.INJECT_MARK.removeMark(page);
		Mark.GENERATE_MARK.removeMark(page);
		Mark.FETCH_MARK.removeMark(page);
		Mark.PARSE_MARK.removeMark(page);
		Mark.UPDATEDB_MARK.removeMark(page);
		Mark.INDEX_MARK.removeMark(page);
		Mark.BETCH_MARK.removeMark(page);
		rowCount++;
		context.getCounter("reduceWrite", "更新数").increment(1);
		setStatus(context);
		System.out.println("url=" + url + " rowkey=" + keyUrl);
		context.write(keyUrl, page);
	}
}
