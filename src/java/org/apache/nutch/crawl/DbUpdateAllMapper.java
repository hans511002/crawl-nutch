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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.urlfilter.SubURLFilters;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.WebPageWritable;
import org.slf4j.Logger;

public class DbUpdateAllMapper extends GoraMapper<String, WebPage, UrlWithScore, NutchWritable> {
	public static final Logger LOG = DbUpdaterJob.LOG;

	private ScoringFilters scoringFilters;
	private URLNormalizers normalizers;
	SubURLFilters subUrlFilter;
	private final List<ScoreDatum> scoreData = new ArrayList<ScoreDatum>();

	// reuse writables
	private UrlWithScore urlWithScore = new UrlWithScore();
	private NutchWritable nutchWritable = new NutchWritable();
	private WebPageWritable pageWritable;
	private String batchID;
	private String batchTime;
	int rowCount = 0;

	public boolean checkUrl(String olurl, String name) {
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
			return address != null;
		} catch (Exception ex) {
			ex.printStackTrace();
			LOG.warn("outlink:" + olurl + " author:" + name + " error:" + ex.getMessage());
			return false;
		}
	}

	@Override
	public void map(String key, WebPage page, Context context) throws IOException, InterruptedException {
		String url = TableUtil.unreverseUrl(key);
		scoreData.clear();
		Map<Utf8, Utf8> outlinks = page.getOutlinks();
		if (outlinks != null) {
			for (Entry<Utf8, Utf8> e : outlinks.entrySet()) {
				int depth = Integer.MAX_VALUE;
				Utf8 depthUtf8 = page.getFromMarkers(DbUpdaterJob.DISTANCE);
				if (depthUtf8 != null)
					depth = Integer.parseInt(depthUtf8.toString());
				String olurl = e.getKey().toString();
				if (!checkUrl(olurl, e.getValue().toString())) {
					page.removeFromOutlinks(e.getKey());
				}
				scoreData.add(new ScoreDatum(0.0f, olurl, e.getValue().toString(), depth));
			}
		}

		// TODO: Outlink filtering (i.e. "only keep the first n outlinks")
		try {
			scoringFilters.distributeScoreToOutlinks(url, page, scoreData, (outlinks == null ? 0 : outlinks.size()));
		} catch (ScoringFilterException e) {
			LOG.warn("Distributing score failed for URL: " + key + " exception:" + StringUtils.stringifyException(e));
		}
		urlWithScore.setUrl(key);
		urlWithScore.setScore(Float.MAX_VALUE);
		pageWritable.setWebPage(page);
		nutchWritable.set(pageWritable);
		// System.err.println(key + " ConfigUrl=" + page.getConfigUrl());
		context.write(urlWithScore, nutchWritable);
		rowCount++;
		for (ScoreDatum scoreDatum : scoreData) {
			String reversedOut = TableUtil.reverseUrl(scoreDatum.getUrl());
			scoreDatum.setUrl(url);// 传入做inlink
			urlWithScore.setUrl(reversedOut);
			urlWithScore.setScore(scoreDatum.getScore());
			nutchWritable.set(scoreDatum);
			rowCount++;
			context.write(urlWithScore, nutchWritable);
		}
		context.setStatus("update rowCount:" + rowCount);
	}

	@Override
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		subUrlFilter = new SubURLFilters(conf);
		scoringFilters = new ScoringFilters(context.getConfiguration());
		pageWritable = new WebPageWritable(context.getConfiguration(), null);
		batchID = NutchConstant.getBatchId(conf);
		batchTime = NutchConstant.getBatchTime(conf);
		normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
		DbUpdaterJob.LOG.info("dbUpdateMap-批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
		NutchConstant.setupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.dbUpdateNode, context, true);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.setStatus("总共更新地址数：" + rowCount);
		DbUpdaterJob.LOG.info("dbUpdateMap-批次ID：" + batchID + "  退出时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
		NutchConstant.cleanupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.dbUpdateNode, context.getTaskAttemptID()
				.getTaskID().toString(), true);
	}
}
