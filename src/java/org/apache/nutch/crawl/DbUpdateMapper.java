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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.scoring.ScoreDatum;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.WebPageIndex;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.WebPageWritable;
import org.slf4j.Logger;

public class DbUpdateMapper extends GoraMapper<String, WebPageIndex, UrlWithScore, NutchWritable> {
	public static final Logger LOG = DbUpdaterJob.LOG;

	private ScoringFilters scoringFilters;
	private final List<ScoreDatum> scoreData = new ArrayList<ScoreDatum>();

	// reuse writables
	private UrlWithScore urlWithScore = new UrlWithScore();
	private NutchWritable nutchWritable = new NutchWritable();
	private WebPageWritable pageWritable;
	private String batchID;
	private String batchTime;
	int rowCount = 0;
	int outCount = 0;
	long[] lst = new long[6];

	@Override
	public void map(String key, WebPageIndex page, Context context) throws IOException, InterruptedException {
		key = NutchConstant.getWebPageUrl(batchID, key);
		if (key == null)
			return;
		String url = TableUtil.unreverseUrl(key);
		scoreData.clear();
		Map<Utf8, Utf8> outlinks = page.getOutlinks();
		if (outlinks != null) {
			for (Entry<Utf8, Utf8> e : outlinks.entrySet()) {
				int depth = 0;
				Utf8 depthUtf8 = page.getFromMarkers(DbUpdaterJob.DISTANCE);
				if (depthUtf8 != null)
					depth = Integer.parseInt(depthUtf8.toString());
				long l = System.currentTimeMillis();
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
				lst[0] += System.currentTimeMillis() - l;
				l = System.currentTimeMillis();
				lst[1] += System.currentTimeMillis() - l;
				scoreData.add(new ScoreDatum(page.getScore(), olurl, e.getValue().toString(), depth + 1));
			}
		}
		long l = System.currentTimeMillis();
		// TODO: Outlink filtering (i.e. "only keep the first n outlinks")
		try {
			scoringFilters.distributeScoreToOutlinks(url, page, scoreData, (outlinks == null ? 0 : outlinks.size()));
		} catch (ScoringFilterException e) {
			LOG.warn("Distributing score failed for URL: " + key + " exception:" + StringUtils.stringifyException(e));
		}
		lst[3] += System.currentTimeMillis() - l;
		l = System.currentTimeMillis();
		urlWithScore.setUrl(key);
		urlWithScore.setScore(Float.MAX_VALUE);
		pageWritable.setWebPage(page);
		nutchWritable.set(pageWritable);
		// System.err.println(key + " ConfigUrl=" + page.getConfigUrl());
		context.write(urlWithScore, nutchWritable);
		rowCount++;
		lst[4] += System.currentTimeMillis() - l;
		for (ScoreDatum scoreDatum : scoreData) {
			l = System.currentTimeMillis();
			String reversedOut = TableUtil.reverseUrl(scoreDatum.getUrl());
			scoreDatum.setUrl(url);// 传入做inlink
			urlWithScore.setUrl(reversedOut);
			urlWithScore.setScore(scoreDatum.getScore());
			nutchWritable.set(scoreDatum);
			rowCount++;
			outCount++;
			context.write(urlWithScore, nutchWritable);
			lst[5] += System.currentTimeMillis() - l;
		}
		setStatus(context);
	}

	void setStatus(Context context) {
		String msg = "总地址数：" + rowCount + " 外部链接数:" + outCount;
		for (int i = 0; i < lst.length; i++) {
			msg += "lst[" + i + "]=" + lst[i] + "<br/>";
		}
		context.setStatus(msg + "  <br/>更新时间：" + (new Date().toLocaleString()));
	}

	@Override
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		scoringFilters = new ScoringFilters(context.getConfiguration());
		pageWritable = new WebPageWritable(context.getConfiguration(), null);
		batchID = NutchConstant.getBatchId(conf);
		batchTime = NutchConstant.getBatchTime(conf);
		DbUpdaterJob.LOG.info("dbUpdateMap-批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
		NutchConstant.setupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.dbUpdateNode, context, true);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		setStatus(context);
		DbUpdaterJob.LOG.info("dbUpdateMap-批次ID：" + batchID + "  退出时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
		NutchConstant.cleanupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.dbUpdateNode, context.getTaskAttemptID()
				.getTaskID().toString(), true);
	}
}
