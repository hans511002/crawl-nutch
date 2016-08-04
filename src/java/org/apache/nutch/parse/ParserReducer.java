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
package org.apache.nutch.parse;

import java.io.IOException;
import java.util.Date;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraReducer;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageIndex;
import org.apache.nutch.urlfilter.SubURLFilters;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.TableUtil;

@SuppressWarnings("deprecation")
public class ParserReducer extends GoraReducer<Text, WebPageIndex, String, WebPageIndex> {
	private String batchID;
	private String batchTime;
	DataStore<String, WebPage> store = null;
	private ParseUtil parseUtil;
	private boolean shouldResume;
	private boolean force;
	private Utf8 batchId;
	private boolean skipTruncated;
	int rowCount = 0;

	@Override
	protected void reduce(Text key, Iterable<WebPageIndex> values, Context context) throws IOException, InterruptedException {
		String urlKey = key.toString();
		for (WebPageIndex page : values) {
			if (rowCount % 100 == 0) {
				context.setStatus(getParseStatus(context));
			}
			rowCount++;
			boolean shoulParse = true;
			Utf8 mark = Mark.FETCH_MARK.checkMark(page);
			String unreverseKey = TableUtil.unreverseUrl(urlKey);
			if (page.getStatus() != CrawlStatus.STATUS_FETCHED) {
				System.err.println(("Status is not STATUS_FETCHED  notparsed " + unreverseKey));
				context.getCounter("ParserStatus", "notparsed").increment(1);
				return;
			}
			if (page.getContent() == null || page.getContent().hasArray() == false || page.getContent().array().length == 0) {
				System.err.println(("nocontent " + unreverseKey));
				context.getCounter("ParserStatus", "nocontent").increment(1);
				return;
			}
			if (batchId.equals(ParserJob.REPARSE)) {
				ParserJob.LOG.debug("Reparsing " + unreverseKey);
			} else {
				if (!NutchJob.shouldProcess(mark, batchId)) {
					ParserJob.LOG.warn("Skipping " + unreverseKey + "; different batch id (" + mark + ")");
				} else if (shouldResume && Mark.PARSE_MARK.checkMark(page) != null) {
					if (force) {
						ParserJob.LOG.info("Forced parsing " + unreverseKey + "; already parsed");
					} else {
						ParserJob.LOG.info("Skipping " + unreverseKey + "; already parsed");
						shoulParse = false;
					}
				} else {
					ParserJob.LOG.info("Parsing " + unreverseKey);
				}
			}

			if (skipTruncated && ParserJob.isTruncated(unreverseKey, page)) {
				ParserJob.LOG.info("skipTruncated " + unreverseKey);
				shoulParse = false;
			}
			if (shoulParse)
				parseUtil.process(urlKey, page);
			ParseStatus pstatus = page.getParseStatus();
			if (pstatus != null) {
				context.getCounter("ParserStatus", ParseStatusCodes.majorCodes[pstatus.getMajorCode()]).increment(1);
			} else {
				context.getCounter("ParserStatus", "failed").increment(1);
				ParserJob.LOG.warn("rowkey:" + key + "  url:" + unreverseKey);
			}

			String indexKey = NutchConstant.getWebPageIndexUrl(batchID, urlKey);
			context.write(indexKey, page);
			WebPageIndex wpi = new WebPageIndex(page);
			Mark.INJECT_MARK.removeMark(wpi);
			Mark.GENERATE_MARK.removeMark(wpi);
			Mark.FETCH_MARK.removeMark(wpi);
			Mark.PARSE_MARK.removeMark(wpi);
			Mark.UPDATEDB_MARK.removeMark(wpi);
			Mark.INDEX_MARK.removeMark(wpi);
			Mark.BETCH_MARK.removeMark(wpi);
			store.put(urlKey, wpi);
		}
	}

	String getParseStatus(Context context) {
		String res = "parser result:";
		for (String code : ParseStatusCodes.majorCodes) {
			org.apache.hadoop.mapreduce.Counter counter = context.getCounter("ParserStatus", code);
			res += (" " + counter.getName() + "=" + counter.getValue()) + "\n";
		}
		return res + " <br/>更新时间：" + (new Date().toLocaleString());
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		if (store != null) {
			store.close();
			store = null;
		}
		String status = getParseStatus(context);
		context.setStatus(status);
		ParserJob.LOG.info(status);
		ParserJob.LOG.info("parserReducer-批次ID：" + batchID + "  退出时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
		NutchConstant.cleanupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.parseNode, context.getTaskAttemptID()
				.getTaskID().toString(), false, true, context.getCounter("ParserStatus", "success").getValue());
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		SubURLFilters subUrlFilter = new SubURLFilters(conf);
		batchID = NutchConstant.getBatchId(conf);
		batchTime = NutchConstant.getBatchTime(conf);
		parseUtil = new ParseUtil(conf);
		shouldResume = conf.getBoolean(ParserJob.RESUME_KEY, false);
		force = conf.getBoolean(ParserJob.FORCE_KEY, false);
		batchId = new Utf8(conf.get(GeneratorJob.BATCH_ID, Nutch.ALL_BATCH_ID_STR));
		skipTruncated = conf.getBoolean(ParserJob.SKIP_TRUNCATED, true);
		batchID = NutchConstant.getBatchId(conf);
		batchTime = NutchConstant.getBatchTime(conf);
		NutchConstant.setupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.parseNode, context, true);

		try {
			store = StorageUtils.createWebStore(conf, String.class, WebPage.class);
			if (store == null)
				throw new IOException("Could not create datastore");
		} catch (ClassNotFoundException e) {
			throw new IOException("ERROR:" + e.getMessage());
		}
		ParserJob.LOG.info("parserReducer-批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
		NutchConstant.setupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.parseNode, context, false);
	}
}
