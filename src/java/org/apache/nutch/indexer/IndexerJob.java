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
package org.apache.nutch.indexer;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.mapreduce.StringComparator;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageIndex;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IndexerJob extends NutchTool implements Tool {

	public static final Logger LOG = LoggerFactory.getLogger(IndexerJob.class);

	private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

	private static final Utf8 REINDEX = new Utf8("-reindex");

	static {
		FIELDS.add(WebPage.Field.SIGNATURE);
		FIELDS.add(WebPage.Field.PARSE_STATUS);
		FIELDS.add(WebPage.Field.SCORE);
		FIELDS.add(WebPage.Field.MARKERS);
	}

	public static class IndexerMapper extends GoraMapper<String, WebPageIndex, String, NutchDocument> {
		public IndexUtil indexUtil;
		public DataStore<String, WebPage> store;
		private long docCount = 0;
		private String batchID;
		private String batchTime;

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			batchID = NutchConstant.getBatchId(conf);
			batchTime = NutchConstant.getBatchTime(conf);
			indexUtil = new IndexUtil(conf);
			try {
				store = StorageUtils.createWebStore(conf, String.class, WebPage.class);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
			LOG.info("IndexerMap-批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
			NutchConstant.setupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.solrIndexNode, context, true);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			store.close();
			LOG.info("batchId:" + batchID + " write " + docCount + " NutchDocument");
			LOG.info("IndexerMap-批次ID：" + batchID + "  退出时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
			context.setStatus("write " + docCount);
			NutchConstant.cleanupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.solrIndexNode, context
					.getTaskAttemptID().getTaskID().toString(), true, true, docCount);
		};

		@Override
		public void map(String key, WebPageIndex page, Context context) throws IOException, InterruptedException {
			key = NutchConstant.getWebPageUrl(batchID, key);
			if (key == null)
				return;
			ParseStatus pstatus = page.getParseStatus();
			if (pstatus == null || !ParseStatusUtils.isSuccess(pstatus) || pstatus.getMinorCode() == ParseStatusCodes.SUCCESS_REDIRECT) {
				LOG.error(key + " parseStatus=" + pstatus);
				return; // filter urls not parsed
			}
			if (page.getTitle() == null || page.getTitle().toString().equals("")) {
				LOG.error("KEY:" + key + " title is null");
				return;
			}
			// System.err.println(page);
			Utf8 mark = Mark.PARSE_MARK.checkMark(page);
			NutchDocument doc = indexUtil.index(key, page);
			if (doc == null) {
				LOG.error(key + "   doc is null");
				return;
			}
			LOG.info(key + " doc=" + doc);
			// if (mark != null) {
			// WebPage wp = new WebPage();
			// Mark.INDEX_MARK.putMark(wp, batchID);
			// store.put(key, wp);// 回写
			// }
			docCount++;
			context.setStatus("write " + docCount);
			context.write(key, doc);
		}
	}

	/**
	 * IndexerAllMapper form webpage
	 * 
	 * @author hans
	 * 
	 */
	public static class IndexerAllMapper extends GoraMapper<String, WebPage, String, NutchDocument> {
		public IndexUtil indexUtil;

		private long docCount = 0;

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			indexUtil = new IndexUtil(conf);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			LOG.info("reindex write " + docCount + " NutchDocument");
			context.setStatus("write " + docCount);
		};

		@Override
		public void map(String key, WebPage page, Context context) throws IOException, InterruptedException {
			ParseStatus pstatus = page.getParseStatus();
			if (page.getText() == null)
				return;
			if (pstatus == null || !ParseStatusUtils.isSuccess(pstatus) || pstatus.getMinorCode() == ParseStatusCodes.SUCCESS_REDIRECT) {
				LOG.error(key + " parseStatus=" + pstatus);
				return; // filter urls not parsed
			}
			NutchDocument doc = indexUtil.index(key, page);
			if (doc == null) {
				LOG.error(key + "   doc is null");
				return;
			}
			docCount++;
			context.setStatus("write " + docCount);
			context.write(key, doc);
		}
	}

	public static Collection<WebPage.Field> getFields(Job job) {
		Configuration conf = job.getConfiguration();
		Collection<WebPage.Field> columns = new HashSet<WebPage.Field>(FIELDS);
		IndexingFilters filters = new IndexingFilters(conf);
		columns.addAll(filters.getFields());
		ScoringFilters scoringFilters = new ScoringFilters(conf);
		columns.addAll(scoringFilters.getFields());
		return columns;
	}

	protected <inV extends Persistent, map extends GoraMapper<String, inV, String, NutchDocument>> Job createIndexJob(Configuration conf,
			String jobName, Class<inV> inValueClass, Class<map> indexerMapperClass) throws IOException, ClassNotFoundException {
		Job job = new NutchJob(conf, jobName);
		// TODO: Figure out why this needs to be here
		job.getConfiguration().setClass("mapred.output.key.comparator.class", StringComparator.class, RawComparator.class);

		Collection<WebPage.Field> fields = getFields(job);
		StorageUtils.initMapperJob(job, fields, inValueClass, String.class, NutchDocument.class, indexerMapperClass);
		job.setNumReduceTasks(0);
		job.setOutputFormatClass(IndexerOutputFormat.class);
		return job;
	}
}
