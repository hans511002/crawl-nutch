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
package org.apache.nutch.indexer.solr.segment;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraInputSplit;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.mapreduce.GoraOutputFormat;
import org.apache.gora.mapreduce.GoraReducer;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchIndexWriterFactory;
import org.apache.nutch.indexer.solr.SolrConstants;
import org.apache.nutch.indexer.solr.SolrWriter;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.element.SegParserJob;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPageSegment;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.ToolUtil;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentReIndexerJob extends NutchTool implements Tool {

	private static final String RESUME_KEY = "parse.job.resume";
	public static final Logger LOG = LoggerFactory.getLogger(SegmentReIndexerJob.class);

	public static class ReIndexReducer extends GoraReducer<Text, WebPageSegment, String, WebPageSegment> {

		public static final Logger LOG = LoggerFactory.getLogger(ReIndexReducer.class);
		int rowCount = 0;
		int successCount = 0;
		int failCount = 0;
		private String batchID;
		private String batchTime;
		SolrWriter solrWriter = null;
		boolean indexed = false;
		private int commitSize;

		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			batchID = NutchConstant.getBatchId(conf);
			batchTime = NutchConstant.getBatchTime(conf);
			solrWriter = new SolrWriter();
			indexed = conf.getBoolean(SegParserJob.SEGMENT_INDEX_KEY, false);
			solrWriter.open(conf);
			LOG.info("parserReduce-批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
			commitSize = conf.getInt(SolrConstants.COMMIT_SIZE, 300);
			NutchConstant.setupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.segmentIndexNode, context, true);
			System.err.println(conf.getClass(GoraOutputFormat.DATA_STORE_CLASS, Object.class));
			System.err.println(conf.getClass(GoraOutputFormat.OUTPUT_KEY_CLASS, Object.class));
			System.err.println(conf.getClass(GoraOutputFormat.OUTPUT_VALUE_CLASS, Object.class));
			System.err.println(conf.get("io.serializations"));
		}

		protected void reduce(Text key, Iterable<WebPageSegment> values, Context context) throws IOException, InterruptedException {
			String urlKey = key.toString();
			for (WebPageSegment page : values) {
				context.setStatus(getParseStatus(context));
				rowCount++;
				NutchDocument doc = SegmentSolrIndexUtil.index(urlKey, page);
				if (doc == null) {
					failCount++;
					page.setMarker(null);
					context.write(urlKey, page);// 回写
					LOG.error(key + "   doc is null");
					return;
				} else {
					successCount++;
					solrWriter.write(doc);
					page.setMarker(new Utf8(batchID + ":" + batchTime));
					context.write(urlKey, page);// 回写
				}
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			solrWriter.close();
			LOG.info("parserReduce-批次ID：" + batchID + "  退出时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
			context.setStatus(getParseStatus(context));
			LOG.info(getParseStatus(context));
			NutchConstant.cleanupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.segmentIndexNode, context
					.getTaskAttemptID().getTaskID().toString(), true);
		}

		String getParseStatus(Context context) {
			return "read:" + rowCount + " index:" + successCount + "  " + " failed:" + failCount + " <br/>更新时间："
					+ (new Date().toLocaleString());
		}

	}

	public static class ReIndexerMapper extends GoraMapper<String, WebPageSegment, Text, WebPageSegment> {
		public DataStore<String, WebPageSegment> store;
		private long rowCount = 0;
		private long sucCount = 0;
		private String batchID;
		private String batchTime;
		private boolean shouldResume;

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			batchID = NutchConstant.getBatchId(conf);
			batchTime = NutchConstant.getBatchTime(conf);
			shouldResume = conf.getBoolean(RESUME_KEY, false);
			try {
				store = StorageUtils.createWebStore(conf, String.class, WebPageSegment.class);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
			LOG.info("IndexerMap-批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
			GoraInputSplit split = (GoraInputSplit) context.getInputSplit();
			context.setStatus(split.getQuery().getStartKey() + "==>" + split.getQuery().getEndKey());
			LOG.info("start_rowkey: " + split.getQuery().getStartKey() + "  end_rowkey:" + split.getQuery().getEndKey());
			NutchConstant.setupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.segmentIndexNode, context, true);
			System.err.println(conf.getClass(GoraOutputFormat.DATA_STORE_CLASS, Object.class));
			System.err.println(conf.getClass(GoraOutputFormat.OUTPUT_KEY_CLASS, Object.class));
			System.err.println(conf.getClass(GoraOutputFormat.OUTPUT_VALUE_CLASS, Object.class));
			System.err.println(conf.get("io.serializations"));
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			store.close();
			LOG.info("IndexerMap-批次ID：" + batchID + "  退出时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
			context.setStatus(getParseStatus(context));
			NutchConstant.cleanupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.segmentIndexNode, context
					.getTaskAttemptID().getTaskID().toString(), true);
		};

		@Override
		public void map(String key, WebPageSegment page, Context context) throws IOException, InterruptedException {
			rowCount++;
			if (page.getTitle() == null || page.getTitle().toString().equals("")) {
				LOG.error("KEY:" + key + " title is null");
				return;
			}
			context.setStatus(getParseStatus(context));
			Utf8 mark = page.getMarker();
			if (mark != null) {
				if (shouldResume && mark.toString().split(":")[0].equals(batchID)) {
					return;
				}
			} else {
				// return;
			}
			sucCount++;
			context.write(new Text(key), page);
		}

		String getParseStatus(Context context) {
			return "read:" + rowCount + "  " + " needIndex:" + sucCount;
		}
	}

	@Override
	public Map<String, Object> run(Map<String, Object> args) throws Exception {
		String solrUrl = (String) args.get(Nutch.ARG_SOLR);
		String batchZKId = this.getConf().get(NutchConstant.BATCH_ID_KEY);
		Integer numTasks = (Integer) args.get(Nutch.ARG_NUMTASKS);

		LOG.info("SegmentSolrIndexerJob: batchId: " + batchZKId);
		NutchIndexWriterFactory.addClassToConf(getConf(), SolrWriter.class);
		getConf().set(SolrConstants.SERVER_URL, solrUrl);
		// currentJob = SegmentIndexerJob.createIndexJob(getConf(), "segment-index:" + batchZKId, WebPageSegment.class,
		// ReIndexerMapper.class);
		// currentJob.getConfiguration().set(NutchConstant.STEPZKBATCHTIME, new Date().toLocaleString());
		// currentJob.setOutputFormatClass(IndexerOutputFormat.class);
		// currentJob.setNumReduceTasks(0);
		// currentJob.waitForCompletion(true);
		// ToolUtil.recordJobStatus(null, currentJob, results);
		// return results;

		currentJob = new NutchJob(getConf(), "segment ReIndex:" + (this.getConf().get(NutchConstant.BATCH_ID_KEY)));
		currentJob.getConfiguration().set(NutchConstant.STEPZKBATCHTIME, new Date().toLocaleString());
		Collection<WebPageSegment.Field> fields = SegmentIndexerJob.getFields(currentJob);// IndexerJob.getFields(currentJob);
		NutchConstant.setSegmentParseRules(currentJob.getConfiguration());
		WebPageSegment.initMapperJob(currentJob, fields, WebPageSegment.class, Text.class, WebPageSegment.class, ReIndexerMapper.class,
				null, true);
		StorageUtils.initReducerJob(currentJob, WebPageSegment.class, ReIndexReducer.class);

		System.err.println(currentJob.getConfiguration().getClass(GoraOutputFormat.DATA_STORE_CLASS, Object.class));
		System.err.println(currentJob.getConfiguration().getClass(GoraOutputFormat.OUTPUT_KEY_CLASS, Object.class));
		System.err.println(currentJob.getConfiguration().getClass(GoraOutputFormat.OUTPUT_VALUE_CLASS, Object.class));
		System.err.println(currentJob.getConfiguration().get("io.serializations"));

		if (numTasks == null || numTasks < 1) {
			currentJob.setNumReduceTasks(currentJob.getConfiguration().getInt("mapred.reduce.tasks", currentJob.getNumReduceTasks()));
		} else {
			currentJob.setNumReduceTasks(numTasks);
		}
		// StorageUtils.initReducerJob(currentJob, WebPageSegment.class, SegmentParserAllParserReducer.class);
		currentJob.waitForCompletion(true);
		ToolUtil.recordJobStatus(null, currentJob, results);
		return results;
	}

	public void indexSolr(String solrUrl, String batchId, int numTasks) throws Exception {
		LOG.info("SolrIndexerJob: starting solrUrl=" + solrUrl + " type=" + batchId);

		run(ToolUtil.toArgMap(Nutch.ARG_SOLR, solrUrl, Nutch.ARG_BATCH, batchId, Nutch.ARG_NUMTASKS, numTasks));
		// do the commits once and for all the reducers in one go
		LOG.info("SolrIndexerJob: starting solrUrl=" + solrUrl + " type=" + batchId);
		SolrServer solr = new HttpSolrServer(solrUrl);
		if (getConf().getBoolean(SolrConstants.COMMIT_INDEX, true)) {
			solr.commit();
		}
		LOG.info("SolrIndexerJob: done.");
	}

	public int run(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Usage: SolrIndexerJob <-index url>  <-batch batchId> "
					+ " <-startkey startKey> <-endkey endKey>  [-numTasks N]  [-crawlId <id>]");
			return -1;
		}
		int numTasks = 0;
		String solrUrl = getConf().get(SolrConstants.SEGMENT_URL, null);
		if (solrUrl != null) {
			getConf().set(SolrConstants.SERVER_URL, solrUrl);
		}
		for (int i = 0; i < args.length; i++) {
			if ("-numTasks".equals(args[i])) {
				numTasks = Integer.parseInt(args[++i]);
			} else if ("-resume".equals(args[i])) {
				getConf().setBoolean(RESUME_KEY, true);
			} else if ("-crawlId".equals(args[i])) {
				getConf().set(Nutch.CRAWL_ID_KEY, args[++i]);
			} else if ("-batch".equals(args[i])) {
				String batchId1 = org.apache.commons.lang.StringUtils.lowerCase(args[i + 1]);
				if (batchId1 != null && !batchId1.equals("")) {
					getConf().set(NutchConstant.BATCH_ID_KEY, batchId1);
				}
				i++;
			} else if ("-index".equals(args[i])) {
				i++;
				getConf().set(SolrConstants.SERVER_URL, args[i]);
			} else if ("-startkey".equals(args[i])) {
				getConf().set(NutchConstant.stepHbaseStartRowKey, args[++i]);
			} else if ("-endkey".equals(args[i])) {
				getConf().set(NutchConstant.stepHbaseEndRowKey, args[++i]);
			} else {
				throw new IllegalArgumentException("Usage: Usage: SolrIndexerJob <-index url>  <-batch batchId> "
						+ " <-startkey startKey> <-endkey endKey>  [-numTasks N]  [-crawlId <id>]");
			}
		}
		if (getConf().get(NutchConstant.BATCH_ID_KEY, null) == null) {
			throw new Exception("args error no -batch label or value");
		}
		if (getConf().get(NutchConstant.stepHbaseStartRowKey) == null || getConf().get(NutchConstant.stepHbaseEndRowKey) == null) {
			throw new Exception("args error no -startkey label value OR -endkey label value ");
		}
		try {
			indexSolr(getConf().get(SolrConstants.SERVER_URL), getConf().get(NutchConstant.BATCH_ID_KEY), numTasks);
			return 0;
		} catch (Exception e) {
			LOG.error("SolrIndexerJob: " + StringUtils.stringifyException(e));
			if (NutchConstant.exitValue != 0) {
				return NutchConstant.exitValue;
			}
			return -1;
		}
	}

	public static void main(String[] args) throws Exception {
		final int res = ToolRunner.run(NutchConfiguration.create(), new SegmentReIndexerJob(), args);
		System.exit(res);
	}

}
