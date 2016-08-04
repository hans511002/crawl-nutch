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
package org.apache.nutch.parse.element;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraInputSplit;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.mapreduce.GoraReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchIndexWriterFactory;
import org.apache.nutch.indexer.solr.SolrConstants;
import org.apache.nutch.indexer.solr.SolrWriter;
import org.apache.nutch.indexer.solr.segment.SegmentSolrIndexUtil;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.ParseFilters;
import org.apache.nutch.parse.ParserFactory;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageIndex;
import org.apache.nutch.storage.WebPageSegment;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegParserAllIndexJob extends NutchTool implements Tool {

	public static final Logger LOG = LoggerFactory.getLogger(SegParserAllIndexJob.class);

	private static final String RESUME_KEY = "parse.job.resume";
	private static final String FORCE_KEY = "parse.job.force";

	public static final String SKIP_TRUNCATED = "parser.skip.truncated";

	private static final Utf8 REPARSE = new Utf8("-reparse");

	private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

	private Configuration conf;

	static {
		FIELDS.add(WebPage.Field.CONTENT);
		FIELDS.add(WebPage.Field.CONFIGURL);
		FIELDS.add(WebPage.Field.SCORE);
		FIELDS.add(WebPage.Field.FETCH_TIME);
		FIELDS.add(WebPage.Field.PREV_FETCH_TIME);
		FIELDS.add(WebPage.Field.MODIFIED_TIME);
		FIELDS.add(WebPage.Field.CONTENT_TYPE);
		FIELDS.add(WebPage.Field.TITLE);
		FIELDS.add(WebPage.Field.HEADERS);
		FIELDS.add(WebPage.Field.MARKERS);
		FIELDS.add(WebPage.Field.METADATA);
	}

	public static class SegmentParserAllReducer extends GoraReducer<Text, WebPageIndex, String, WebPageSegment> {
		int rowCount = 0;
		int successCount = 0;
		int failCount = 0;
		private String batchID;
		private String batchTime;
		private SegMentParsers parses;
		SolrWriter solrWriter = null;
		boolean indexed = false;

		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			parses = new SegMentParsers(conf);
			batchID = NutchConstant.getBatchId(conf);
			batchTime = NutchConstant.getBatchTime(conf);
			solrWriter = new SolrWriter();
			indexed = conf.getBoolean(SegParserJob.SEGMENT_INDEX_KEY, false);
			solrWriter.open(conf);
			LOG.info("parserReduce-批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
			NutchConstant.setupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.segmentParsNode, context, true);
		}

		protected void reduce(Text key, Iterable<WebPageIndex> values, Context context) throws IOException, InterruptedException {
			String urlKey = key.toString();
			String unreverseKey = TableUtil.unreverseUrl(urlKey);
			for (WebPageIndex page : values) {
				rowCount++;
				context.setStatus(getParseStatus(context));
				if (page.getContent() == null)
					return;
				WebPageSegment wps = SegParserReducer.parseSegMent(parses, unreverseKey, page);// 有解析成功的数据返回
				if (wps != null) {
					if (indexed) {
						NutchDocument doc = SegmentSolrIndexUtil.index(urlKey, wps);
						if (doc != null) {
							solrWriter.write(doc);
							wps.setMarker(new Utf8(batchID + ":" + batchTime));
						} else {
							SegParserJob.LOG.warn(urlKey + " url:" + unreverseKey + " doc is null");
						}
					}
					context.write(urlKey, wps);
					successCount++;
				} else {
					failCount++;
				}
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			solrWriter.close();
			LOG.info("parserReduce-批次ID：" + batchID + "  退出时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
			context.setStatus(getParseStatus(context));
			LOG.info(getParseStatus(context));
			NutchConstant.cleanupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.segmentParsNode, context
					.getTaskAttemptID().getTaskID().toString(), true);
		}

		String getParseStatus(Context context) {
			return "read:" + rowCount + " parsed:" + successCount + "  " + " failed:" + failCount + " <br/>更新时间："
					+ (new Date().toLocaleString());
		}
	}

	public static class SegmentParserAllMapper extends GoraMapper<String, WebPageIndex, Text, WebPageIndex> {
		private SegMentParsers parses;
		private boolean shouldResume;// 需要加写webpage
		SolrWriter solrWriter = null;

		private String batchID;
		private String batchTime;
		int rowCount = 0;
		int successCount = 0;

		// String indexKey;

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			parses = new SegMentParsers(conf);
			shouldResume = conf.getBoolean(RESUME_KEY, false);
			batchID = NutchConstant.getBatchId(conf);
			batchTime = NutchConstant.getBatchTime(conf);
			solrWriter = new SolrWriter();
			solrWriter.open(conf);
			LOG.info("parserMap-批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
			GoraInputSplit split = (GoraInputSplit) context.getInputSplit();
			context.setStatus(split.getQuery().getStartKey() + "==>" + split.getQuery().getEndKey());
			LOG.info("start_rowkey: " + split.getQuery().getStartKey() + "  end_rowkey:" + split.getQuery().getEndKey());
			NutchConstant.setupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.segmentParsNode, context, true);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			solrWriter.close();
			LOG.info("parserMap-批次ID：" + batchID + "  退出时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
			context.setStatus(getParseStatus(context));
			LOG.info(getParseStatus(context));
			NutchConstant.cleanupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.segmentParsNode, context
					.getTaskAttemptID().getTaskID().toString(), true);
		}

		String getParseStatus(Context context) {
			return "read:" + rowCount + " needed parsed:" + successCount + " <br/>更新时间：" + (new Date().toLocaleString());
		}

		@Override
		public void map(String key, WebPageIndex page, Context context) throws IOException, InterruptedException {
			rowCount++;
			context.setStatus(getParseStatus(context));
			if (page.getContent() == null)
				return;
			key = key.substring(14);
			context.write(new Text(key), page);
			successCount++;
		}
	}

	public SegParserAllIndexJob() {

	}

	public SegParserAllIndexJob(Configuration conf) {
		setConf(conf);
	}

	/**
	 * Checks if the page's content is truncated.
	 * 
	 * @param url
	 * @param page
	 * @return If the page is truncated <code>true</code>. When it is not, or when it could be determined, <code>false</code>.
	 */
	public static boolean isTruncated(String url, WebPage page) {
		ByteBuffer content = page.getContent();
		if (content == null) {
			return false;
		}
		Utf8 lengthUtf8 = page.getFromHeaders(new Utf8(HttpHeaders.CONTENT_LENGTH));
		if (lengthUtf8 == null) {
			return false;
		}
		String lengthStr = lengthUtf8.toString().trim();
		if (StringUtil.isEmpty(lengthStr)) {
			return false;
		}
		int inHeaderSize;
		try {
			inHeaderSize = Integer.parseInt(lengthStr);
		} catch (NumberFormatException e) {
			LOG.warn("Wrong contentlength format for " + url, e);
			return false;
		}
		int actualSize = content.limit();
		if (inHeaderSize > actualSize) {
			LOG.warn(url + " skipped. Content of size " + inHeaderSize + " was truncated to " + actualSize);
			return true;
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug(url + " actualSize=" + actualSize + " inHeaderSize=" + inHeaderSize);
		}
		return false;
	}

	public Collection<WebPage.Field> getFields(Job job) {
		Configuration conf = job.getConfiguration();
		Collection<WebPage.Field> fields = new HashSet<WebPage.Field>(FIELDS);
		ParserFactory parserFactory = new ParserFactory(conf);
		ParseFilters parseFilters = new ParseFilters(conf);

		Collection<WebPage.Field> parsePluginFields = parserFactory.getFields();
		Collection<WebPage.Field> signaturePluginFields = SignatureFactory.getFields(conf);
		Collection<WebPage.Field> htmlParsePluginFields = parseFilters.getFields();

		if (parsePluginFields != null) {
			fields.addAll(parsePluginFields);
		}
		if (signaturePluginFields != null) {
			fields.addAll(signaturePluginFields);
		}
		if (htmlParsePluginFields != null) {
			fields.addAll(htmlParsePluginFields);
		}

		return fields;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Map<String, Object> run(Map<String, Object> args) throws Exception {
		String batchId = (String) args.get(Nutch.ARG_BATCH);
		Boolean shouldResume = (Boolean) args.get(Nutch.ARG_RESUME);
		Boolean force = (Boolean) args.get(Nutch.ARG_FORCE);
		Integer numTasks = (Integer) args.get(Nutch.ARG_NUMTASKS);

		if (batchId != null) {
			getConf().set(GeneratorJob.BATCH_ID, batchId);
		}
		if (shouldResume != null) {
			getConf().setBoolean(RESUME_KEY, shouldResume);
		}
		if (force != null) {
			getConf().setBoolean(FORCE_KEY, force);
		}
		String batchZKId = this.getConf().get(NutchConstant.BATCH_ID_KEY);
		LOG.info("FetcherJob: batchId: " + batchZKId);

		LOG.info("ParserJob: batchId: " + batchZKId);
		LOG.info("ParserJob: resuming:\t" + getConf().getBoolean(RESUME_KEY, false));
		LOG.info("ParserJob: forced reparse:\t" + getConf().getBoolean(FORCE_KEY, false));
		if (batchId == null || batchId.equals(Nutch.ALL_BATCH_ID_STR)) {
			LOG.info("ParserJob: parsing all from Index");
		} else {
			LOG.info("ParserJob: batchId:\t" + batchId);
		}
		if (getConf().getBoolean(SegParserJob.SEGMENT_INDEX_KEY, false)) {
			currentJob = new NutchJob(getConf(), "segment parse-indexAllIndex:" + (this.getConf().get(NutchConstant.BATCH_ID_KEY)));
		} else {
			currentJob = new NutchJob(getConf(), "segment parseAllIndex:" + (this.getConf().get(NutchConstant.BATCH_ID_KEY)));
		}
		currentJob.getConfiguration().set(NutchConstant.STEPZKBATCHTIME, new Date().toLocaleString());
		Collection<WebPage.Field> fields = getFields(currentJob);
		NutchConstant.setSegmentParseRules(currentJob.getConfiguration());
		StorageUtils.initMapperJob(currentJob, fields, WebPageIndex.class, Text.class, WebPageIndex.class, SegmentParserAllMapper.class);
		StorageUtils.initReducerJob(currentJob, WebPageSegment.class, SegmentParserAllReducer.class);

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

	public int parse(boolean shouldResume, boolean force, int numTasks) throws Exception {
		LOG.info("ParserJob: starting");
		run(ToolUtil.toArgMap(Nutch.ARG_RESUME, shouldResume, Nutch.ARG_FORCE, force, Nutch.ARG_NUMTASKS, numTasks));
		LOG.info("ParserJob: success");
		return 0;
	}

	public int run(String[] args) throws Exception {
		boolean shouldResume = false;
		boolean force = false;
		int numTasks = 0;
		if (args.length < 1) {
			System.err.println("Usage: ParserJob [-crawlId <id>] [-resume] [-batch batchId] [-numTasks N] [-toindex] [-index url]");
			return -1;
		}
		String solrUrl = getConf().get(SolrConstants.SEGMENT_URL, null);
		if (solrUrl != null) {
			getConf().set(SolrConstants.SERVER_URL, solrUrl);
		}
		for (int i = 0; i < args.length; i++) {
			if ("-resume".equals(args[i])) {
				shouldResume = true;
			} else if ("-crawlId".equals(args[i])) {
				getConf().set(Nutch.CRAWL_ID_KEY, args[++i]);
			} else if ("-toindex".equals(args[i])) {
				getConf().setBoolean(SegParserJob.SEGMENT_INDEX_KEY, true);
			} else if ("-numTasks".equals(args[i])) {
				numTasks = Integer.parseInt(args[++i]);
			} else if ("-batch".equals(args[i])) {
				String batchId1 = org.apache.commons.lang.StringUtils.lowerCase(args[i + 1]);
				if (batchId1 != null && !batchId1.equals("")) {
					getConf().set(NutchConstant.BATCH_ID_KEY, batchId1);
				}
				i++;
			} else if ("-index".equals(args[i])) {
				i++;
				getConf().set(SolrConstants.SERVER_URL, args[i]);
			}
		}

		NutchIndexWriterFactory.addClassToConf(getConf(), SolrWriter.class);
		if (getConf().get(NutchConstant.BATCH_ID_KEY, null) == null) {
			throw new Exception("args error no -batch label or value");
		}
		return parse(shouldResume, force, numTasks);
	}

	public static void main(String[] args) throws Exception {
		final int res = ToolRunner.run(NutchConfiguration.create(), new SegParserAllIndexJob(), args);
		System.exit(res);
	}
}
