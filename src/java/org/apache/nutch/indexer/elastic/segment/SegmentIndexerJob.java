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
package org.apache.nutch.indexer.elastic.segment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.mapreduce.GoraReducer;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchIndexWriterFactory;
import org.apache.nutch.indexer.elastic.ElasticConstants;
import org.apache.nutch.indexer.elastic.ElasticWriter;
import org.apache.nutch.indexer.solr.SolrConstants;
import org.apache.nutch.indexer.solr.SolrWriter;
import org.apache.nutch.indexer.solr.segment.SegmentSolrIndexUtil;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.element.DomParser;
import org.apache.nutch.parse.element.SegMentParsers;
import org.apache.nutch.parse.element.SegParserJob;
import org.apache.nutch.parse.element.SegParserReducer;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;
import org.apache.nutch.storage.WebPageSegmentIndex;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.ToolUtil;
import org.apache.solr.common.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentIndexerJob extends NutchTool implements Tool {

	public static final Logger LOG = LoggerFactory.getLogger(SegmentIndexerJob.class);

	private static final String FORCE_KEY = "parse.job.force";
	public static final String SEGMENT_INDEX_KEY = "segment.parse.job.index";

	public static final String SKIP_TRUNCATED = "parser.skip.truncated";
	private static final Collection<WebPageSegment.Field> FIELDS = new HashSet<WebPageSegment.Field>();

	private Configuration conf;
	static {
		FIELDS.add(WebPageSegment.Field.BASE_URL);
		FIELDS.add(WebPageSegment.Field.CONFIGURL);
		FIELDS.add(WebPageSegment.Field.SCORE);
		FIELDS.add(WebPageSegment.Field.FETCH_TIME);
		FIELDS.add(WebPageSegment.Field.PARSETIME);
		FIELDS.add(WebPageSegment.Field.DATATIME);
		FIELDS.add(WebPageSegment.Field.TITLE);
		FIELDS.add(WebPageSegment.Field.ROOTSITEID);
		FIELDS.add(WebPageSegment.Field.MEDIATYPEID);
		FIELDS.add(WebPageSegment.Field.MEDIALEVELID);
		FIELDS.add(WebPageSegment.Field.TOPICTYPEID);
		FIELDS.add(WebPageSegment.Field.POLICTICTYPEID);
		FIELDS.add(WebPageSegment.Field.AREAID);
		FIELDS.add(WebPageSegment.Field.extendInfoAttrs);
		FIELDS.add(WebPageSegment.Field.SEGMENTATTR);
		FIELDS.add(WebPageSegment.Field.SEGMENTCNT);
		FIELDS.add(WebPageSegment.Field.MARKER);
	}

	public static Collection<WebPageSegment.Field> getFields(Job job) {
		return FIELDS;
	}

	public static class SegIndexReducer extends GoraReducer<Text, WebPageSegmentIndex, String, WebPageSegment> {

		public static final Logger LOG = LoggerFactory.getLogger(SegIndexReducer.class);
		int rowCount = 0;
		int successCount = 0;
		int failCount = 0;
		private String batchID;
		private String batchTime;
		ElasticWriter elasticWriter = null;
		boolean indexed = false;
		private int commitSize;
		long l = 0;

		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			batchID = NutchConstant.getBatchId(conf);
			batchTime = NutchConstant.getBatchTime(conf);
			elasticWriter = new ElasticWriter();
			indexed = conf.getBoolean(SegParserJob.SEGMENT_INDEX_KEY, false);
			elasticWriter.open(context);
			LOG.info("parserReduce-批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
			commitSize = conf.getInt(ElasticConstants.COMMIT_SIZE, 300);
			NutchConstant.setupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.segmentIndexNode, context, false);
			l = System.currentTimeMillis();
		}

		protected void reduce(Text key, Iterable<WebPageSegmentIndex> values, Context context) throws IOException, InterruptedException {
			String urlKey = key.toString();
			for (WebPageSegment page : values) {
				rowCount++;
				context.setStatus(getParseStatus(context));
				NutchDocument doc = SegmentSolrIndexUtil.index(urlKey, page);
				if (doc == null) {
					failCount++;
					page.setMarker(null);
					context.write(urlKey, page);// 回写
					LOG.error(key + "   doc is null");
					return;
				} else {
					successCount++;
					long ls = System.currentTimeMillis();
					if ((successCount % commitSize) == 0) {
						System.out.println("解析" + commitSize + "条，用时：" + (ls - l) + "ms");
						l = ls;
					}
					elasticWriter.write(doc);
					page.setMarker(new Utf8(batchID + ":" + batchTime));
					context.write(urlKey, page);// 回写
					if ((successCount % commitSize) == 0) {
						ls = System.currentTimeMillis();
						System.out.println("提交" + commitSize + "条，用时：" + (ls - l) + "ms");
						l = ls;
					}
				}
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			elasticWriter.close();
			LOG.info("parserReduce-批次ID：" + batchID + "  退出时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
			context.setStatus(getParseStatus(context));
			LOG.info(getParseStatus(context));
			NutchConstant.cleanupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.segmentIndexNode, context
					.getTaskAttemptID().getTaskID().toString(), false, true, successCount);
		}

		String getParseStatus(Context context) {
			return "read:" + rowCount + " Success:" + (rowCount - failCount) + "  " + " failed:" + failCount + " <br/>更新时间："
					+ (new Date().toLocaleString());
		}

	}

	public static class SegIndexMapper extends GoraMapper<String, WebPageSegmentIndex, Text, WebPageSegmentIndex> {
		private String batchID;
		private String batchTime;
		int rowCount = 0;
		int failCount = 0;
		long l = 0, all = 0;

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			batchID = NutchConstant.getBatchId(conf);
			batchTime = NutchConstant.getBatchTime(conf);
			LOG.info("parserMap-批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
			NutchConstant.setupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.segmentIndexNode, context, true);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			LOG.info("parserMap-批次ID：" + batchID + "  退出时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
			context.setStatus(getParseStatus(context));
			LOG.info(getParseStatus(context));
			NutchConstant.cleanupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.segmentIndexNode, context
					.getTaskAttemptID().getTaskID().toString(), true, true, rowCount - failCount);
		}

		String getParseStatus(Context context) {
			return "write:" + rowCount;
		}

		@Override
		public void map(String key, WebPageSegmentIndex page, Context context) throws IOException, InterruptedException {
			if (page.getTitle() == null)
				return;
			key = NutchConstant.getWebPageUrl(batchID, key);
			if (key == null)
				return;
			rowCount++;
			if (rowCount % 10 == 0) {
				context.setStatus(getParseStatus(context));
			}
			context.write(new Text(key), page);
		}
	}

	public SegmentIndexerJob() {

	}

	public SegmentIndexerJob(Configuration conf) {
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
		Boolean force = (Boolean) args.get(Nutch.ARG_FORCE);
		Integer numTasks = (Integer) args.get(Nutch.ARG_NUMTASKS);

		if (force != null) {
			getConf().setBoolean(FORCE_KEY, force);
		}
		String batchZKId = this.getConf().get(NutchConstant.BATCH_ID_KEY);
		if (NutchConstant.preparStartJob(this.getConf(), NutchConstant.BatchNode.segmentIndexNode, NutchConstant.BatchNode.segmentParsNode,
				LOG, false) == 0)
			return null;
		LOG.info("SegmentParserJob: batchId: " + batchZKId);
		LOG.info("SegmentParserJob: forced reparse:\t" + getConf().getBoolean(FORCE_KEY, false));

		NutchIndexWriterFactory.addClassToConf(getConf(), ElasticWriter.class);
		String gids = NutchConstant.getGids(getConf(), "all");
		currentJob = new NutchJob(getConf(), "[" + (this.getConf().get(NutchConstant.BATCH_ID_KEY)) + "]segmentIndex[" + gids + "]");
		currentJob.getConfiguration().set(NutchConstant.STEPZKBATCHTIME, new Date().toLocaleString());
		Collection<WebPageSegment.Field> fields = SegmentIndexerJob.getFields(currentJob);
		NutchConstant.setSegmentParseRules(currentJob.getConfiguration());
		WebPageSegment.initMapperJob(currentJob, fields, WebPageSegmentIndex.class, Text.class, WebPageSegmentIndex.class,
				SegIndexMapper.class, null, true);
		StorageUtils.initReducerJob(currentJob, WebPageSegment.class, SegIndexReducer.class);

		if (numTasks == null || numTasks < 1) {
			currentJob.setNumReduceTasks(currentJob.getConfiguration().getInt("mapred.reduce.tasks", currentJob.getNumReduceTasks()));
		} else {
			currentJob.setNumReduceTasks(numTasks);
		}
		
		currentJob.waitForCompletion(true);
		NutchConstant.preparEndJob(this.getConf(), NutchConstant.BatchNode.segmentIndexNode, LOG);
		ToolUtil.recordJobStatus(null, currentJob, results);
		return results;
	}

	public int indexElastic(String elasticUrl, boolean force, int numTasks) throws Exception {
		try {
			LOG.info("ParserJob: starting");
			run(ToolUtil.toArgMap(Nutch.ARG_SOLR, elasticUrl, Nutch.ARG_FORCE, force, Nutch.ARG_NUMTASKS, numTasks));
			LOG.info("ParserJob: success");
		} catch (Exception e) {
			if (NutchConstant.exitValue != 0) {
				return NutchConstant.exitValue;
			} else {
				throw e;
			}
		}
		return 0;
	}

	public int run(String[] args) throws Exception {
		boolean force = false;
		if (args.length < 1) {
			System.err.println("Usage: SegmentIndexJob <-batch batchId>  [-index solr_url] [-crawlId <id>] [-force]");
			return -1;
		}
		int numTasks = 0;
		String elasticUrl = getConf().get(SolrConstants.SEGMENT_URL, null);
		if (elasticUrl != null) {
			getConf().set(ElasticConstants.SERVER_URL, elasticUrl);
		}
		for (int i = 0; i < args.length; i++) {
			if ("-numTasks".equals(args[i])) {
				numTasks = Integer.parseInt(args[++i]);
			} else if ("-force".equals(args[i])) {
				force = true;
				getConf().setBoolean(NutchConstant.STEP_FORCE_RUN_KEY, true);
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
				elasticUrl = args[i];
				getConf().set(ElasticConstants.SERVER_URL, args[i]);
			}
		}

		if (getConf().get(NutchConstant.BATCH_ID_KEY, null) == null) {
			throw new Exception("args error no -batch label or value");
		}
		return indexElastic(elasticUrl, force, numTasks);
	}

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			String strDt = "2013-07-29 10:14:00".toString();
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			String tstamp = DateUtil.getThreadLocalDateFormat().format(new Date(df.parse(strDt).getTime()));
			String ststamp = DateUtil.getThreadLocalDateFormat().format(new Date(df.parse(strDt).getTime()));
			System.err.println(strDt);
			System.err.println(tstamp);
			System.err.println(ststamp);

			Configuration conf = NutchConfiguration.create();
			if (conf == null)
				return;
			conf.set("elastic.index", "ea");
			conf.set("storage.crawl.id", "ea");
			conf.set("hbase.client.scanner.caching", "1");
//			conf.set(SolrConstants.SERVER_URL, conf.get(SolrConstants.SEGMENT_URL));
//			conf.set(SolrConstants.SERVER_URL, "http://localhost:8080/solr/thematic");
			DataStore<String, WebPage> store = StorageUtils.createWebStore(conf, String.class, WebPage.class);
			if (store == null)
				throw new RuntimeException("Could not create datastore");
			Query<String, WebPage> query = store.newQuery();
			if ((query instanceof Configurable)) {
				((Configurable) query).setConf(conf);
			}
			Job job = new NutchJob(conf, "segmentParse:");
//			query.setFields(StorageUtils.toStringArray(getFields(job)));
			query.setStartKey("com.soufun.jiahecheng0771:http/house/2910117344/housedetail.htm");
			// query.setEndKey("com.qq.news:http/a/20090313/001870.htm");
			NutchConstant.setSegmentParseRules(conf);
			NutchConstant.getSegmentParseRules(conf);
			SegMentParsers parses = new SegMentParsers(conf);
			Result<String, WebPage> rs = query.execute();
			int cout = 0;
			DomParser parse = new DomParser();
			parse.setConf(conf);
			long l = System.currentTimeMillis();
			ElasticWriter elasticWriter = new ElasticWriter();
			elasticWriter.open(conf);
			
			SolrWriter writer = new SolrWriter();
			writer.open(conf);
			while (rs.next()) {
				long sl = System.currentTimeMillis();
				WebPage page = rs.get();
				String key = rs.getKey();
				if (page.getContent() == null)
					continue;
				String unreverseKey = TableUtil.unreverseUrl(key);

				WebPageSegment wps = SegParserReducer.parseSegMent(parses, unreverseKey, page);// 有解析成功的数据返回
//				System.err.println(wps);
		
				System.err.println("第" + cout + "个 用时：" + (System.currentTimeMillis() - sl) + "  rowkey: " + key);
				if (wps != null) {
					NutchDocument doc = SegmentSolrIndexUtil.index(key, wps);
					if (doc != null) {
						SegmentIndexerJob.LOG.info(key + " url:" + unreverseKey + " write to elastic");
						elasticWriter.write(doc);
						writer.write(doc);
					} else {
						SegmentIndexerJob.LOG.warn(key + " url:" + unreverseKey + " doc is null");
					}
				}
				cout++;
			}
			elasticWriter.close();
			System.out.println("第" + cout + "个 用时：" + (System.currentTimeMillis() - l));
			System.exit(0);
		} else {
			final int res = ToolRunner.run(NutchConfiguration.create(), new SegmentIndexerJob(), args);
			System.exit(res);
		}

	}

}
