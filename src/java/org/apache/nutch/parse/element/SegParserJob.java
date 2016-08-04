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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraMapper;
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
import org.apache.solr.common.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegParserJob extends NutchTool implements Tool {

	public static final Logger LOG = LoggerFactory.getLogger(SegParserJob.class);

	private static final String FORCE_KEY = "parse.job.force";
	public static final String SEGMENT_INDEX_KEY = "segment.parse.job.index";

	public static final String SKIP_TRUNCATED = "parser.skip.truncated";

	private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

	private Configuration conf;

	static {
		FIELDS.add(WebPage.Field.TEXT);
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

	public static class SegParserMapper extends GoraMapper<String, WebPageIndex, Text, WebPageIndex> {
		private String batchID;
		private String batchTime;
		int rowCount = 0;
		int successCount = 0;

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			batchID = NutchConstant.getBatchId(conf);
			batchTime = NutchConstant.getBatchTime(conf);
			LOG.info("parserMap-批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
			NutchConstant.setupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.segmentParsNode, context, true);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
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
			key = NutchConstant.getWebPageUrl(batchID, key);
			if (key == null)
				return;
			if (page.getContent() == null) {
				System.err.println("内容为空：" + TableUtil.unreverseUrl(key));
				return;
			} else if (page.getTitle() == null) {
				System.err.println("标题为空：" + TableUtil.unreverseUrl(key));
				return;
			} else if (page.getText() == null) {
				System.err.println("文本为空：" + TableUtil.unreverseUrl(key));
				return;
			} else if (page.getConfigUrl() == null) {
				System.err.println("配置地址为空：" + TableUtil.unreverseUrl(key));
				return;
			}
			if (rowCount % 1000 == 0) {
				context.setStatus(getParseStatus(context));
			}
			successCount++;
			context.write(new Text(key), page);
		}
	}

	public SegParserJob() {

	}

	public SegParserJob(Configuration conf) {
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

	public static Collection<WebPage.Field> getFields(Job job) {
		Configuration conf = job.getConfiguration();
		Collection<WebPage.Field> fields = new HashSet<WebPage.Field>(FIELDS);
		ParserFactory parserFactory = new ParserFactory(conf);
		ParseFilters parseFilters = new ParseFilters(conf);
		Collection<WebPage.Field> parsePluginFields = parserFactory.getFields();
		Collection<WebPage.Field> htmlParsePluginFields = parseFilters.getFields();
		SegMentParsers segmentParses = new SegMentParsers(conf);
		Collection<WebPage.Field> segmentParsePluginFields = segmentParses.getFields(job);
		if (parsePluginFields != null) {
			fields.addAll(parsePluginFields);
		}
		if (htmlParsePluginFields != null) {
			fields.addAll(htmlParsePluginFields);
		}
		if (segmentParsePluginFields != null) {
			fields.addAll(segmentParsePluginFields);
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
		Boolean force = (Boolean) args.get(Nutch.ARG_FORCE);

		if (force != null) {
			getConf().setBoolean(FORCE_KEY, force);
		}
		String batchZKId = this.getConf().get(NutchConstant.BATCH_ID_KEY);
		if (this.getConf().getBoolean(NutchConstant.noSolrIndexNode, false)) {
			if (NutchConstant.preparStartJob(this.getConf(), NutchConstant.BatchNode.segmentParsNode, NutchConstant.BatchNode.dbUpdateNode,
					LOG, false) == 0)
				return null;
		} else {
			if (NutchConstant.preparStartJob(this.getConf(), NutchConstant.BatchNode.segmentParsNode,
					NutchConstant.BatchNode.solrIndexNode, LOG, false) == 0)
				return null;
		}
		LOG.info("SegmentParserJob: batchId: " + batchZKId);
		LOG.info("SegmentParserJob: forced reparse:\t" + getConf().getBoolean(FORCE_KEY, false));
		String gids = NutchConstant.getGids(getConf(), "all");
		currentJob = new NutchJob(getConf(), "[" + (this.getConf().get(NutchConstant.BATCH_ID_KEY)) + "]segmentParse[" + gids + "]");
		Collection<WebPage.Field> fields = getFields(currentJob);
		NutchConstant.setSegmentParseRules(currentJob.getConfiguration());
		StorageUtils.initMapperJob(currentJob, fields, WebPageIndex.class, Text.class, WebPageIndex.class, SegParserMapper.class);
		StorageUtils.initReducerJob(currentJob, WebPageSegment.class, SegParserReducer.class);

		currentJob.waitForCompletion(true);
		NutchConstant.preparEndJob(this.getConf(), NutchConstant.BatchNode.segmentParsNode, LOG);
		ToolUtil.recordJobStatus(null, currentJob, results);
		return results;
	}

	public int parse(String solrUrl, boolean force) throws Exception {
		try {
			LOG.info("ParserJob: starting");
			run(ToolUtil.toArgMap(Nutch.ARG_SOLR, solrUrl, Nutch.ARG_FORCE, force));
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
			System.err.println("Usage: ParserJob <-batch batchId> [-toindex] [-index solr_url] [-crawlId <id>] [-force]");
			return -1;
		}
		String solrUrl = getConf().get(SolrConstants.SEGMENT_URL, null);
		if (solrUrl != null) {
			getConf().set(SolrConstants.SERVER_URL, solrUrl);
		}
		for (int i = 0; i < args.length; i++) {
			if ("-force".equals(args[i])) {
				force = true;
				getConf().setBoolean(NutchConstant.STEP_FORCE_RUN_KEY, true);
			} else if ("-crawlId".equals(args[i])) {
				getConf().set(Nutch.CRAWL_ID_KEY, args[++i]);
			} else if ("-toindex".equals(args[i])) {
				getConf().setBoolean(SEGMENT_INDEX_KEY, true);
			} else if ("-batch".equals(args[i])) {
				String batchId1 = org.apache.commons.lang.StringUtils.lowerCase(args[i + 1]);
				if (batchId1 != null && !batchId1.equals("")) {
					getConf().set(NutchConstant.BATCH_ID_KEY, batchId1);
				}
				i++;
			} else if ("-index".equals(args[i])) {
				i++;
				solrUrl = args[i];
				getConf().set(SolrConstants.SERVER_URL, args[i]);
			}
		}
		NutchIndexWriterFactory.addClassToConf(getConf(), SolrWriter.class);

		if (getConf().get(NutchConstant.BATCH_ID_KEY, null) == null) {
			throw new Exception("args error no -batch label or value");
		}
		return parse(solrUrl, force);
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
			conf.set(Nutch.CRAWL_ID_KEY, "ea");
			// if (conf != null)
			// return;
			conf.set("hbase.client.scanner.caching", "10");
			conf.set(SolrConstants.SERVER_URL, conf.get(SolrConstants.SEGMENT_URL));
			// conf.set(SolrConstants.SERVER_URL, "http://192.168.10.101:8080/solr/ea");
			DataStore<String, WebPageIndex> store = StorageUtils.createWebStore(conf, String.class, WebPageIndex.class);
			if (store == null)
				throw new RuntimeException("Could not create datastore");
			Query<String, WebPageIndex> query = store.newQuery();
			if ((query instanceof Configurable)) {
				((Configurable) query).setConf(conf);
			}
			Job job = new NutchJob(conf, "segmentParse:");
			String batchId = "20130924164200";
			query.setFields(StorageUtils.toStringArray(SegParserJob.getFields(job)));
			query.setStartKey("20130924164200" + TableUtil.reverseUrl("http://cd.58.com/zufang/12925649137029x.shtml"));
			query.setEndKey("20130924164200" + TableUtil.reverseUrl("http://cd.58.com/zufanh"));
			// query.setEndKey("com.qq.news:http/a/20090313/001870.htm");
			NutchConstant.setSegmentParseRules(conf);
			NutchConstant.getSegmentParseRules(conf);
			SegMentParsers parses = new SegMentParsers(conf);
			Result<String, WebPageIndex> rs = query.execute();
			int cout = 0;
			DomParser parse = new DomParser();
			// FileInputStream fi = new FileInputStream("test");
			// byte[] b = new byte[128000];
			// fi.read(b);
			// fi.close();
			parse.setConf(conf);
			long l = System.currentTimeMillis();
			SolrWriter solrWriter = null;
			solrWriter = new SolrWriter();
			solrWriter.open(conf);
			while (rs.next()) {
				long sl = System.currentTimeMillis();
				WebPage page = rs.get();
				// System.err.println(page);
				// System.err.println();
				String key = rs.getKey();
				key = NutchConstant.getWebPageUrl(batchId, key);
				if (page.getContent() == null)
					continue;
				// key = NutchConstant.getWebPageUrl("1", key);
				// if (key == null)
				// continue;
				String unreverseKey = TableUtil.unreverseUrl(key);

				WebPageSegment wps = SegParserReducer.parseSegMent(parses, unreverseKey, page);// 有解析成功的数据返回
				System.err.println(wps);

				// System.out.println(unreverseKey + " 位置:" + cnt.indexOf("C-Main-Article-QQ"));
				// String ps = parse.getNodeValue(page.getContent().array(), "C-Main-Article-QQ");
				//
				// Node node = parse.getDomNode(b, "gb2312");
				// node = parse.getNodeById(node, "list_container_hot");
				// Node _node = parse.getNodeByTagPath(node, new String[][] { { "DT", "class", "clear" } });
				//
				// if (_node != null) {
				// System.out.println(parse.getNodeAttrValue(_node, "id"));
				// Node prevNode = _node.getPreviousSibling();
				// Node nextNode = _node.getNextSibling();
				// System.out.println(_node.getPrefix());
				// if (prevNode != null)
				// System.out.println(prevNode.getNodeName() + " value:" + parse.getNodeValue(prevNode));
				// if (nextNode != null)
				// System.out.println(nextNode.getNodeName() + " value:" + parse.getNodeValue(nextNode));
				// }
				// if (ps != null)
				// break;
				// System.out.println(ps);
				// /////////dom 测试 //////
				// if ((cout% 100) == 0)
				{
					System.out.println("第" + cout + "个 用时：" + (System.currentTimeMillis() - sl));
				}
				if (wps != null) {
					NutchDocument doc = SegmentSolrIndexUtil.index(key, wps);
					if (doc != null) {
						SegParserJob.LOG.info(key + " url:" + unreverseKey + " write to solr");
						solrWriter.write(doc);
					} else {
						SegParserJob.LOG.warn(key + " url:" + unreverseKey + " doc is null");
					}
				}
				cout++;
			}
			solrWriter.close();
			System.out.println(cout + "个 用时：" + (System.currentTimeMillis() - l));
			System.exit(0);
		} else {
			final int res = ToolRunner.run(NutchConfiguration.create(), new SegParserJob(), args);
			System.exit(res);
		}

	}
}
