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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageIndex;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.StringUtil;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParserJob extends NutchTool implements Tool {

	public static final Logger LOG = LoggerFactory.getLogger(ParserJob.class);

	public static final String RESUME_KEY = "parse.job.resume";
	public static final String FORCE_KEY = "parse.job.force";

	public static final String SKIP_TRUNCATED = "parser.skip.truncated";

	public static final Utf8 REPARSE = new Utf8("-reparse");

	private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

	private Configuration conf;

	static {
		FIELDS.add(WebPage.Field.BASE_URL);
		FIELDS.add(WebPage.Field.STATUS);
		FIELDS.add(WebPage.Field.FETCH_TIME);
		FIELDS.add(WebPage.Field.PREV_FETCH_TIME);
		FIELDS.add(WebPage.Field.FETCH_INTERVAL);
		FIELDS.add(WebPage.Field.RETRIES_SINCE_FETCH);
		FIELDS.add(WebPage.Field.MODIFIED_TIME);
		FIELDS.add(WebPage.Field.PROTOCOL_STATUS);
		FIELDS.add(WebPage.Field.CONTENT);
		FIELDS.add(WebPage.Field.CONTENT_TYPE);
		FIELDS.add(WebPage.Field.PREV_SIGNATURE);
		FIELDS.add(WebPage.Field.SIGNATURE);
		// FIELDS.add(WebPage.Field.TITLE);
		// FIELDS.add(WebPage.Field.TEXT);
		// FIELDS.add(WebPage.Field.PARSE_STATUS);
		// FIELDS.add(WebPage.Field.SCORE);
		// FIELDS.add(WebPage.Field.REPR_URL);
		FIELDS.add(WebPage.Field.HEADERS);
		// FIELDS.add(WebPage.Field.OUTLINKS);
		// FIELDS.add(WebPage.Field.INLINKS);
		FIELDS.add(WebPage.Field.MARKERS);
		FIELDS.add(WebPage.Field.METADATA);
		FIELDS.add(WebPage.Field.CRAWLTYPE);
		FIELDS.add(WebPage.Field.CONFIGURL);

	}

	public static class ParserMapper extends GoraMapper<String, WebPageIndex, Text, WebPageIndex> {
		// private ParseUtil parseUtil;
		// private boolean shouldResume;
		// private boolean force;
		// private Utf8 batchId;
		// private boolean skipTruncated;
		private String batchID;
		private String batchTime;
		int rowCount = 0;

		// String indexKey;

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			// parseUtil = new ParseUtil(conf);
			// shouldResume = conf.getBoolean(RESUME_KEY, false);
			// force = conf.getBoolean(FORCE_KEY, false);
			// batchId = new Utf8(conf.get(GeneratorJob.BATCH_ID, Nutch.ALL_BATCH_ID_STR));
			// skipTruncated = conf.getBoolean(SKIP_TRUNCATED, true);
			batchID = NutchConstant.getBatchId(conf);
			batchTime = NutchConstant.getBatchTime(conf);
			LOG.info("parserMap-批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
			NutchConstant.setupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.parseNode, context, true);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			LOG.info("parserMap-批次ID：" + batchID + "  退出时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
			context.setStatus("总共需要解析的网页数：" + rowCount);
			// context.setStatus(getParseStatus(context));
			// LOG.info(getParseStatus(context));
			NutchConstant.cleanupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.parseNode, context
					.getTaskAttemptID().getTaskID().toString(), true);
		}

		// String getParseStatus(Context context) {
		// String res = "parser result:";
		// for (String code : ParseStatusCodes.majorCodes) {
		// org.apache.hadoop.mapreduce.Counter counter = context.getCounter("ParserStatus", code);
		// res += (" " + counter.getName() + "=" + counter.getValue()) + "\n";
		// }
		// return res;
		// }

		@Override
		public void map(String key, WebPageIndex page, Context context) throws IOException, InterruptedException {
			key = NutchConstant.getWebPageUrl(batchID, key);
			if (key == null)
				return;
			// boolean shoulParse = true;
			// Utf8 mark = Mark.FETCH_MARK.checkMark(page);
			// String unreverseKey = TableUtil.unreverseUrl(key);
			// if (page.getStatus() != CrawlStatus.STATUS_FETCHED)
			// return;
			// if (batchId.equals(REPARSE)) {
			// LOG.debug("Reparsing " + unreverseKey);
			// } else {
			// if (!NutchJob.shouldProcess(mark, batchId)) {
			// LOG.warn("Skipping " + unreverseKey + "; different batch id (" + mark + ")");
			// } else if (shouldResume && Mark.PARSE_MARK.checkMark(page) != null) {
			// if (force) {
			// LOG.info("Forced parsing " + unreverseKey + "; already parsed");
			// } else {
			// LOG.info("Skipping " + unreverseKey + "; already parsed");
			// shoulParse = false;
			// }
			// } else {
			// LOG.info("Parsing " + unreverseKey);
			// }
			// }
			//
			// if (skipTruncated && isTruncated(unreverseKey, page)) {
			// LOG.info("skipTruncated " + unreverseKey);
			// shoulParse = false;
			// }
			// if (shoulParse)
			// parseUtil.process(key, page);
			// // StateManager stateManager = page.getStateManager();
			// // System.err.println("ParserMapper parseUtil.process:" + stateManager.isDirty(page, 13));
			// // System.err.println(" url=" + key + "  ParserMapper line:167 page=" + page);
			// ParseStatus pstatus = page.getParseStatus();
			// if (pstatus != null) {
			// context.getCounter("ParserStatus", ParseStatusCodes.majorCodes[pstatus.getMajorCode()]).increment(1);
			// } else {
			// context.getCounter("ParserStatus", "failed").increment(1);
			// LOG.warn("rowkey:" + key + "  url:" + unreverseKey);
			// }
			// if (rowCount % 100 == 0) {
			// context.setStatus(getParseStatus(context));
			// }
			System.out.println(key + "===url:" + TableUtil.unreverseUrl(key));
			rowCount++;
			context.write(new Text(key), page);
		}
	}

	public ParserJob() {

	}

	public ParserJob(Configuration conf) {
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
		if (NutchConstant.preparStartJob(this.getConf(), NutchConstant.BatchNode.parseNode, NutchConstant.BatchNode.fetchNode, LOG, false) == 0)
			return null;

		LOG.info("ParserJob: batchId: " + batchZKId);
		LOG.info("ParserJob: resuming:\t" + getConf().getBoolean(RESUME_KEY, false));
		LOG.info("ParserJob: forced reparse:\t" + getConf().getBoolean(FORCE_KEY, false));
		if (batchId == null || batchId.equals(Nutch.ALL_BATCH_ID_STR)) {
			LOG.info("ParserJob: parsing all");
		} else {
			LOG.info("ParserJob: batchId:\t" + batchId);
		}
		String gids = NutchConstant.getGids(getConf(), "all");
		currentJob = new NutchJob(getConf(), "[" + (this.getConf().get(NutchConstant.BATCH_ID_KEY)) + "]parse[" + gids + "]");
		Collection<WebPage.Field> fields = getFields(currentJob);
		StorageUtils.initMapperJob(currentJob, fields, WebPageIndex.class, Text.class, WebPageIndex.class, ParserMapper.class);
		StorageUtils.initReducerJob(currentJob, WebPageIndex.class, ParserReducer.class);
		currentJob.setNumReduceTasks(this.getConf().getInt("mapred.reduce.tasks", currentJob.getNumReduceTasks()));
		currentJob.waitForCompletion(true);
		NutchConstant.preparEndJob(this.getConf(), NutchConstant.BatchNode.parseNode, LOG);
		ToolUtil.recordJobStatus(null, currentJob, results);
		return results;
	}

	public int parse(String batchId, boolean shouldResume, boolean force) throws Exception {
		LOG.info("ParserJob: starting");
		try {
			Map<String, Object> res = run(ToolUtil.toArgMap(Nutch.ARG_BATCH, batchId, Nutch.ARG_RESUME, shouldResume, Nutch.ARG_FORCE,
					force));
			LOG.info("ParserJob: success");
			if (res == null)
				return -1;
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
		boolean shouldResume = false;
		boolean force = false;
		String batchId = null;

		if (args.length < 1) {
			System.err.println("Usage: ParserJob (<batchId> | -all) [-crawlId <id>] [-resume] [-force] [-numTasks N] [-batch batchId]  ");
			System.err.println("    <batchId>     - symbolic batch ID created by Generator");
			System.err.println("    -crawlId <id> - the id to prefix the schemas to operate on, \n \t \t    (default: storage.crawl.id)");
			System.err.println("    -all          - consider pages from all crawl jobs");
			System.err.println("    -resume       - resume a previous incomplete job");
			System.err.println("    -force        - force re-parsing even if a page is already parsed");
			return -1;
		}
		for (int i = 0; i < args.length; i++) {
			if ("-resume".equals(args[i])) {
				shouldResume = true;
			} else if ("-force".equals(args[i])) {
				force = true;
				getConf().setBoolean(NutchConstant.STEP_FORCE_RUN_KEY, true);
			} else if ("-crawlId".equals(args[i])) {
				getConf().set(Nutch.CRAWL_ID_KEY, args[++i]);
			} else if ("-all".equals(args[i])) {
				batchId = args[i];
			} else if ("-numTasks".equals(args[i])) {
				int numTasks = Integer.parseInt(args[++i]);
				this.getConf().setInt("mapred.reduce.tasks", numTasks);
			} else if ("-batch".equals(args[i])) {
				String batchId1 = org.apache.commons.lang.StringUtils.lowerCase(args[i + 1]);
				if (batchId1 != null && !batchId1.equals("")) {
					getConf().set(NutchConstant.BATCH_ID_KEY, batchId1);
				}
				i++;
			} else {
				if (batchId != null) {
					System.err.println("BatchId already set to '" + batchId + "'!");
					return -1;
				}
				batchId = args[i];
			}
		}

		if (getConf().get(NutchConstant.BATCH_ID_KEY, null) == null) {
			throw new Exception("args error no -batch label or value");
		}
		if (batchId == null) {
			// System.err.println("BatchId not set (or -all/-reparse not specified)!");
			// return -1;
			batchId = "-all";
		}
		return parse(batchId, shouldResume, force);
	}

	public static void main(String[] args) throws Exception {
		final int res = ToolRunner.run(NutchConfiguration.create(), new ParserJob(), args);
		System.exit(res);
	}
}
