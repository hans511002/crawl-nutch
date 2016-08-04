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

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.UrlWithScore.UrlOnlyPartitioner;
import org.apache.nutch.crawl.UrlWithScore.UrlScoreComparator;
import org.apache.nutch.crawl.UrlWithScore.UrlScoreComparator.UrlOnlyComparator;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.ToolUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbUpdaterAllJob extends NutchTool implements Tool {

	public static final Logger LOG = LoggerFactory.getLogger(DbUpdaterAllJob.class);

	private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

	static {
		FIELDS.add(WebPage.Field.SCORE);
		FIELDS.add(WebPage.Field.STATUS);
		FIELDS.add(WebPage.Field.OUTLINKS);
		FIELDS.add(WebPage.Field.INLINKS);
		FIELDS.add(WebPage.Field.STATUS);
		FIELDS.add(WebPage.Field.FETCH_INTERVAL);
		FIELDS.add(WebPage.Field.PREV_FETCH_TIME);
		FIELDS.add(WebPage.Field.RETRIES_SINCE_FETCH);
		FIELDS.add(WebPage.Field.PREV_SIGNATURE);
		FIELDS.add(WebPage.Field.SIGNATURE);
		FIELDS.add(WebPage.Field.FETCH_TIME);
		FIELDS.add(WebPage.Field.MODIFIED_TIME);
		FIELDS.add(WebPage.Field.MARKERS);
		FIELDS.add(WebPage.Field.METADATA);
		FIELDS.add(WebPage.Field.PARSE_STATUS);

		FIELDS.add(WebPage.Field.CONFIGURL);
		FIELDS.add(WebPage.Field.CRAWLTYPE);
	}

	public static final Utf8 DISTANCE = new Utf8("dist");

	public DbUpdaterAllJob() {

	}

	public DbUpdaterAllJob(Configuration conf) {
		setConf(conf);
	}

	public Map<String, Object> run(Map<String, Object> args) throws Exception {
		String crawlId = (String) args.get(Nutch.ARG_CRAWL);
		numJobs = 1;
		currentJobNum = 0;
		currentJob = new NutchJob(getConf(), "updateAll-table:" + (this.getConf().get(NutchConstant.BATCH_ID_KEY)));
		if (crawlId != null) {
			currentJob.getConfiguration().set(Nutch.CRAWL_ID_KEY, crawlId);
		}
		// job.setBoolean(ALL, updateAll);
		ScoringFilters scoringFilters = new ScoringFilters(getConf());
		HashSet<WebPage.Field> fields = new HashSet<WebPage.Field>(FIELDS);
		fields.addAll(scoringFilters.getFields());

		// Partition by {url}, sort by {url,score} and group by {url}.
		// This ensures that the inlinks are sorted by score when they enter
		// the reducer.
		currentJob.getConfiguration().set(NutchConstant.STEPZKBATCHTIME, new Date().toLocaleString());
		currentJob.setPartitionerClass(UrlOnlyPartitioner.class);
		currentJob.setSortComparatorClass(UrlScoreComparator.class);
		currentJob.setGroupingComparatorClass(UrlOnlyComparator.class);
		String batchZKId = this.getConf().get(NutchConstant.BATCH_ID_KEY);
		NutchConstant.setUrlConfig(currentJob.getConfiguration(), 2);
		LOG.info("DbUpdaterAllJob: batchId: " + batchZKId + " batchTime:" + NutchConstant.getBatchTime(currentJob.getConfiguration()));
		LOG.info("DbUpdaterAllJob  batchId: " + batchZKId);
		StorageUtils.initMapperJob(currentJob, fields, WebPage.class, UrlWithScore.class, NutchWritable.class, DbUpdateAllMapper.class);
		StorageUtils.initReducerJob(currentJob, WebPage.class, DbUpdateAllReducer.class);
		currentJob.waitForCompletion(true);
		ToolUtil.recordJobStatus(null, currentJob, results);
		return results;
	}

	private int updateTable(String crawlId) throws Exception {
		try {
			LOG.info("DbUpdaterJob: starting");
			Object res = run(ToolUtil.toArgMap(Nutch.ARG_CRAWL, crawlId));
			LOG.info("DbUpdaterJob: done");
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
		String usage = "Usage: DbUpdaterJob  <-batch batchId> [-crawlId <id>] ";
		if (args.length == 0) {
			System.err.println(usage);
			return -1;
		}
		String crawlId = null;
		for (int i = 0; i < args.length; i++) {
			if ("-batch".equals(args[i])) {
				String batchId1 = org.apache.commons.lang.StringUtils.lowerCase(args[i + 1]);
				if (batchId1 != null && !batchId1.equals("")) {
					getConf().set(NutchConstant.BATCH_ID_KEY, batchId1);
				}
				i++;
			} else if ("-crawlId".equals(args[i])) {
				getConf().set(Nutch.CRAWL_ID_KEY, args[++i]);
			} else if ("-batch".equals(args[i])) {
				String batchId1 = org.apache.commons.lang.StringUtils.lowerCase(args[i + 1]);
				if (batchId1 != null && !batchId1.equals("")) {
					getConf().set(NutchConstant.BATCH_ID_KEY, batchId1);
				}
				i++;
			} else {
				throw new IllegalArgumentException("usage: " + "<-batch batchId> (-crawlId <id>) ");
			}
		}
		return updateTable(crawlId);
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(NutchConfiguration.create(), new DbUpdaterAllJob(), args);
		System.exit(res);
	}

}
