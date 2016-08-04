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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;

public class ParserAllReducer extends GoraReducer<Text, WebPage, String, WebPage> {
	private String batchID;
	private String batchTime;

	@Override
	protected void reduce(Text key, Iterable<WebPage> values, Context context) throws IOException, InterruptedException {
		for (WebPage page : values) {
			Mark.INJECT_MARK.removeMarkIfExist(page);
			Mark.GENERATE_MARK.removeMarkIfExist(page);
			Mark.FETCH_MARK.removeMarkIfExist(page);
			Mark.PARSE_MARK.removeMarkIfExist(page);
			Mark.UPDATEDB_MARK.removeMarkIfExist(page);
			Mark.INDEX_MARK.removeMarkIfExist(page);
			Mark.BETCH_MARK.removeMarkIfExist(page);
			page.putToMarkers(new Utf8("_pa_"), new Utf8(batchID));
			context.write(key.toString(), page);
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		FetcherJob.LOG.info("parserAllReducer-批次ID：" + batchID + "  退出时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
		NutchConstant.cleanupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.parseNode, context.getTaskAttemptID()
				.getTaskID().toString(), false);
	};

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		batchID = NutchConstant.getBatchId(conf);
		batchTime = NutchConstant.getBatchTime(conf);
		FetcherJob.LOG.info("parserAllReducer-批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);
		NutchConstant.setupSerialStepProcess(context.getConfiguration(), NutchConstant.BatchNode.parseNode, context, false);
	}
}
