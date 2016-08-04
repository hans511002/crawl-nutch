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

import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.storage.WebPageSegment;
import org.apache.nutch.util.TableUtil;
import org.apache.solr.common.util.DateUtil;

/**
 * Utility to create an indexed document from a webpage.
 * 
 */
public class SegmentSolrIndexUtil {
	private static final Log LOG = LogFactory.getLog(SegmentSolrIndexUtil.class);
	private static java.text.SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	public SegmentSolrIndexUtil(Configuration conf) {

	}

	public static String toString(Utf8 utf8) {
		return (utf8 == null ? null : utf8.toString());
	}

	/**
	 * Index a webpage.
	 * 
	 * @param key
	 *            The key of the page (reversed url).
	 * @param page
	 *            The webpage.
	 * @return The indexed document, or null if skipped by index filters.
	 */
	public static NutchDocument index(String key, WebPageSegment page) {
		NutchDocument doc = new NutchDocument();
		String url = TableUtil.unreverseUrl(key);
		if (page.getTitle() == null || page.getConfigUrl() == null)
			return null;
		doc.add("rowkey", key);
		float boost = page.getScore();
		if (boost < 0.1)
			boost = 1.0f;
		doc.setScore(boost);
		// store boost for use by explain and dedup
		doc.add("boost", Float.toString(boost));
		doc.add("rootSite", page.getRootSiteId() + "");
		doc.add("mdType", page.getMediaTypeId() + "");
		doc.add("mdLevel", page.getMediaLevelId() + "");
		doc.add("topic", page.getTopicTypeId() + "");
		doc.add("polictic", page.getPolicticTypeId() + "");
		doc.add("areaId", page.getAreaId() + "");

		String host = null;
		try {
			URL u = new URL(url);
			host = u.getHost();
		} catch (MalformedURLException e) {
			LOG.warn("Error indexing " + key + ": " + e);
			return null;
		}
		if (host != null) {
			// add host as un-stored, indexed and tokenized
			doc.add("host", host);
			// add site as un-stored, indexed and un-tokenized
			doc.add("site", host);
		}
		// url is both stored and indexed, so it's both searchable and returned
		doc.add("url", url);
		doc.add("cfgurl", page.getConfigUrl().toString());
		String tstamp = DateUtil.getThreadLocalDateFormat().format(new Date(page.getFetchTime()));
		doc.add("fetchTime", tstamp);
		doc.add("fTime", tstamp);
		tstamp = DateUtil.getThreadLocalDateFormat().format(new Date(page.getParseTime()));
		doc.add("parseTime", tstamp);
		doc.add("pTime", tstamp);
		tstamp = DateUtil.getThreadLocalDateFormat().format(new Date(page.getDataTime()));
		doc.add("dataTime", tstamp);
		doc.add("dTime", tstamp);
		doc.add("title", page.getTitle().toString());

		Map<Utf8, Utf8> segCnt = page.getSegMentCnt();
		// doc.add("keywords", segCnt.get(WebPageSegment.keyworksColName).toString());
		// doc.add("description", segCnt.get(WebPageSegment.descriptionColName).toString());
		for (Utf8 cntkey : segCnt.keySet()) {
			String colName = cntkey.toString();
			if (colName.endsWith("list")) {//
				continue;
			}
			Utf8 v = segCnt.get(cntkey);
			if (v == null)
				continue;
			String val = v.toString();
			if (colName.endsWith("time")) {
				try {
					tstamp = DateUtil.getThreadLocalDateFormat().format(df.parse(val));
					doc.add("cnt_" + colName.replace("time", "_time"), tstamp);
				} catch (Exception e) {
					doc.add("cnt_" + colName, val);
				}
			} else {
				if (cntkey.equals(WebPageSegment.keyworksColName)) {
					doc.add("keywords", val);
				} else if (cntkey.equals(WebPageSegment.descriptionColName)) {
					doc.add("description", val);
				} else {
					doc.add("cnt_" + colName, val.replaceAll("(?i)(<p>|</p>)", ""));
				}
			}
		}
		Map<Utf8, Utf8> segExInfo = page.getExtendInfoAttrs();
		for (Utf8 exInfokey : segExInfo.keySet()) {
			String colName = exInfokey.toString();
			Utf8 v = segExInfo.get(exInfokey);
			if (v == null)
				continue;
			String val = v.toString();
			doc.add("exinfo_" + colName, val);
		}
		// segmeng_col-segmeng_attr_col:value
		Map<Utf8, Utf8> segMentAttr = page.getSegMentAttr();
		for (Utf8 exInfokey : segMentAttr.keySet()) {
			String colName = exInfokey.toString();
			Utf8 v = segMentAttr.get(exInfokey);
			if (v == null)
				continue;
			String val = v.toString();
			doc.add("cntattr_" + colName, val);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Indexing URL: " + url);
		}
		return doc;
	}
}
