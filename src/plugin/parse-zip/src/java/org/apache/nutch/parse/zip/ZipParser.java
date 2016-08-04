/**
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
 */

package org.apache.nutch.parse.zip;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZipParser class based on MSPowerPointParser class by Stephan Strittmatter. Nutch parse plugin for zip files - Content Type :
 * application/zip
 * 
 * @author Rohit Kulkarni & Ashish Vaidya
 */
public class ZipParser implements org.apache.nutch.parse.Parser {

	private static final Logger LOG = LoggerFactory.getLogger(ZipParser.class);
	private Configuration conf;
	private static Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();
	static {
		FIELDS.add(WebPage.Field.BASE_URL);
	}

	/** Creates a new instance of ZipParser */
	public ZipParser() {
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public Parse getParse(String url, WebPage page) {
		Outlink[] outlinks = null;
		List<Outlink> outLinksList = new ArrayList<Outlink>();
		Properties properties = null;
		String text = "";
		String title = "";

		try {
			final String contentLen = new String(page.getMetadata().get(Metadata.CONTENT_LENGTH).array());
			final int len = Integer.parseInt(contentLen);
			if (LOG.isDebugEnabled()) {
				LOG.debug("ziplen: " + len);
			}
			final byte[] contentInBytes = page.getContent().array();
			final ByteArrayInputStream bainput = new ByteArrayInputStream(contentInBytes);
			final InputStream input = bainput;

			if (contentLen != null && contentInBytes.length != len) {
				return ParseStatusUtils.getEmptyParse(ParseStatusCodes.FAILED_TRUNCATED, "Content truncated at " + contentInBytes.length
						+ " bytes. Parser can't handle incomplete zip file.", getConf());
			}
			ZipTextExtractor extractor = new ZipTextExtractor(getConf());
			// extract text
			String baseUrl = TableUtil.toString(page.getBaseUrl());
			String[] res = extractor.extractText(new ByteArrayInputStream(contentInBytes), baseUrl, outLinksList);
			title = res[0];
			text = res[1];
			outlinks = (Outlink[]) outLinksList.toArray(new Outlink[outLinksList.size()]);

		} catch (Exception e) {
			return ParseStatusUtils.getEmptyParse(e, getConf());
		}
		ParseStatus status = new ParseStatus();
		status.setMajorCode(ParseStatusCodes.SUCCESS);
		status.setMinorCode(ParseStatusCodes.SUCCESS_OK);
		Parse parse = new Parse(text, title, outlinks, status);
		return parse;

	}

	@Override
	public Collection<Field> getFields() {
		return FIELDS;
	}

}
