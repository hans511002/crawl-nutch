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

package org.apache.nutch.parse.ext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.OutlinkExtractor;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.util.CommandRunner;
import org.apache.nutch.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper that invokes external command to do real parsing job.
 * 
 * @author John Xing
 */

public class ExtParser implements Parser {

	public static final Logger LOG = LoggerFactory.getLogger("org.apache.nutch.parse.ext");

	static final int BUFFER_SIZE = 4096;

	static final int TIMEOUT_DEFAULT = 30; // in seconds

	// handy map from String contentType to String[] {command, timeoutString, encoding}
	Hashtable TYPE_PARAMS_MAP = new Hashtable();

	private Configuration conf;

	private boolean loaded = false;
	private static Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();
	static {
		FIELDS.add(WebPage.Field.BASE_URL);
	}

	public ExtParser() {
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
		Extension[] extensions = PluginRepository.get(conf).getExtensionPoint("org.apache.nutch.parse.Parser").getExtensions();

		String contentType, command, timeoutString, encoding;

		for (int i = 0; i < extensions.length; i++) {
			Extension extension = extensions[i];

			// only look for extensions defined by plugin parse-ext
			if (!extension.getDescriptor().getPluginId().equals("parse-ext"))
				continue;

			contentType = extension.getAttribute("contentType");
			if (contentType == null || contentType.equals(""))
				continue;

			command = extension.getAttribute("command");
			if (command == null || command.equals(""))
				continue;

			// null encoding means default
			encoding = extension.getAttribute("encoding");
			if (encoding == null)
				encoding = Charset.defaultCharset().name();

			timeoutString = extension.getAttribute("timeout");
			if (timeoutString == null || timeoutString.equals(""))
				timeoutString = "" + TIMEOUT_DEFAULT;

			TYPE_PARAMS_MAP.put(contentType, new String[] { command, timeoutString, encoding });
		}
	}

	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public Parse getParse(String url, WebPage page) {
		String contentType = TableUtil.toString(page.getContentType());

		String[] params = (String[]) TYPE_PARAMS_MAP.get(contentType);
		if (params == null)
			return ParseStatusUtils.getEmptyParse(ParseStatusCodes.FAILED, "No external command defined for contentType: " + contentType,
					getConf());

		String command = params[0];
		int timeout = Integer.parseInt(params[1]);
		String encoding = params[2];

		if (LOG.isTraceEnabled()) {
			LOG.trace("Use " + command + " with timeout=" + timeout + "secs");
		}

		String text = null;
		String title = null;

		try {

			byte[] raw = page.getContent().array();

			String contentLength = new String(page.getMetadata().get(Metadata.CONTENT_LENGTH).array());
			if (contentLength != null && raw.length != Integer.parseInt(contentLength)) {
				return ParseStatusUtils.getEmptyParse(ParseStatusCodes.FAILED_TRUNCATED, "Content truncated at " + raw.length
						+ " bytes. Parser can't handle incomplete " + contentType + " file.", getConf());
			}

			ByteArrayOutputStream os = new ByteArrayOutputStream(BUFFER_SIZE);
			ByteArrayOutputStream es = new ByteArrayOutputStream(BUFFER_SIZE / 4);

			CommandRunner cr = new CommandRunner();

			cr.setCommand(command + " " + contentType);
			cr.setInputStream(new ByteArrayInputStream(raw));
			cr.setStdOutputStream(os);
			cr.setStdErrorStream(es);

			cr.setTimeout(timeout);

			cr.evaluate();

			if (cr.getExitValue() != 0)
				return ParseStatusUtils.getEmptyParse(ParseStatusCodes.FAILED_TRUNCATED, "External command " + command
						+ " failed with error:" + es.toString(), getConf());

			text = os.toString(encoding);

		} catch (Exception e) { // run time exception
			return ParseStatusUtils.getEmptyParse(e, getConf());
		}
		if (text == null)
			text = "";
		if (title == null)
			title = "";
		// collect outlink
		Outlink[] outlinks = OutlinkExtractor.getOutlinks(text, getConf());
		ParseStatus status = new ParseStatus();
		status.setMajorCode(ParseStatusCodes.SUCCESS);
		status.setMinorCode(ParseStatusCodes.SUCCESS_OK);
		Parse parse = new Parse(text, "", outlinks, status);
		return parse;
	}

	@Override
	public Collection<Field> getFields() {
		return FIELDS;

	}
}
