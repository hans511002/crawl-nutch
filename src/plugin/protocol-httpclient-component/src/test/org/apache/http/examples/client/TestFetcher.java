/*
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
package org.apache.http.examples.client;

import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.HttpClient;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.message.BasicHeader;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HTTP;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.element.SegMentParsers;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.protocol.httpclient.HttpComponent;
import org.apache.nutch.protocol.httpclient.HttpComponentResponse;
import org.apache.nutch.urlfilter.UrlPathMatch;
import org.apache.nutch.util.NutchConfiguration;

/**
 * Basic fetcher test 1. generate seedlist 2. inject 3. generate 3. fetch 4. Verify contents
 * 
 * @author nutch-dev <nutch-dev at lucene.apache.org>
 * 
 */
public class TestFetcher {
	private static String getAgentString(String agentName, String agentVersion, String agentDesc, String agentURL, String agentEmail) {

		if ((agentName == null) || (agentName.trim().length() == 0)) {

		}

		StringBuffer buf = new StringBuffer();

		buf.append(agentName);
		if (agentVersion != null) {
			buf.append("/");
			buf.append(agentVersion);
		}
		if (((agentDesc != null) && (agentDesc.length() != 0)) || ((agentEmail != null) && (agentEmail.length() != 0))
				|| ((agentURL != null) && (agentURL.length() != 0))) {
			buf.append(" (");

			if ((agentDesc != null) && (agentDesc.length() != 0)) {
				buf.append(agentDesc);
				if ((agentURL != null) || (agentEmail != null))
					buf.append("; ");
			}

			if ((agentURL != null) && (agentURL.length() != 0)) {
				buf.append(agentURL);
				if (agentEmail != null)
					buf.append("; ");
			}

			if ((agentEmail != null) && (agentEmail.length() != 0))
				buf.append(agentEmail);

			buf.append(")");
		}
		return buf.toString();
	}

	public static void main(String[] args) throws Exception {

		String url = "http://m.58.com/cd/zufang/";
		url = "http://i.m.58.com/cd/zufang/15538653692039x.shtml";
		url = "http://i.m.58.com/cd/zufang/15403127032966x.shtml";
		url = "http://m.58.com/wuhou/qiuzu/?from=list_select_quyu";
		url = "http://i.m.58.com/cd/qiuzu/15691792568835x.shtml";
		url = "http://i.m.58.com/cd/qiuzu/15514728510981x.shtml";

		url = "http://m.58.com/cd/ershoufang/";
		url = "http://i.m.58.com/cd/ershoufang/15660173611521x.shtml";
		// url = "http://i.m.58.com/cd/ershoufang/15692610703237x.shtml";
		// url = "http://i.m.58.com/cd/ershoufang/15646523265417x.shtml";
		// url = "http://i.m.58.com/cd/ershoufang/15682093896709x.shtml";
		url = "http://m.58.com/cd/hezu";
		url = "http://i.m.58.com/cd/hezu/11632175277065x.shtml";
		// url = "http://i.m.58.com/cd/hezu/15568727765129x.shtml";
		// url = "http://i.m.58.com/cd/hezu/15568727765129x.shtml";
		url = "http://wap.ganji.com/cd/fang1/445542193x";

		Pattern pattern = Pattern.compile("((.*?)\\?device=wap$)|((.*?)device=wap&(.*))|((.*?)&device=wap$)");
		System.out.println(pattern.matcher(url).replaceAll("$2$4$5$7"));
		pattern = Pattern.compile("(device=wap)");
		System.out.println(pattern.matcher(url).replaceAll(""));

		Configuration conf = NutchConfiguration.create();
		conf.set(Nutch.CRAWL_ID_KEY, "ea");
		NutchConstant.setUrlConfig(conf, 3);
		NutchConstant.setSegmentParseRules(conf);
		NutchConstant.getSegmentParseRules(conf);

		SegMentParsers parses = new SegMentParsers(conf);
		// Result<String, WebPage> rs = query.execute();
		long curTime = System.currentTimeMillis();
		UrlPathMatch urlcfg = NutchConstant.getUrlConfig(conf);
		boolean filter = conf.getBoolean(GeneratorJob.GENERATOR_FILTER, true);
		boolean normalise = conf.getBoolean(GeneratorJob.GENERATOR_NORMALISE, true);
		long limit = conf.getLong(GeneratorJob.GENERATOR_TOP_N, Long.MAX_VALUE);
		if (limit < 5) {
			limit = Long.MAX_VALUE;
		}
		int retryMax = conf.getInt("db.fetch.retry.max", 3);

		limit = Integer.MAX_VALUE;

		curTime = conf.getLong(GeneratorJob.GENERATOR_CUR_TIME, System.currentTimeMillis());

		ProtocolFactory protocolFactory = new ProtocolFactory(conf);

		int rowCount = 0;
		HttpComponent httpComponent = new HttpComponent();
		httpComponent.setConf(conf);
		long l = System.currentTimeMillis();

		try {
			l = System.currentTimeMillis();
			HttpClient httpClient = httpComponent.getClient();
			HttpParams httpParams = httpClient.getParams();
			httpClient.getParams().setParameter("http.protocol.cookie-policy", CookiePolicy.BROWSER_COMPATIBILITY);
			httpClient.getParams().setParameter("http.protocol.content-charset", HTTP.UTF_8);
			String userAgent = getAgentString("NutchCVS", null, "Nutch", "http://lucene.apache.org/nutch/bot.html",
					"nutch-agent@lucene.apache.org");
			userAgent = "Mozilla/5.0 (Linux; U; Android 2.2; en-us; Nexus One Build/FRF91) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1";
			// userAgent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.72 Safari/537.36";
			// userAgent = "Mozilla/5.0 (Windows NT 5.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1";
			// userAgent = "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.77 Safari/537.1";
			String acceptLanguage = "en-us,en-gb,en;q=0.7,*;q=0.3";
			String accept = "text/html,application/xml;q=0.9,application/xhtml+xml,text/xml;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5";
			String acceptCharset = "utf-8,ISO-8859-1;q=0.7,*;q=0.7";
			System.out.println("userAgent=" + userAgent);
			// Set up an HTTPS socket factory that accepts self-signed certs.
			ArrayList<BasicHeader> headers = new ArrayList<BasicHeader>();
			// Set the User Agent in the header
			headers.add(new BasicHeader("User-Agent", userAgent));
			// prefer English
			// headers.add(new BasicHeader("Accept-Language", acceptLanguage));
			// // prefer UTF-8
			// headers.add(new BasicHeader("Accept-Charset", acceptCharset));
			// // prefer understandable formats
			// headers.add(new BasicHeader("Accept", accept));
			// accept gzipped content
			headers.add(new BasicHeader("Accept-Encoding", "x-gzip, gzip, deflate"));
			httpParams.setParameter(ClientPNames.DEFAULT_HEADERS, headers);

			org.apache.nutch.net.protocols.Response response = new HttpComponentResponse(httpComponent, new URL(url), null, true);
			System.out.println("==========================================================");
			System.out.println(new String(response.getContent()).replace("\"utf-8\"", "\"GB2312\""));
			System.out.println("==========================================================");
			int code = response.getCode();
			System.out.println((new Date().toLocaleString()) + " num:" + rowCount + " code:" + code + " time:"
					+ (System.currentTimeMillis() - l) + "  url:" + url);
			l = System.currentTimeMillis();
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
	}
}
