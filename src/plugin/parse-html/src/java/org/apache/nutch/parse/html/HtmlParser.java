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

package org.apache.nutch.parse.html;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseFilters;
import org.apache.nutch.parse.ParseStatusCodes;
import org.apache.nutch.parse.ParseStatusUtils;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.storage.ParseStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.urlfilter.UrlPathMatch;
import org.apache.nutch.urlfilter.UrlPathMatch.UrlNodeConfig;
import org.apache.nutch.util.Bytes;
import org.apache.nutch.util.EncodingDetector;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TableUtil;
import org.cyberneko.html.parsers.DOMFragmentParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class HtmlParser implements org.apache.nutch.parse.HtmlStringParser {
	public static final Logger LOG = LoggerFactory.getLogger(ParseUtil.class);

	// I used 1000 bytes at first, but found that some documents have
	// meta tag well past the first 1000 bytes.
	// (e.g. http://cn.promo.yahoo.com/customcare/music.html)
	private static final int CHUNK_SIZE = 2000;

	// NUTCH-1006 Meta equiv with single quotes not accepted
	private static Pattern metaPattern = Pattern.compile("<meta\\s+([^>]*http-equiv=(\"|')?content-type(\"|')?[^>]*)>",
			Pattern.CASE_INSENSITIVE);
	private static Pattern charsetPattern = Pattern.compile("charset=\\s*([a-z][_\\-0-9a-z]*)",
			Pattern.CASE_INSENSITIVE);

	private static Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

	static {
		FIELDS.add(WebPage.Field.BASE_URL);
	}

	private String parserImpl;

	/**
	 * Given a <code>byte[]</code> representing an html file of an <em>unknown</em> encoding, read out 'charset'
	 * parameter in the meta tag from the first <code>CHUNK_SIZE</code> bytes. If there's no meta tag for Content-Type
	 * or no charset is specified, <code>null</code> is returned. <br />
	 * FIXME: non-byte oriented character encodings (UTF-16, UTF-32) can't be handled with this. We need to do something
	 * similar to what's done by mozilla
	 * (http://lxr.mozilla.org/seamonkey/source/parser/htmlparser/src/nsParser.cpp#1993). See also
	 * http://www.w3.org/TR/REC-xml/#sec-guessing <br />
	 * 
	 * @param content
	 *            <code>byte[]</code> representation of an html file
	 */

	public static String sniffCharacterEncoding(byte[] content) {
		int length = content.length < CHUNK_SIZE ? content.length : CHUNK_SIZE;

		// We don't care about non-ASCII parts so that it's sufficient
		// to just inflate each byte to a 16-bit value by padding.
		// For instance, the sequence {0x41, 0x82, 0xb7} will be turned into
		// {U+0041, U+0082, U+00B7}.
		String str = "";
		try {
			str = new String(content, 0, length, Charset.forName("ASCII").toString());
		} catch (UnsupportedEncodingException e) {
			// code should never come here, but just in case...
			return null;
		}

		Matcher metaMatcher = metaPattern.matcher(str);
		String encoding = null;
		if (metaMatcher.find()) {
			Matcher charsetMatcher = charsetPattern.matcher(metaMatcher.group(1));
			if (charsetMatcher.find())
				encoding = new String(charsetMatcher.group(1));
		}

		return encoding;
	}

	public String defaultCharEncoding;

	private Configuration conf;

	private DOMContentUtils utils;

	private ParseFilters htmlParseFilters;

	private String cachingPolicy;

	@Override
	public Parse getParse(String url, String content) {
		String text = "";
		String title = "";
		DocumentFragment root;
		try {
			byte[] contentInOctets = content.getBytes();
			InputSource input = new InputSource(new ByteArrayInputStream(contentInOctets));
			input.setEncoding(defaultCharEncoding);
			if (LOG.isTraceEnabled()) {
				LOG.trace("Parsing...");
			}
			root = parse(input);
		} catch (IOException e) {
			LOG.error("Failed with the following IOException: ", e);
			return ParseStatusUtils.getEmptyParse(e, getConf());
		} catch (DOMException e) {
			LOG.error("Failed with the following DOMException: ", e);
			return ParseStatusUtils.getEmptyParse(e, getConf());
		} catch (SAXException e) {
			LOG.error("Failed with the following SAXException: ", e);
			return ParseStatusUtils.getEmptyParse(e, getConf());
		} catch (Exception e) {
			LOG.error("Failed with the following Exception: ", e);
			return ParseStatusUtils.getEmptyParse(e, getConf());
		}
		StringBuilder sb = new StringBuilder();
		if (LOG.isTraceEnabled()) {
			LOG.trace("Getting text...");
		}
		getTextHelper(sb, root);
		text = sb.toString().replaceAll("<P></P>", "");
		sb.setLength(0);
		ParseStatus status = new ParseStatus();
		status.setMajorCode(ParseStatusCodes.SUCCESS);
		return new Parse(text, title, null, status);
	}

	public void getTextHelper(StringBuilder sb, Node node) {
		getTextHelper(sb, node, false, false);
	}

	// 使用递归分段
	public boolean getTextHelper(StringBuilder sb, Node node, boolean parIsPause, boolean priIsPause) {
		if (node == null)
			return false;
		String nodeName = node.getNodeName();
		short nodeType = node.getNodeType();
		if (nodeType == Node.TEXT_NODE) {
			// cleanup and trim the value
			String text = node.getNodeValue();
			text = text.replaceAll("\\s+", " ");
			text = text.trim();
			if (text.length() > 0)
				sb.append(text);
			return false;
		}
		boolean curIsPause = false;
		if ("script".equalsIgnoreCase(nodeName)) {
			return false;
		} else if ("style".equalsIgnoreCase(nodeName)) {
			return false;
		} else if (nodeName.toUpperCase().equals("P") || nodeName.toUpperCase().equals("DIV")
				|| nodeName.toUpperCase().equals("SPAN")) {
			curIsPause = true;
		}
		if (curIsPause && (!parIsPause || (parIsPause && priIsPause)))
			sb.append("<P>");
		NodeList currentChildren = node.getChildNodes();
		int childLen = (currentChildren != null) ? currentChildren.getLength() : 0;
		if (childLen == 0)
			return false;
		boolean _priIsPause = true;
		for (int i = 0; i < childLen; i++) {
			Node _node = currentChildren.item(i);
			_priIsPause = getTextHelper(sb, _node, curIsPause, _priIsPause);
		}
		if (curIsPause && (!parIsPause || (parIsPause && priIsPause))) {
			sb.append("</P>");
		}
		return curIsPause;
	}

	public Parse getParse(String url, WebPage page) {
		HTMLMetaTags metaTags = new HTMLMetaTags();

		String baseUrl = TableUtil.toString(page.getBaseUrl());
		URL base;
		try {
			base = new URL(baseUrl);
		} catch (MalformedURLException e) {
			return ParseStatusUtils.getEmptyParse(e, getConf());
		}

		String text = "";
		String title = "";
		String content = "";
		Outlink[] outlinks = new Outlink[0];
		Metadata metadata = new Metadata();
		String encoding = null;
		// parse the content
		DocumentFragment root;
		try {
			byte[] contentInOctets = page.getContent().array();
			InputSource input = new InputSource(new ByteArrayInputStream(contentInOctets));

			EncodingDetector detector = new EncodingDetector(conf);
			detector.autoDetectClues(page, true);
			detector.addClue(sniffCharacterEncoding(contentInOctets), "sniffed");
			encoding = detector.guessEncoding(page, defaultCharEncoding);

			metadata.set(Metadata.ORIGINAL_CHAR_ENCODING, encoding);
			metadata.set(Metadata.CHAR_ENCODING_FOR_CONVERSION, encoding);

			input.setEncoding(encoding);
			if (LOG.isTraceEnabled()) {
				LOG.trace("Parsing...");
			}
			root = parse(input);
		} catch (IOException e) {
			LOG.error("Failed with the following IOException: ", e);
			return ParseStatusUtils.getEmptyParse(e, getConf());
		} catch (DOMException e) {
			LOG.error("Failed with the following DOMException: ", e);
			return ParseStatusUtils.getEmptyParse(e, getConf());
		} catch (SAXException e) {
			LOG.error("Failed with the following SAXException: ", e);
			return ParseStatusUtils.getEmptyParse(e, getConf());
		} catch (Exception e) {
			LOG.error("Failed with the following Exception: ", e);
			return ParseStatusUtils.getEmptyParse(e, getConf());
		}

		// get meta directives
		HTMLMetaProcessor.getMetaTags(metaTags, root, base);
		if (metaTags.getBaseHref() != null)
			page.putToMetadata(new Utf8("baseurl"), ByteBuffer.wrap(Bytes.toBytes(metaTags.getBaseHref().toString())));
		if (metaTags.getNoCache())
			page.putToMetadata(new Utf8("nocache"), ByteBuffer.wrap(Bytes.toBytes(metaTags.getNoCache())));
		if (metaTags.getNoFollow())
			page.putToMetadata(new Utf8("noFollow"), ByteBuffer.wrap(Bytes.toBytes(metaTags.getNoFollow())));
		if (metaTags.getNoIndex())
			page.putToMetadata(new Utf8("noIndex"), ByteBuffer.wrap(Bytes.toBytes(metaTags.getNoIndex())));
		if (metaTags.getRefresh()) {
			page.putToMetadata(new Utf8("refresh"), ByteBuffer.wrap(Bytes.toBytes(metaTags.getRefresh())));
			page.putToMetadata(new Utf8("refreshHref"),
					ByteBuffer.wrap(Bytes.toBytes(metaTags.getRefreshHref().toString())));
		}
		Properties properties = metaTags.getGeneralTags();

		Iterator<?> it = properties.keySet().iterator();
		while (it.hasNext()) {
			String key = (String) it.next();
			page.putToMetadata(new Utf8(key), ByteBuffer.wrap(Bytes.toBytes(properties.getProperty(key))));
		}
		properties = metaTags.getHttpEquivTags();
		it = properties.keySet().iterator();
		while (it.hasNext()) {
			String key = (String) it.next();
			page.putToMetadata(new Utf8(key), ByteBuffer.wrap(Bytes.toBytes(properties.getProperty(key))));
		}
		if (LOG.isTraceEnabled()) {
			LOG.trace("Meta tags for " + base + ": " + metaTags.toString());
		}
		// check meta directives
		// if (!metaTags.getNoIndex()) { // okay to index
		StringBuilder sb = new StringBuilder();
		if (LOG.isTraceEnabled()) {
			LOG.trace("Getting text...");
		}
		// getTextHelper(sb, root);
		// text = sb.toString().replaceAll("<P></P>", "");
		utils.getText(sb, root); // extract text
		text = sb.toString();
		sb.setLength(0);
		if (LOG.isTraceEnabled()) {
			LOG.trace("Getting title...");
		}
		utils.getTitle(sb, root); // extract title
		title = sb.toString().trim();
		// }

		if (!metaTags.getNoFollow()) { // okay to follow links
			ArrayList<Outlink> l = new ArrayList<Outlink>(); // extract outlinks
			URL baseTag = utils.getBase(root);
			if (LOG.isTraceEnabled()) {
				LOG.trace("Getting links...");
			}
			utils.getOutlinks(baseTag != null ? baseTag : base, l, root);
			outlinks = l.toArray(new Outlink[l.size()]);
			if (LOG.isTraceEnabled()) {
				LOG.trace("found " + outlinks.length + " outlinks in " + url);
			}
		}
		ParseStatus status = new ParseStatus();
		status.setMajorCode(ParseStatusCodes.SUCCESS);
		if (metaTags.getRefresh()) {
			status.setMinorCode(ParseStatusCodes.SUCCESS_REDIRECT);
			status.addToArgs(new Utf8(metaTags.getRefreshHref().toString()));
			status.addToArgs(new Utf8(Integer.toString(metaTags.getRefreshTime())));
		}
		UrlPathMatch urlcfg = null;
		String pluginName = null;
		try {
			urlcfg = NutchConstant.getUrlConfig(conf);
			UrlPathMatch urlSegment = urlcfg.match(url);// 获取配置项
			UrlNodeConfig nodeValue = (UrlNodeConfig) urlSegment.getNodeValue();
			page.nodeConfig = nodeValue;
			pluginName = page.nodeConfig.getConf("parase.plugin.outlink.name");
		} catch (IOException e) {
			LOG.warn("获取解析插件失败" + page.getBaseUrl());
		}

		if (null != pluginName) {
			String pName[] = pluginName.split(",");
			try {
				if (encoding != null) {
					content = new String(page.getContent().array(), encoding);
				} else {
					content = new String(page.getContent().array());
				}
			} catch (Exception e) {
				content = null;
			}

			if (null != content) {
				for (int i = 0; i < pName.length; i++) {
					if ("addjsoutlink".equalsIgnoreCase(pName[i])) {
						outlinks = new AddJsOutLinkPlugin(outlinks, content, page).getOutLink();
					} else if ("mergelink".equalsIgnoreCase(pName[i])) {
						outlinks = new MergeOutLinkPlugin(outlinks, content, page).getOutLink();
					} else if ("special".equalsIgnoreCase(pName[i])) {
						outlinks = new AddSpecialOutLinkPlugin(outlinks, content, page).getOutLink();
					} else if ("filter".equalsIgnoreCase(pName[i])) {
						outlinks = new FilterSpecialOutLinkPlugin(outlinks, page).getOutLink();
					} else if ("pattern".equalsIgnoreCase(pName[i])) {
						outlinks = new PatternSpecialOutLinkPlugin(outlinks, page).getOutLink();
					} else if ("partrange".equalsIgnoreCase(pName[i])) {
						outlinks = new PartRangeOutLinkPlugin(outlinks, content, page).getOutLink();
					}
				}
			}
		}
		Parse parse = new Parse(text, title, outlinks, status);
		parse = htmlParseFilters.filter(url, page, parse, metaTags, root);

		if (metaTags.getNoCache()) { // not okay to cache
			page.putToMetadata(new Utf8(Nutch.CACHING_FORBIDDEN_KEY), ByteBuffer.wrap(Bytes.toBytes(cachingPolicy)));
		}
		return parse;
	}

	public DocumentFragment parse(InputSource input) throws Exception {
		if (parserImpl.equalsIgnoreCase("tagsoup"))
			return parseTagSoup(input);
		else
			return parseNeko(input);
	}

	private DocumentFragment parseTagSoup(InputSource input) throws Exception {
		HTMLDocumentImpl doc = new HTMLDocumentImpl();
		DocumentFragment frag = doc.createDocumentFragment();
		DOMBuilder builder = new DOMBuilder(doc, frag);
		org.ccil.cowan.tagsoup.Parser reader = new org.ccil.cowan.tagsoup.Parser();
		reader.setContentHandler(builder);
		reader.setFeature(org.ccil.cowan.tagsoup.Parser.ignoreBogonsFeature, true);
		reader.setFeature(org.ccil.cowan.tagsoup.Parser.bogonsEmptyFeature, false);
		reader.setProperty("http://xml.org/sax/properties/lexical-handler", builder);
		reader.parse(input);
		return frag;
	}

	private DocumentFragment parseNeko(InputSource input) throws Exception {
		DOMFragmentParser parser = new DOMFragmentParser();
		try {
			parser.setFeature("http://cyberneko.org/html/features/augmentations", true);
			parser.setProperty("http://cyberneko.org/html/properties/default-encoding", defaultCharEncoding);
			parser.setFeature("http://cyberneko.org/html/features/scanner/ignore-specified-charset", true);
			parser.setFeature("http://cyberneko.org/html/features/balance-tags/ignore-outside-content", false);
			parser.setFeature("http://cyberneko.org/html/features/balance-tags/document-fragment", true);
			parser.setFeature("http://cyberneko.org/html/features/report-errors", LOG.isTraceEnabled());
		} catch (SAXException e) {
		}
		// convert Document to DocumentFragment
		HTMLDocumentImpl doc = new HTMLDocumentImpl();
		doc.setErrorChecking(false);
		DocumentFragment res = doc.createDocumentFragment();
		DocumentFragment frag = doc.createDocumentFragment();
		parser.parse(input, frag);
		res.appendChild(frag);

		try {
			while (true) {
				frag = doc.createDocumentFragment();
				parser.parse(input, frag);
				if (!frag.hasChildNodes())
					break;
				if (LOG.isInfoEnabled()) {
					LOG.info(" - new frag, " + frag.getChildNodes().getLength() + " nodes.");
				}
				res.appendChild(frag);
			}
		} catch (Exception x) {
			LOG.error("Failed with the following Exception: ", x);
		}
		return res;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
		this.htmlParseFilters = new ParseFilters(getConf());
		this.parserImpl = getConf().get("parser.html.impl", "neko");
		this.defaultCharEncoding = getConf().get("parser.character.encoding.default", "windows-1252");
		this.utils = new DOMContentUtils(conf);
		this.cachingPolicy = getConf().get("parser.caching.forbidden.policy", Nutch.CACHING_FORBIDDEN_CONTENT);
	}

	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public Collection<WebPage.Field> getFields() {
		return FIELDS;
	}

	public static void main(String[] args) throws Exception {
		// LOG.setLevel(Level.FINE);
		String name = args[0];
		String url = "file:" + name;
		File file = new File(name);
		byte[] bytes = new byte[(int) file.length()];
		DataInputStream in = new DataInputStream(new FileInputStream(file));
		in.readFully(bytes);
		Configuration conf = NutchConfiguration.create();
		HtmlParser parser = new HtmlParser();
		parser.setConf(conf);
		WebPage page = new WebPage();
		page.setBaseUrl(new Utf8(url));
		page.setContent(ByteBuffer.wrap(bytes));
		page.setContentType(new Utf8("text/html"));
		Parse parse = parser.getParse(url, page);
		System.out.println("title: " + parse.getTitle());
		System.out.println("text: " + parse.getText());
		System.out.println("outlinks: " + Arrays.toString(parse.getOutlinks()));

	}

}
