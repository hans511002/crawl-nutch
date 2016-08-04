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
package org.apache.nutch.parse;

// Commons Logging imports
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.StringUtils;
import org.apache.nutch.crawl.CrawlStatus;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.crawl.Signature;
import org.apache.nutch.crawl.SignatureComparator;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.metadata.HttpHeaders;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.urlfilter.SubURLFilters;
import org.apache.nutch.urlfilter.UrlPathMatch;
import org.apache.nutch.urlfilter.UrlPathMatch.UrlNodeConfig;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A Utility class containing methods to simply perform parsing utilities such as iterating through a preferred list of
 * {@link Parser}s to obtain {@link Parse} objects.
 * 
 * @author mattmann
 * @author J&eacute;r&ocirc;me Charron
 * @author S&eacute;bastien Le Callonnec
 */
public class ParseUtil extends Configured {

	/* our log stream */
	public static final Logger LOG = LoggerFactory.getLogger(ParseUtil.class);

	private static final int DEFAULT_MAX_PARSE_TIME = 30;

	private Configuration conf;
	private Signature sig;
	private URLFilters filters;
	private URLNormalizers normalizers;
	private int maxOutlinks;
	private boolean ignoreExternalLinks;
	private ParserFactory parserFactory;
	/** Parser timeout set to 30 sec by default. Set -1 to deactivate **/
	private int maxParseTime;
	private ExecutorService executorService;

	/**
	 * 
	 * @param conf
	 */
	public ParseUtil(Configuration conf) {
		super(conf);
		setConf(conf);
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		parserFactory = new ParserFactory(conf);
		maxParseTime = conf.getInt("parser.timeout", DEFAULT_MAX_PARSE_TIME);
		sig = SignatureFactory.getSignature(conf);
		filters = new URLFilters(conf);
		normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_OUTLINK);
		int maxOutlinksPerPage = conf.getInt("db.max.outlinks.per.page", 100);
		maxOutlinks = (maxOutlinksPerPage < 0) ? Integer.MAX_VALUE : maxOutlinksPerPage;
		ignoreExternalLinks = conf.getBoolean("db.ignore.external.links", false);
		executorService = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("parse-%d")
				.setDaemon(true).build());
	}

	/**
	 * Performs a parse by iterating through a List of preferred {@link Parser}s until a successful parse is performed
	 * and a {@link Parse} object is returned. If the parse is unsuccessful, a message is logged to the
	 * <code>WARNING</code> level, and an empty parse is returned.
	 * 
	 * @throws ParserNotFound
	 *             If there is no suitable parser found.
	 * @throws ParseException
	 *             If there is an error parsing.
	 */
	public Parse parse(String url, WebPage page) throws ParserNotFound, ParseException {
		Parser[] parsers = null;

		String contentType = TableUtil.toString(page.getContentType());

		// System.err.println(" url=" + url + " ParserUtil line:117 contentType=" + contentType);
		parsers = this.parserFactory.getParsers(contentType, url);
		// System.err.println(" url=" + url + " ParserUtil line:119 parsers=" + parsers + " length:" + parsers.length);

		for (int i = 0; i < parsers.length; i++) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Parsing [" + url + "] with [" + parsers[i] + "]");
			}
			Parse parse = null;
			// System.err.println(" url=" + url + " ParserUtil pline:126 arsers[" + i + "]=" + parsers[i]);

			if (maxParseTime != -1)
				parse = runParser(parsers[i], url, page);
			else
				parse = parsers[i].getParse(url, page);
			// System.err.println(" url=" + url + " ParserUtil line:132 parse=" + parse);

			if (parse != null && ParseStatusUtils.isSuccess(parse.getParseStatus())) {
				// System.err.println(" url=" + url + " ParserUtil line:135 parse.getTitle=" + parse.getTitle());
				return parse;
			}
		}
		// System.err.println(" url=" + url + " ParserUtil line:138 " + "Unable to successfully parse content " + url +
		// " of type "
		// + contentType);

		LOG.warn("Unable to successfully parse content " + url + " of type " + contentType);
		return ParseStatusUtils.getEmptyParse(new ParseException("Unable to successfully parse content"), null);
	}

	public Parse parse(String url, String content, String contentType) throws ParserNotFound, ParseException {
		Parser[] parsers = null;
		parsers = this.parserFactory.getParsers(contentType, url);
		for (int i = 0; i < parsers.length; i++) {
			if (parsers[i] instanceof HtmlStringParser) {
				Parse parse = null;
				if (maxParseTime != -1)
					parse = runParser(parsers[i], url, content);
				else
					parse = ((HtmlStringParser) parsers[i]).getParse(url, content);

				if (parse != null && ParseStatusUtils.isSuccess(parse.getParseStatus())) {
					return parse;
				}
			}
		}
		LOG.warn("Unable to successfully parse content " + url + " of type " + contentType);
		return ParseStatusUtils.getEmptyParse(new ParseException("Unable to successfully parse content"), null);
	}

	private Parse runParser(Parser p, String url, String content) {
		ParseCallable pc = new ParseCallable(p, content, url);
		Future<Parse> task = executorService.submit(pc);
		Parse res = null;
		try {
			res = task.get(maxParseTime, TimeUnit.SECONDS);
		} catch (Exception e) {
			LOG.warn("Error parsing " + url, e);
			task.cancel(true);
		} finally {
			pc = null;
		}
		return res;
	}

	private Parse runParser(Parser p, String url, WebPage page) {
		ParseCallable pc = new ParseCallable(p, page, url);
		Future<Parse> task = executorService.submit(pc);
		Parse res = null;
		try {
			res = task.get(maxParseTime, TimeUnit.SECONDS);
		} catch (Exception e) {
			LOG.warn("Error parsing " + url, e);
			task.cancel(true);
		} finally {
			pc = null;
		}
		return res;
	}

	/**
	 * Parses given web page and stores parsed content within page. Puts a meta-redirect to outlinks.
	 * 
	 * @param key
	 * @param page
	 */
	public void process(String key, WebPage page) {
		String url = TableUtil.unreverseUrl(key);
		byte status = (byte) page.getStatus();
		// System.err.println(" url=" + key + " status=" + status);
		if (status != CrawlStatus.STATUS_FETCHED) {
			LOG.warn("Skipping " + url + " as status is: " + CrawlStatus.getName(status));
			return;
		}

		Parse parse;
		try {
			parse = parse(url, page);
		} catch (ParserNotFound e) {
			// do not print stacktrace for the fact that some types are not mapped.
			LOG.warn("No suitable parser found: " + e.getMessage());
			e.printStackTrace();
			return;
		} catch (final Exception e) {
			LOG.warn("Error parsing: " + url + ": " + StringUtils.stringifyException(e));
			e.printStackTrace();
			return;
		}
		if (parse == null) {
			LOG.warn("Error parse: " + url + " result is null");
			return;
		}

		final byte[] signature = sig.calculate(page);
		// System.err.println(" url=" + url + " ParserUtil line:198 signature=" + signature);
		org.apache.nutch.storage.ParseStatus pstatus = parse.getParseStatus();
		page.setParseStatus(pstatus);
		if (ParseStatusUtils.isSuccess(pstatus)) {
			// System.err.println(" url=" + url + " ParserUtil line:205 pstatus=" + pstatus);
			if (pstatus.getMinorCode() == ParseStatusCodes.SUCCESS_REDIRECT) {
				String newUrl = ParseStatusUtils.getMessage(pstatus);
				int refreshTime = Integer.parseInt(ParseStatusUtils.getArg(pstatus, 1));
				try {
					newUrl = normalizers.normalize(newUrl, URLNormalizers.SCOPE_FETCHER);
					if (newUrl == null) {
						LOG.warn("redirect normalized to null " + url);
						return;
					}
					try {
						newUrl = filters.filter(newUrl);
					} catch (URLFilterException e) {
						return;
					}
					if (newUrl == null) {
						LOG.warn("redirect filtered to null " + url);
						return;
					}
				} catch (MalformedURLException e) {
					LOG.warn("malformed url exception parsing redirect " + url);
					return;
				}
				page.putToOutlinks(new Utf8(newUrl), new Utf8());
				page.putToMetadata(FetcherJob.REDIRECT_DISCOVERED, TableUtil.YES_VAL);
				if (newUrl == null || newUrl.equals(url)) {
					String reprUrl = URLUtil.chooseRepr(url, newUrl, refreshTime < FetcherJob.PERM_REFRESH_TIME);
					if (reprUrl == null) {
						LOG.warn("reprUrl==null for " + url);
						return;
					} else {
						page.setReprUrl(new Utf8(reprUrl));
					}
				}
			} else {
				page.setTitle(new Utf8(parse.getTitle()));
				page.setText(new Utf8(parse.getText()));
				Utf8 contentLength = page.getFromHeaders(new Utf8(HttpHeaders.CONTENT_LENGTH));
				if (contentLength == null)
					page.putToHeaders(new Utf8(HttpHeaders.CONTENT_LENGTH), new Utf8(parse.getText().length() + ""));
				ByteBuffer prevSig = page.getSignature();
				if (prevSig != null) {
					page.setPrevSignature(prevSig);
					if (SignatureComparator.compare(prevSig.array(), signature) == 0) {
						page.setFetchInterval(page.getFetchInterval() * 5 + 30 * FetchSchedule.SECONDS_PER_DAY);
					}
				}
				page.setSignature(ByteBuffer.wrap(signature));
				if (page.getOutlinks() != null) {
					page.getOutlinks().clear();
				}
				final Outlink[] outlinks = parse.getOutlinks();
				final int count = 0;
				String fromHost;
				if (ignoreExternalLinks) {
					try {
						fromHost = new URL(url).getHost().toLowerCase();
					} catch (final MalformedURLException e) {
						fromHost = null;
					}
				} else {
					fromHost = null;
				}

				String filterurl = null;
				if (page.nodeConfig != null)
					filterurl = page.nodeConfig.getConf("parase.plugin.outlink.name.filterpattern");
				Pattern filterpattern = (null == filterurl || filterurl.trim().length() <= 0) ? null : Pattern
						.compile(filterurl);
				for (int i = 0; count < maxOutlinks && i < outlinks.length; i++) {
					String toUrl = outlinks[i].getToUrl();
					try {
						toUrl = normalizers.normalize(toUrl, URLNormalizers.SCOPE_OUTLINK);
						toUrl = filters.filter(toUrl);
						try {
							int spindex = toUrl.indexOf(' ');
							if (spindex > 0) {
								toUrl = toUrl.substring(0, spindex);
							}
							if (NutchConstant.urlcfg != null) {
								UrlPathMatch urlSegment = NutchConstant.urlcfg.match(toUrl);// 获取配置项
								if (urlSegment == null) {
									LOG.info("未匹配到配置地址，过滤Outlink：" + toUrl + " Anchor=" + outlinks[i].getAnchor());
									continue;
								}
								if (null != filterpattern && !filterpattern.matcher(toUrl).matches()) {
									LOG.info("未匹配到配置地址，filter过滤Outlink：" + toUrl + " Anchor=" + outlinks[i].getAnchor());
									continue;
								}
								if (urlSegment != null) {
									UrlNodeConfig value = (UrlNodeConfig) urlSegment.getNodeValue();
									if (value.regFormat != null) {
										toUrl = value.regFormat.replace(toUrl);
									}
									String configUrl = urlSegment.getConfigPath();
									if (!configUrl.equals(toUrl)
											&& !(toUrl.endsWith("/") && configUrl.equals(toUrl.substring(0,
													toUrl.length() - 1)))) {
										if (!SubURLFilters.filter(toUrl, value.subFilters)) {
											LOG.info("过滤Outlink：" + toUrl);
											continue;
										}
									}
								}
							}

							new URI(toUrl);
						} catch (Exception ex) {
							continue;
						}
					} catch (MalformedURLException e2) {
						continue;
					} catch (URLFilterException e) {
						continue;
					}
					if (toUrl == null) {
						continue;
					}
					Utf8 utf8ToUrl = new Utf8(toUrl);
					if (page.getFromOutlinks(utf8ToUrl) != null) {
						// skip duplicate outlinks
						continue;
					}
					String toHost;
					if (ignoreExternalLinks) {
						try {
							toHost = new URL(toUrl).getHost().toLowerCase();
						} catch (final MalformedURLException e) {
							toHost = null;
						}
						if (toHost == null || !toHost.equals(fromHost)) { // external links
							continue; // skip it
						}
					}
					page.putToOutlinks(utf8ToUrl, new Utf8(outlinks[i].getAnchor()));
				}
				Utf8 fetchMark = Mark.FETCH_MARK.checkMark(page);
				if (fetchMark != null) {
					Mark.PARSE_MARK.putMark(page, fetchMark);
				}
			}
		}
	}
}
