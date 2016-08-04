package com.crawl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.ByteBuffer;

import org.apache.avro.util.Utf8;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.nutch.crawl.FetchSchedule;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.ProtocolStatus;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.urlfilter.SubURLFilters;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class App {
	public static final Logger LOG = LoggerFactory.getLogger(App.class);
	public static ParseUtil parseUtil;
	public static boolean skipTruncated;
	public static URLFilters filters = null;
	public static URLNormalizers normalizers = null;
	public static FetchSchedule schedule;
	public static ScoringFilters scoringFilters;
	public static long rowCount = 0;
	public static long scanRowCount = 0;
	public static String batchID;
	public static String batchTime;
	public static String reprUrl;
	public static URLFilters urlFilters;
	public static SubURLFilters subUrlFilter;

	public static App thisrun = null;

	public App() {
		thisrun = this;
	}

	public abstract void setCookies(DefaultHttpClient httpClient);

	public static boolean checkUrl(String olurl, String name) {
		try {
			int spindex = olurl.indexOf(' ');
			if (spindex > 0) {
				olurl = olurl.substring(0, spindex);
			}
			spindex = olurl.indexOf('>');
			if (spindex > 0) {
				olurl = olurl.substring(0, spindex);
			}
			spindex = olurl.indexOf('<');
			if (spindex > 0) {
				olurl = olurl.substring(0, spindex);
			}
			olurl = normalizers.normalize(olurl, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
			URI u = new URI(olurl);
			if (subUrlFilter != null && !subUrlFilter.filter(olurl))
				return false;
			InetAddress address = InetAddress.getByName(u.getHost());
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			LOG.warn("outlink:" + olurl + " author:" + name + " error:" + ex.getMessage());
			return false;
		}
	}

	public static void handleRedirect(String url, String newUrl, boolean temp, String redirType, WebPage page)
			throws URLFilterException, IOException, InterruptedException {
		newUrl = normalizers.normalize(newUrl, URLNormalizers.SCOPE_FETCHER);
		newUrl = urlFilters.filter(newUrl);
		if (newUrl == null || newUrl.equals(url)) {
			return;
		}
		page.putToOutlinks(new Utf8(newUrl), new Utf8());
		page.putToMetadata(FetcherJob.REDIRECT_DISCOVERED, TableUtil.YES_VAL);
		reprUrl = URLUtil.chooseRepr(reprUrl, newUrl, temp);
		if (reprUrl == null) {
			LOG.warn("url:" + url + " reprUrl==null");
		} else {
			page.setReprUrl(new Utf8(reprUrl));
			if (LOG.isDebugEnabled()) {
				LOG.debug("url:" + url + "  - " + redirType + " redirect to " + reprUrl + " (fetching later)");
			}
		}
	}

	public static void output(WebPage page, Content content, ProtocolStatus pstatus, byte status) throws IOException,
			InterruptedException {
		page.setStatus(status);
		// final long prevFetchTime = page.getFetchTime();
		long curTime = System.currentTimeMillis();
		page.setPrevFetchTime(curTime);
		page.setFetchTime(curTime);
		if (pstatus != null) {
			page.setProtocolStatus(pstatus);
		}
		if (content != null) {
			page.setContent(ByteBuffer.wrap(content.getContent()));
			page.setContentType(new Utf8(content.getContentType()));
			page.setBaseUrl(new Utf8(content.getBaseUrl()));
		}
		if (content != null) {
			Metadata data = content.getMetadata();
			String[] names = data.names();
			for (String name : names) {
				page.putToHeaders(new Utf8(name), new Utf8(data.get(name)));
			}
		}
		// remove content if storingContent is false. Content is added to page above
		// for ParseUtil be able to parse it.
		if (content == null) {
			page.setContent(ByteBuffer.wrap(new byte[0]));
		}
	}

}
