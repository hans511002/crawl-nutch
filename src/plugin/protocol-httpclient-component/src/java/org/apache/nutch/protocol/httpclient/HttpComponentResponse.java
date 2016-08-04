package org.apache.nutch.protocol.httpclient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.util.EntityUtils;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.SpellCheckedMetadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.parse.html.HtmlParser;
import org.apache.nutch.protocol.http.api.HttpBase;
import org.apache.nutch.protocol.httpclient.site.HttpClientUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.util.EncodingDetector;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TestLogUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpComponentResponse implements Response {
	public static final Logger LOG = LoggerFactory.getLogger("org.apache.hadoop.protocl.httpclient.component");
	private URL url;
	private byte[] content;
	private int code = -1;
	private Metadata headers = new SpellCheckedMetadata();
	private HttpClient httpClient;

	public HttpComponentResponse(HttpComponent httpComponent, URL url, WebPage page, boolean useHttpClient)
			throws IOException, Exception {
		// Prepare GET method for HTTP request
		this.url = url;
		this.httpClient = httpComponent.getClient();
		HttpRequestBase httpRequest = null;
		HttpResponse response = null;
		if (useHttpClient) {
			try {
				httpClient.getParams()
						.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, httpComponent.getTimeout());
				httpRequest = new HttpGet(url.toString());
				if (page.nodeConfig != null) {
					HashMap<String, String> map = page.nodeConfig.httpClientCfg;
					if (null != map && map.containsKey("post") && "true".equals(map.get("post"))) {
						httpRequest = new HttpPost(url.toString());
					}
					if (null != map && map.containsKey("ClearCookie") && "true".equals(map.get("ClearCookie"))) {
						httpRequest.getParams().setParameter(ClientPNames.COOKIE_POLICY, CookiePolicy.IGNORE_COOKIES); // 忽略httpClient中保存的cookie。
					}
				}
				if ("https".equals(this.url.getProtocol())) {
					response = HttpClientUtils.sendSSLPostRequest(this.httpClient, httpRequest);
				} else {
					response = this.httpClient.execute(httpRequest);
				}
				packageResponse(httpComponent, url, httpRequest, response);
			} finally {
				// 关闭连接
				if (null != httpRequest) {
					httpRequest.releaseConnection();
				}
			}
		} else {
			HttpPost httpPost = null;
			boolean needLogin = false;
			String loginjs = null;
			String validatePath = null;
			String loginoutPath = null;
			String loginPath = null;
			String username = null;
			String password = null;
			if (null != page && null != page.nodeConfig) {
				needLogin = page.nodeConfig.needLogin;
				loginPath = page.nodeConfig.loginAddress;
				loginjs = page.nodeConfig.loginJS;
				username = page.nodeConfig.loginUserName;
				password = page.nodeConfig.loginPassword;
				validatePath = page.nodeConfig.loginValidateAddress;
				loginoutPath = page.nodeConfig.loginoutAddress;
			}

			TestLogUtil.log("url:" + url.toString());
			TestLogUtil.log("needLogin:" + needLogin);
			TestLogUtil.log("loginPath:" + loginPath);
			TestLogUtil.log("loginjs:" + loginjs);
			TestLogUtil.log("username:" + username);
			TestLogUtil.log("password:" + password);
			TestLogUtil.log("validatePath:" + validatePath);
			TestLogUtil.log("loginoutPath:" + loginoutPath);
			try {
				httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT,
						httpComponent.getTimeout() * 4);
				httpPost = new HttpPost(httpComponent.getLocalExplorerProxyHost());
				List<NameValuePair> nvps = new ArrayList<NameValuePair>();
				nvps.add(new BasicNameValuePair("url", url.toString()));
				nvps.add(new BasicNameValuePair("isNeedlogin", String.valueOf(needLogin)));
				nvps.add(new BasicNameValuePair("loginpath", loginPath));
				nvps.add(new BasicNameValuePair("loginjs", loginjs));
				nvps.add(new BasicNameValuePair("username", username));
				nvps.add(new BasicNameValuePair("password", password));
				nvps.add(new BasicNameValuePair("validatepath", validatePath));
				nvps.add(new BasicNameValuePair("loginoutPath", loginoutPath));
				httpPost.setEntity(new UrlEncodedFormEntity(nvps, "UTF-8"));
				response = this.httpClient.execute(httpPost);
				packageResponse(httpComponent, url, httpPost, response);
			} catch (Exception e) {
				LOG.error("[*HttpClientProxy*] Exceptions:" + e.getMessage() + ", url:" + url);
				e.printStackTrace();
				throw new IOException(e.getMessage());
			} finally {
				// 关闭连接
				if (null != httpPost) {
					httpPost.reset();
				}
			}
		}
	}

	private void packageResponse(HttpComponent httpComponent, URL url, HttpRequestBase httpRequest,
			HttpResponse response) throws HttpException, IOException {
		this.code = response.getStatusLine().getStatusCode();
		Header[] heads = response.getAllHeaders();
		this.headers.clear();
		for (int i = 0; i < heads.length; i++) {
			this.headers.set(heads[i].getName(), heads[i].getValue());
			// System.err.println(heads[i].getName() + "    =      " + heads[i].getValue());
		}
		HttpEntity entity = response.getEntity();
		long contentLength = httpComponent.getMaxContent();
		if (entity != null && entity.getContentLength() > 0) {
			if (entity.getContentLength() < contentLength)
				contentLength = entity.getContentLength();
		}
		// System.out.println("Response content length: " + entity.getContentLength());
		// System.out.println("Chunked?: " + entity.isChunked());

		String contentType = this.headers.get(Response.CONTENT_TYPE);
		String charSet = HttpBase.parseCharacterEncoding(contentType);
		// always read content. Sometimes content is useful to find a cause
		// for error.
		// HttpEntity entity = response.getEntity();
		// System.out.println(charSet);
		InputStream in = entity.getContent();
		try {
			byte[] buffer = new byte[HttpBase.BUFFER_SIZE];
			int bufferFilled = 0;
			int totalRead = 0;
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			while ((bufferFilled = in.read(buffer, 0, buffer.length)) > 0 && totalRead + bufferFilled <= contentLength) {
				totalRead += bufferFilled;
				out.write(buffer, 0, bufferFilled);
			}
			this.content = out.toByteArray();
		} catch (SocketTimeoutException e) {
			if (this.code == 200) {
				throw new IOException(e.toString());
			}
			// for codes other than 200 OK, we are fine with empty content
		} finally {
			try {
				if (in != null) {
					in.close();
				}
				EntityUtils.consume(entity);
				if (null != httpRequest) {
					httpRequest.abort();
				}
			} catch (Exception e) {
			}
		}

		StringBuilder fetchTrace = null;
		if (HttpComponent.LOG.isTraceEnabled()) {
			// Trace message
			fetchTrace = new StringBuilder("url: " + url + "; status code: " + this.code + "; bytes received: "
					+ this.content.length);
			if (getHeader(Response.CONTENT_LENGTH) != null)
				fetchTrace.append("; Content-Length: " + getHeader(Response.CONTENT_LENGTH));
			if (getHeader(Response.LOCATION) != null)
				fetchTrace.append("; Location: " + getHeader(Response.LOCATION));
		}
		// Extract gzip, x-gzip and deflate content
		if (this.content != null) {
			// check if we have to uncompress it
			String contentEncoding = this.headers.get(Response.CONTENT_ENCODING);
			// System.out.println("content.length=" + this.content.length + "  contentEncoding:" + contentEncoding);
			if (contentEncoding != null && HttpComponent.LOG.isTraceEnabled())
				fetchTrace.append("; Content-Encoding: " + contentEncoding);
			if ("gzip".equals(contentEncoding) || "x-gzip".equals(contentEncoding)) {
				this.content = httpComponent.processGzipEncoded(this.content, url);
				if (HttpComponent.LOG.isTraceEnabled())
					fetchTrace.append("; extracted to " + this.content.length + " bytes");
			} else if ("deflate".equals(contentEncoding)) {
				this.content = httpComponent.processDeflateEncoded(this.content, url);
				if (HttpComponent.LOG.isTraceEnabled())
					fetchTrace.append("; extracted to " + this.content.length + " bytes");
			}
		}
		// Logger trace message
		if (HttpComponent.LOG.isTraceEnabled()) {
			HttpComponent.LOG.trace(fetchTrace.toString());
		}
		if (charSet == null) {
			charSet = HtmlParser.sniffCharacterEncoding(content);
			if (charSet != null) {
				this.headers
						.set(Response.CONTENT_TYPE, this.headers.get(Response.CONTENT_TYPE) + ";charset=" + charSet);
				contentType = this.headers.get(Response.CONTENT_TYPE);
			} else {
				WebPage page = new WebPage();
				page.setContent(ByteBuffer.wrap(this.content));
				EncodingDetector detector = new EncodingDetector(httpComponent.conf);
				detector.autoDetectClues(page, true);
				detector.addClue(HtmlParser.sniffCharacterEncoding(content), "sniffed");
				String encoding = detector.guessEncoding(page,
						httpComponent.conf.get("parser.character.encoding.default", "windows-1252"));
				if (encoding != null) {
					this.headers.set(Response.CONTENT_TYPE, this.headers.get(Response.CONTENT_TYPE) + ";charset="
							+ charSet);
					contentType = this.headers.get(Response.CONTENT_TYPE);
					charSet = encoding;
				}
			}
		}
		if (charSet != null && !charSet.equals("utf-8")) {
			if (contentType != null)
				this.headers.set(Response.CONTENT_TYPE, contentType.replace(charSet, "utf-8"));
			if (null == content) {
				content = new byte[0];
				return;
			}
			String txt = new String(content, charSet);
			content = txt.getBytes("utf-8");
		}
	}

	@Override
	public int getCode() {
		return this.code;
	}

	@Override
	public byte[] getContent() {
		return this.content;
	}

	@Override
	public String getHeader(String name) {
		return this.headers.get(name);
	}

	@Override
	public Metadata getHeaders() {
		return this.headers;
	}

	@Override
	public URL getUrl() {
		return this.url;
	}

	public static void main(String[] args) {
		try {

			HttpComponent http = new HttpComponent();
			http.setConf(NutchConfiguration.create());
			http.configureClient();
			long l = System.currentTimeMillis();
			URL url = new URL("https://192.168.11.10:10008/ServletApp");
			WebPage page = new WebPage();
			boolean followRedirects = true;
			Response response = new HttpComponentResponse(http, url, page, followRedirects);
			System.out.println(new String(response.getContent(), "utf-8"));
			System.err.println(System.currentTimeMillis() - l);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
