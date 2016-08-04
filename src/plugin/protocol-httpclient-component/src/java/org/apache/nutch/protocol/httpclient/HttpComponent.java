package org.apache.nutch.protocol.httpclient;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HTTP;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.http.api.HttpBase;
import org.apache.nutch.protocol.httpclient.bean.LoggerInfo;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.urlfilter.UrlPathMatch.UrlNodeConfig;
import org.apache.nutch.util.DeflateUtils;
import org.apache.nutch.util.GZIPUtils;
import org.apache.nutch.util.TestLogUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.crawl.App;

public class HttpComponent extends HttpBase {
	public static final Logger LOG = LoggerFactory.getLogger("org.apache.hadoop.protocl.httpclient.component");
	private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();
	private PoolingClientConnectionManager poolingClientConnectionManager = new PoolingClientConnectionManager();
	public DefaultHttpClient httpClient = new DefaultHttpClient(poolingClientConnectionManager);
	public static ArrayList<BasicHeader> defaultHttpHeaders = null;
	int maxThreadsTotal = 20;
	int defaultThreadsTotal = 10;
	protected Configuration conf;

	private String localExplorerProxyAddress;
	private String localExplorerProxyUserName;
	private String localExplorerProxyPassword;

	static {
		FIELDS.add(WebPage.Field.MODIFIED_TIME);
		FIELDS.add(WebPage.Field.HEADERS);
	}

	public HttpComponent() {
		super(LOG);
		httpClient.setRedirectStrategy(new HttpClientRedirectStrategy());
	}

	public synchronized DefaultHttpClient getClient() {
		return httpClient;
	}

	/**
	 * @param url
	 * @param datum
	 * @param redirect
	 * @param host
	 * @param loginUrlLst
	 * @return
	 * @throws ProtocolException
	 * @throws IOException
	 */
	public Response getResponse(URL url, WebPage page, boolean redirect) throws ProtocolException, IOException {
		int crawlType = page.getCrawlType();
		TestLogUtil.log("CrawlType:" + crawlType);
		if (crawlType == 0) {
			crawlType = 3;
		}
		Response response = null;
		IOException ex = null;
		if ((crawlType & 1) == 1) {
			try {
				UrlNodeConfig nodeConfig = page.nodeConfig;
				HttpParams httpParams = httpClient.getParams();
				httpParams.removeParameter(ClientPNames.DEFAULT_HEADERS);
				if (nodeConfig != null && nodeConfig.httpClientCfg != null && nodeConfig.httpClientCfg.size() > 0) {
					ArrayList<BasicHeader> httpHeaders = new ArrayList<BasicHeader>();
					HashMap<String, String> map = nodeConfig.httpClientCfg;
					for (String key : map.keySet()) {
						httpHeaders.add(new BasicHeader(key, map.get(key)));
					}
					if (!map.containsKey("Accept-Language"))
						httpHeaders.add(new BasicHeader("Accept-Language", acceptLanguage));
					if (!map.containsKey("Accept-Charset"))
						httpHeaders.add(new BasicHeader("Accept-Charset", acceptCharset));
					if (!map.containsKey("Accept"))
						httpHeaders.add(new BasicHeader("Accept", accept));
					if (!map.containsKey("Accept-Encoding"))
						httpHeaders.add(new BasicHeader("Accept-Encoding", "x-gzip, gzip, deflate"));
					httpParams.setParameter(ClientPNames.DEFAULT_HEADERS, httpHeaders);
				} else {
					httpParams.setParameter(ClientPNames.DEFAULT_HEADERS, defaultHttpHeaders);
				}
				if (App.thisrun != null)
					App.thisrun.setCookies(httpClient);

				long l = System.currentTimeMillis();
				response = new HttpComponentResponse(this, url, page, true);
				System.out.println((new Date().toLocaleString()) + " code:" + response.getCode() + "  len:"
						+ response.getContent().length + " time:" + (System.currentTimeMillis() - l) + "  url:" + url);
				int code = response.getCode();
				if (code == 200) {
					return response;
				} else if (code > 0) {
					System.err.println(new String(response.getContent()));
					throw new IOException("fetch code:" + code + "  url" + url);
				}
				TestLogUtil.logPrefixBlank(" ResposeCode:" + code);
				if ((code >= 300 && code < 400 && code != 304) || code == 401) {// 需要登陆，重新请求
					if (resolveCredentials(page)) {
						TestLogUtil.logPrefixBlank("Server login, validate credentail!");
						response = new HttpComponentResponse(this, url, page, true);
					}
					// // 若服务端登录失败，并且客户端的登陆JS存在，则使用客户端登录
					// code = response.getCode();
					// String loginJs = page.nodeConfig.loginJS;
					// TestLogUtil.logPrefixBlank("Server login, Second ResposeCode:" + code);
					// if (!((code >= 300 && code < 400 && code != 304) || code == 401) || (null == loginJs ||
					// loginJs.trim().length() <=
					// 0)) {
					// TestLogUtil.logPrefixBlank("Server login success");
					// return response;
					// }
					// TestLogUtil.logPrefixBlank("Server login fail, loginJs:" + loginJs);
				}
			} catch (Exception e) {
				// poolingClientConnectionManager = new PoolingClientConnectionManager();
				// this.httpClient = new DefaultHttpClient(poolingClientConnectionManager);
				System.out.print("HttpComponent 109:" + url + " response:" + response + " message:" + e.getMessage());
				ex = new IOException(e);
				FetcherJob.LOG.error("HttpComponent 110 fetch:" + url + " error:" + ex.getMessage());
				TestLogUtil.logPrefixBlank("Server login, fetch:" + url + " error:" + ex.getMessage());
			}
		}
		if ((crawlType & 2) == 2) {
			try {
				TestLogUtil.logPrefixBlank("Client login");
				long l = System.currentTimeMillis();
				response = new HttpComponentResponse(this, url, page, false);
				System.out.println((new Date().toLocaleString()) + " code:" + response.getCode() + " time:"
						+ (System.currentTimeMillis() - l) + "  url:" + url);
				if (response != null) {
					TestLogUtil.log("Client login, Response contentLength:" + response.getContent() == null ? "0"
							: new String(response.getContent()).length());
					int code = response.getCode();
					if (code != 200 && ex != null) {
						System.out.println("client failed throws IOException to Httpbase");
						throw ex;
					}
					return response;
				}
			} catch (Exception e) {
				System.out.print("HttpComponent 127:" + url + " message:");
				e.printStackTrace(System.out);
				FetcherJob.LOG.error("HttpComponent 129:fetch:" + url + " error:" + e.getMessage());
				TestLogUtil.logPrefixBlank("Client login, fetch:" + url + " error:" + e.getMessage());
				if (ex == null)
					ex = new IOException(e);
				else {
					ex = new IOException(e.getMessage(), ex);
				}
			}
		}
		if (ex != null) {
			System.out.println("throws IOException to Httpbase");
			throw ex;
		}
		return response;
	}

	public void setConf(Configuration conf) {
		super.setConf(conf);
		this.conf = conf;
		this.maxThreadsTotal = conf.getInt("fetcher.threads.fetch", 10);
		this.localExplorerProxyAddress = conf.get("auchor.local.explorer.proxy.address", null);
		this.localExplorerProxyUserName = conf.get("auchor.local.explorer.proxy.username", null);
		this.localExplorerProxyPassword = conf.get("auchor.local.explorer.proxy.password", null);
		this.configureClient();
	}

	/**
	 * Configures the HTTP client
	 */
	@SuppressWarnings("deprecation")
	public void configureClient() {
		if (defaultHttpHeaders == null) {
			defaultHttpHeaders = new ArrayList<BasicHeader>();
			HttpParams httpParams = httpClient.getParams();
			httpClient.getParams().setParameter("http.protocol.cookie-policy", CookiePolicy.BROWSER_COMPATIBILITY);
			httpClient.getParams().setParameter("http.protocol.content-charset", HTTP.UTF_8);
			httpParams.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, timeout);
			httpParams.setParameter(CoreConnectionPNames.SO_TIMEOUT, timeout * 3);
			httpParams.setParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, BUFFER_SIZE);
			this.poolingClientConnectionManager.setMaxTotal(maxThreadsTotal);
			this.poolingClientConnectionManager.setDefaultMaxPerRoute(defaultThreadsTotal);
			// Set up an HTTPS socket factory that accepts self-signed certs.
			// Set the User Agent in the header
			// headers.add(new BasicHeader("User-Agent", userAgent));
			// prefer English
			defaultHttpHeaders.add(new BasicHeader("Accept-Language", acceptLanguage));
			// prefer UTF-8
			defaultHttpHeaders.add(new BasicHeader("Accept-Charset", acceptCharset));
			// prefer understandable formats
			defaultHttpHeaders.add(new BasicHeader("Accept", accept));
			// accept gzipped content
			defaultHttpHeaders.add(new BasicHeader("Accept-Encoding", "x-gzip, gzip, deflate"));
			HttpComponent.defaultHttpHeaders
					.add(new BasicHeader("User-Agent",
							"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"));
			httpParams.setParameter(ClientPNames.DEFAULT_HEADERS, defaultHttpHeaders);
		}
	}

	/**
	 * If credentials for the authentication scope determined from the specified <code>url</code> is not already set in
	 * the HTTP client, then this method sets the default credentials to fetch the specified <code>url</code>. If
	 * credentials are found for the authentication scope, the method returns without altering the client.
	 * 
	 * @param rooturl
	 *            URL to be fetched
	 */
	private boolean resolveCredentials(WebPage page) {
		UrlNodeConfig urlNodeConfig = page.nodeConfig;
		TestLogUtil.logPrefixBlank("NeedLogin:" + urlNodeConfig.needLogin);
		if (!urlNodeConfig.needLogin) {
			return false;
		}
		if (null == urlNodeConfig.loginCLASS || urlNodeConfig.loginCLASS.trim().length() <= 0) {// 默认登录
			TestLogUtil.logPrefixBlank("Default Login");
			String loginAddress = urlNodeConfig.loginAddress;
			String loginUsername = urlNodeConfig.loginUserName;
			String loginPassword = urlNodeConfig.loginPassword;
			TestLogUtil.logPrefixBlank("Default Login info:" + " loginUsername:" + loginUsername + " ,loginPassword:"
					+ loginPassword + " ,loginAddress:" + loginAddress);
			try {
				URL url = new URL(loginAddress);
				this.httpClient.getCredentialsProvider().setCredentials(
						new AuthScope(url.getHost(), AuthScope.ANY_PORT),
						new UsernamePasswordCredentials(loginUsername, loginPassword));
				return true;
			} catch (MalformedURLException e) {
				e.printStackTrace(System.out);
				TestLogUtil.logPrefixBlank("Default login error, loginaddress=" + loginAddress + ", msg:"
						+ e.getMessage());
				LOG.error("Default login error, loginaddress=" + loginAddress + ", msg:" + e.getMessage());
			}
		} else {// 反射登录
			String loginClassName = urlNodeConfig.loginCLASS;
			TestLogUtil.logPrefixBlank("Invoke Login " + "loginClassName:" + loginClassName);
			if (null == loginClassName) {
				return false;
			}
			try {
				Class<?> loginClazz = Class.forName(loginClassName);
				Class<?> paramType[] = new Class[2];
				paramType[0] = HttpClient.class;
				paramType[1] = WebPage.class;

				Object paramParam[] = new Object[2];
				paramParam[0] = this.httpClient;
				paramParam[1] = page;
				Method method = loginClazz.getMethod("login", paramType);
				method.invoke(loginClazz.newInstance(), paramParam);
				return true;
			} catch (Exception e) {
				LOG.error("Reflect login error, loginClassName=" + loginClassName + ", msg:" + e.getMessage());
				TestLogUtil.logPrefixBlank("Reflect login error, loginClassName=" + loginClassName + ", msg:"
						+ e.getMessage());
				e.printStackTrace(System.out);
			}
		}
		return false;
	}

	public byte[] processGzipEncoded(byte[] compressed, URL url) throws IOException {

		if (LOG.isTraceEnabled()) {
			LOG.trace(new LoggerInfo("uncompressing....").getInfo());
		}

		byte[] content;
		if (getMaxContent() >= 0) {
			content = GZIPUtils.unzipBestEffort(compressed, getMaxContent());
		} else {
			content = GZIPUtils.unzipBestEffort(compressed);
		}

		if (content == null)
			throw new IOException("unzipBestEffort returned null");

		if (LOG.isTraceEnabled()) {
			LOG.trace(new LoggerInfo("fetched " + compressed.length + " bytes of compressed content (expanded to "
					+ content.length + " bytes) from " + url).getInfo());
		}
		return content;
	}

	public byte[] processDeflateEncoded(byte[] compressed, URL url) throws IOException {

		if (LOG.isTraceEnabled()) {
			LOG.trace(new LoggerInfo("inflating....").getInfo());
		}

		byte[] content = DeflateUtils.inflateBestEffort(compressed, getMaxContent());

		if (content == null)
			throw new IOException("inflateBestEffort returned null");

		if (LOG.isTraceEnabled()) {
			LOG.trace(new LoggerInfo("fetched " + compressed.length + " bytes of compressed content (expanded to "
					+ content.length + " bytes) from " + url).getInfo());
		}
		return content;
	}

	@Override
	public Collection<Field> getFields() {
		return FIELDS;
	}

	public String getLocalExplorerProxyHost() {
		return localExplorerProxyAddress;
	}

	public String getLocalExplorerProxyUser() {
		return localExplorerProxyUserName;
	}

	public String getLocalExplorerProxyPass() {
		return localExplorerProxyPassword;
	}
}
