package org.apache.nutch.protocol.httpclient.commponent;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.NTCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.client.params.HttpClientParams;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpParams;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.httpclient.bean.CredentialsBean;
import org.apache.nutch.protocol.httpclient.bean.LoggerInfo;
import org.apache.nutch.protocol.httpclient.commponent.site.HttpResponseComponentDefualt;
import org.apache.nutch.protocol.httpclient.commponent.site.HttpResponseComponentServlet;
import org.apache.nutch.protocol.httpclient.commponent.site.HttpResponseComponentSina;
import org.apache.nutch.util.DeflateUtils;
import org.apache.nutch.util.GZIPUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class HttpComponent
{
	public static final Logger LOG = LoggerFactory
			.getLogger("org.apache.hadoop.protocl.httpclient.component");
	protected int maxContent = 64 * 1024;
	private PoolingClientConnectionManager poolingClientConnectionManager = new PoolingClientConnectionManager();
	private DefaultHttpClient httpClient = new DefaultHttpClient(
			poolingClientConnectionManager);
	private static Configuration conf;
	private static String authFile;
	private static String agentHost;
	private static String defaultUsername;
	private static String defaultPassword;
	private static String defaultRealm;
	private static String defaultScheme;
	int maxThreadsTotal = 20;
	int defaultThreadsTotal = 10;
	private String proxyUsername;
	private String proxyPassword;
	private String proxyRealm;
	private static boolean authRulesRead = false;

	/**
	 * 以下参数来自HttpBase的参数
	 */
	private int timeout;
	private int BUFFER_SIZE;
	private String userAgent;
	private String acceptLanguage;
	private boolean useProxy;
	private String proxyHost;
	private int proxyPort;

	public HttpComponent()
	{
	}

	public synchronized HttpClient getClient()
	{
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
	public Response getResponse(URL url, CrawlDatum datum, boolean redirect,
			String host) throws ProtocolException, IOException
	{
		resolveCredentials(url);
		CredentialsBean credentialsBean = HttpComponentInit.credentialsMap
				.get(host);
		HttpResponseComponent response = null;
		if (null != credentialsBean
				&& "HttpResponseComponentSina".equalsIgnoreCase(credentialsBean
						.getHandle()))
		{
			response = new HttpResponseComponentSina(this, credentialsBean,
					url, datum, redirect, host);
		}
		else if(null != credentialsBean
				&& "HttpResponseComponentServlet".equalsIgnoreCase(credentialsBean
						.getHandle()))
		{
			response = new HttpResponseComponentServlet(this, credentialsBean,
					url, datum, redirect, host);
		}
		else
		{
			response = new HttpResponseComponentDefualt(this, credentialsBean,
					url, datum, redirect, host);
		}

		return response;
	}

	public void setConf(Configuration conf)
	{
		this.conf = conf;
		this.maxThreadsTotal = conf.getInt("fetcher.threads.fetch", 10);
		this.proxyUsername = conf.get("http.proxy.username", "");
		this.proxyPassword = conf.get("http.proxy.password", "");
		this.proxyRealm = conf.get("http.proxy.realm", "");
		agentHost = conf.get("http.agent.host", "");
		authFile = conf.get("http.auth.file", "");

		configureClient();
		try
		{
			setCredentials();
		}
		catch (Exception ex)
		{
			if (LOG.isErrorEnabled())
			{
				LOG.error(new LoggerInfo("Could not read " + authFile + " : "
						+ ex.getMessage()).getInfo());
			}
		}
	}

	/**
	 * Configures the HTTP client
	 */
	private void configureClient()
	{
		HttpParams httpParams = httpClient.getParams();
		httpClient.getParams().setParameter("http.protocol.cookie-policy",
				CookiePolicy.BROWSER_COMPATIBILITY);
		httpParams.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT,
				HttpComponentInit.HTTPCONNECTIONTIMEOUT);
		httpParams.setParameter(CoreConnectionPNames.SO_TIMEOUT, HttpComponentInit.HTTPSOCKETTIMEOUT);
		httpParams.setParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE,
				BUFFER_SIZE);
		poolingClientConnectionManager.setMaxTotal(maxThreadsTotal);
		this.poolingClientConnectionManager.setDefaultMaxPerRoute(defaultThreadsTotal);

		// executeMethod(HttpMethod) seems to ignore the connection timeout on
		// the connection manager.
		// set it explicitly on the HttpClient.
		HttpClientParams.setConnectionManagerTimeout(httpParams, timeout);

		// Set up an HTTPS socket factory that accepts self-signed certs.
		ArrayList<BasicHeader> headers = new ArrayList<BasicHeader>();
		// Set the User Agent in the header
		headers.add(new BasicHeader("User-Agent", userAgent));
		// prefer English
		headers.add(new BasicHeader("Accept-Language", acceptLanguage));
		// prefer UTF-8
		headers.add(new BasicHeader("Accept-Charset",
				"utf-8,ISO-8859-1;q=0.7,*;q=0.7"));
		// prefer understandable formats
		headers.add(new BasicHeader(
				"Accept",
				"text/html,application/xml;q=0.9,application/xhtml+xml,text/xml;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5"));
		// accept gzipped content
		// headers.add(new BasicHeader("Accept-Encoding",
		// "x-gzip, gzip, deflate"));
		httpParams.setParameter(ClientPNames.DEFAULT_HEADERS, headers);

		// // HTTP proxy server details
//		if (useProxy)
//		{
			// httpClient.setProxy(proxyHost, proxyPort);
//			if (proxyUsername.length() > 0)
//			{
//				AuthScope proxyAuthScope = getAuthScope(this.proxyHost,
//						this.proxyPort, this.proxyRealm);
//				NTCredentials proxyCredentials = new NTCredentials(
//						this.proxyUsername, this.proxyPassword, this.agentHost,
//						this.proxyRealm);
				// client.getState().setProxyCredentials(
				// proxyAuthScope, proxyCredentials);
//			}
//		}
	}

	/**
	 * Reads authentication configuration file (defined as 'http.auth.file' in
	 * Nutch configuration file) and sets the credentials for the configured
	 * authentication scopes in the HTTP client object.
	 * @throws ParserConfigurationException If a document builder can not be
	 *             created.
	 * @throws SAXException If any parsing error occurs.
	 * @throws IOException If any I/O error occurs.
	 */
	private synchronized void setCredentials()
			throws ParserConfigurationException, SAXException, IOException
	{
		if (authRulesRead)
			return;

		authRulesRead = true; // Avoid re-attempting to read
		InputStream is = conf.getConfResourceAsInputStream(authFile);
		if (is != null)
		{
			Document doc = DocumentBuilderFactory.newInstance()
					.newDocumentBuilder().parse(is);

			Element rootElement = doc.getDocumentElement();
			if (!"auth-configuration".equals(rootElement.getTagName()))
			{
				if (LOG.isWarnEnabled())
					LOG.warn(new LoggerInfo("Bad auth conf file: root element <"
							+ rootElement.getTagName() + "> found in "
							+ authFile + " - must be <auth-configuration>").getInfo());
			}

			// For each set of credentials
			NodeList credList = rootElement.getChildNodes();
			for (int i = 0; i < credList.getLength(); i++)
			{
				Node credNode = credList.item(i);
				if (!(credNode instanceof Element))
					continue;

				Element credElement = (Element) credNode;
				if (!"credentials".equals(credElement.getTagName()))
				{
					continue;
				}

				String username = credElement.getAttribute("username");
				String password = credElement.getAttribute("password");

				// For each authentication scope
				NodeList scopeList = credElement.getChildNodes();
				for (int j = 0; j < scopeList.getLength(); j++)
				{
					Node scopeNode = scopeList.item(j);
					if (!(scopeNode instanceof Element))
						continue;

					Element scopeElement = (Element) scopeNode;

					if ("default".equals(scopeElement.getTagName()))
					{
						// Determine realm and scheme, if any
						String realm = scopeElement.getAttribute("realm");
						String scheme = scopeElement.getAttribute("scheme");

						// Set default credentials
						defaultUsername = username;
						defaultPassword = password;
						defaultRealm = realm;
						defaultScheme = scheme;

						if (LOG.isTraceEnabled())
						{
							LOG.trace(new LoggerInfo("Credentials - username: " + username
									+ "; set as default" + " for realm: "
									+ realm + "; scheme: " + scheme).getInfo());
						}
					}
					else if ("authscope".equals(scopeElement.getTagName()))
					{
						// Determine authentication scope details
						String host = scopeElement.getAttribute("host");
						int port = -1; // For setting port to
										// AuthScope.ANY_PORT
						try
						{
							port = Integer.parseInt(scopeElement
									.getAttribute("port"));
						}
						catch (Exception ex)
						{
							// do nothing, port is already set to any port
						}
						String realm = scopeElement.getAttribute("realm");
						String scheme = scopeElement.getAttribute("scheme");

						// Set credentials for the determined scope
						AuthScope authScope = getAuthScope(host, port, realm,
								scheme);
						NTCredentials credentials = new NTCredentials(username,
								password, agentHost, realm);

						httpClient.getCredentialsProvider().setCredentials(
								authScope, credentials);

						if (LOG.isTraceEnabled())
						{
							LOG.trace(new LoggerInfo("Credentials - username: " + username
									+ "; set for AuthScope - " + "host: "
									+ host + "; port: " + port + "; realm: "
									+ realm + "; scheme: " + scheme).getInfo());
						}
					}
					else
					{
						if (LOG.isWarnEnabled())
							LOG.warn(new LoggerInfo("Bad auth conf file: Element <"
									+ scopeElement.getTagName()
									+ "> not recognized in " + authFile
									+ " - expected <authscope>").getInfo());
					}
				}
			}
			is.close();
		}
	}

	/**
	 * If credentials for the authentication scope determined from the specified
	 * <code>url</code> is not already set in the HTTP client, then this method
	 * sets the default credentials to fetch the specified <code>url</code>. If
	 * credentials are found for the authentication scope, the method returns
	 * without altering the client.
	 * @param url URL to be fetched
	 */
	private void resolveCredentials(URL url)
	{
		if (defaultUsername != null && defaultUsername.length() > 0)
		{
			int port = url.getPort();
			if (port == -1)
			{
				if ("https".equals(url.getProtocol()))
					port = 443;
				else
					port = 80;
			}

			AuthScope scope = new AuthScope(url.getHost(), port);

			if (httpClient.getCredentialsProvider().getCredentials(scope) != null)
			{
				if (LOG.isTraceEnabled())
					LOG.trace(new LoggerInfo("Pre-configured credentials with scope - host: "
							+ url.getHost() + "; port: " + port
							+ "; found for url: " + url).getInfo());

				// Credentials are already configured, so do nothing and return
				return;
			}

			if (LOG.isTraceEnabled())
				LOG.trace(new LoggerInfo("Pre-configured credentials with scope -  host: "
						+ url.getHost() + "; port: " + port
						+ "; not found for url: " + url).getInfo());

			AuthScope serverAuthScope = getAuthScope(url.getHost(), port,
					defaultRealm, defaultScheme);

			NTCredentials serverCredentials = new NTCredentials(
					defaultUsername, defaultPassword, agentHost, defaultRealm);

			httpClient.getCredentialsProvider().setCredentials(serverAuthScope,
					serverCredentials);
		}
	}

	/**
	 * Returns an authentication scope for the specified <code>host</code>,
	 * <code>port</code>, <code>realm</code> and <code>scheme</code>.
	 * @param host Host name or address.
	 * @param port Port number.
	 * @param realm Authentication realm.
	 * @param scheme Authentication scheme.
	 */
	private static AuthScope getAuthScope(String host, int port, String realm,
			String scheme)
	{
		if (host.length() == 0)
			host = null;

		if (port < 0)
			port = -1;

		if (realm.length() == 0)
			realm = null;

		if (scheme.length() == 0)
			scheme = null;

		return new AuthScope(host, port, realm, scheme);
	}

	/**
	 * Returns an authentication scope for the specified <code>host</code>,
	 * <code>port</code> and <code>realm</code>.
	 * @param host Host name or address.
	 * @param port Port number.
	 * @param realm Authentication realm.
	 */
	private static AuthScope getAuthScope(String host, int port, String realm)
	{
		return getAuthScope(host, port, realm, "");
	}

	public int getMaxContent()
	{
		return maxContent;
	}

	public void setMaxContent(int maxContent)
	{
		this.maxContent = maxContent;
	}

	public byte[] processGzipEncoded(byte[] compressed, URL url)
			throws IOException
	{

		if (LOG.isTraceEnabled())
		{
			LOG.trace(new LoggerInfo("uncompressing....").getInfo());
		}

		byte[] content;
		if (getMaxContent() >= 0)
		{
			content = GZIPUtils.unzipBestEffort(compressed, getMaxContent());
		}
		else
		{
			content = GZIPUtils.unzipBestEffort(compressed);
		}

		if (content == null)
			throw new IOException("unzipBestEffort returned null");

		if (LOG.isTraceEnabled())
		{
			LOG.trace(new LoggerInfo("fetched " + compressed.length
					+ " bytes of compressed content (expanded to "
					+ content.length + " bytes) from " + url).getInfo());
		}
		return content;
	}

	public byte[] processDeflateEncoded(byte[] compressed, URL url)
			throws IOException
	{

		if (LOG.isTraceEnabled())
		{
			LOG.trace(new LoggerInfo("inflating....").getInfo());
		}

		byte[] content = DeflateUtils.inflateBestEffort(compressed,
				getMaxContent());

		if (content == null)
			throw new IOException("inflateBestEffort returned null");

		if (LOG.isTraceEnabled())
		{
			LOG.trace(new LoggerInfo("fetched " + compressed.length
					+ " bytes of compressed content (expanded to "
					+ content.length + " bytes) from " + url).getInfo());
		}
		return content;
	}

	/**
	 * login in
	 * @param httpResponseComponent
	 * @param cb
	 * @param url
	 * @throws ClientProtocolException
	 * @throws IOException
	 */
	public void login(HttpResponseComponent httpResponseComponent,
			CredentialsBean cb, String url)
	{
		if (!(httpResponseComponent instanceof HttpResponseComponentSina
				|| httpResponseComponent instanceof HttpResponseComponentServlet))
		{
			return;
		}
		
		if (cb.isLogin())
		{
			return;
		}

		synchronized (HttpComponent.class)
		{
			if (cb.isLogin())
			{
				return;
			}
			try
			{
				boolean success = false;
				while(!success && cb.getLoginCurrentTime() < CredentialsBean.LOGINMAXTIME)
				{
					success = httpResponseComponent.login(this.httpClient, cb);
					if (!success)
					{
						cb.setLoginCurrentTime(cb.getLoginCurrentTime() + 1);
						LOG.error(new LoggerInfo("HttpComponent is logining."
								+ " HttpComponent:" + this + " URL:" + url 
								+ " retryTime:" + cb.getLoginCurrentTime()).getInfo());
					}
				}
				
				cb.setLogin(success);
			}
			catch (Exception e)
			{
				cb.setLogin(false);
				LOG.error(new LoggerInfo("Exception Login:" + e.getMessage()
						+ " HttpComponent:" + this + " URL:" + url).getInfo());
				LOG.error(new LoggerInfo("HttpComponent's Login status."
						+ " HttpComponent:" + this + " URL:"
						+ url + " status:" + cb.isLogin()).getInfo());
			}

			HttpComponentInit.setLogin(cb);
			LOG.info(new LoggerInfo("HttpComponent's Login status."
					+ " HttpComponent:" + this + " URL:"
					+ url + " status:" + cb.isLogin()).getInfo());
		}
	}

	public int getTimeout()
	{
		return timeout;
	}

	public void setTimeout(int timeout)
	{
		this.timeout = timeout;
	}

	public int getBUFFER_SIZE()
	{
		return BUFFER_SIZE;
	}

	public void setBUFFER_SIZE(int bUFFER_SIZE)
	{
		BUFFER_SIZE = bUFFER_SIZE;
	}

	public String getUserAgent()
	{
		return userAgent;
	}

	public void setUserAgent(String userAgent)
	{
		this.userAgent = userAgent;
	}

	public String getAcceptLanguage()
	{
		return acceptLanguage;
	}

	public void setAcceptLanguage(String acceptLanguage)
	{
		this.acceptLanguage = acceptLanguage;
	}

	public boolean isUseProxy()
	{
		return useProxy;
	}

	public void setUseProxy(boolean useProxy)
	{
		this.useProxy = useProxy;
	}

	public String getProxyHost()
	{
		return proxyHost;
	}

	public void setProxyHost(String proxyHost)
	{
		this.proxyHost = proxyHost;
	}

	public int getProxyPort()
	{
		return proxyPort;
	}

	public void setProxyPort(int proxyPort)
	{
		this.proxyPort = proxyPort;
	}
}
