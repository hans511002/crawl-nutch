package org.apache.nutch.protocol.httpclient;

import java.io.IOException;
import java.net.URL;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.http.api.HttpBase;
import org.apache.nutch.protocol.httpclient.bean.CredentialsBean;
import org.apache.nutch.protocol.httpclient.commponent.HttpComponent;
import org.apache.nutch.protocol.httpclient.commponent.HttpComponentInit;
import org.apache.nutch.protocol.httpclient.commponent.HttpComponentManager;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpControl extends HttpBase
{
	public static final Logger LOGGER = LoggerFactory.getLogger("org.apache.hadoop.protocl.httpclient.component");
	protected Configuration conf;
	private Http http;

	public HttpControl()
	{
		super(LOGGER);
	}

	@Override
	protected Response getResponse(URL url, CrawlDatum datum,
			boolean redirect) throws ProtocolException, IOException
	{
		Response response = getHttpComponentResponse(url, datum, redirect);
		if (null != response)
		{
			return response;
		}
		
		// 使用httpclient处理
		if (null == this.http)
		{
			this.http = new Http(this);
			this.http.setTimeout(timeout);
			this.http.setBUFFER_SIZE(BUFFER_SIZE);
			this.http.setUserAgent(userAgent);
			this.http.setAcceptLanguage(acceptLanguage);
			this.http.setUseProxy(useProxy);
			this.http.setProxyHost(proxyHost);
			this.http.setProxyPort(proxyPort);
		}
		response = this.http.getResponse(url, datum, redirect);
		return response;
	}

	/**
	 * do httpComponent request
	 * @param url
	 * @param datum
	 * @param redirect
	 * @return
	 * @throws ProtocolException
	 * @throws IOException
	 */
	private Response getHttpComponentResponse(URL url, CrawlDatum datum,
			boolean redirect) throws ProtocolException, IOException
	{
		// 获取最优匹配host
		String temp = url.getHost();
		Iterator<String> iterator = HttpComponentInit.credentialsMap.keySet().iterator();
		String realHostUrl = "";
		while (iterator.hasNext())
		{
			String tempHostUrl = iterator.next();
			if (temp.endsWith(tempHostUrl))
			{
				realHostUrl = tempHostUrl.length() > realHostUrl
						.length() ? tempHostUrl : realHostUrl;
			}
		}
		
		CredentialsBean credentialsBean = HttpComponentInit.credentialsMap.get(realHostUrl);
		
		// 使用 common HttpClient 处理
		if (null == credentialsBean && !HttpComponentInit.isHttpComponentClient)
		{
			return null;
		}
		
		if (null != credentialsBean && null == credentialsBean.getHandle() 
				&& credentialsBean.getHandle().length()<=0)
		{
			return null;
		}
		
		// 使用http component client处理
		HttpComponent httpComponent = null;
		if (null != credentialsBean)
		{
			httpComponent = HttpComponentManager.getHttpComponent(realHostUrl);
			if (null == httpComponent)
			{
				httpComponent = HttpComponentInit.initHttpComponentClient(
						realHostUrl, this.conf);
				httpComponent.setTimeout(timeout);
				httpComponent.setBUFFER_SIZE(BUFFER_SIZE);
				httpComponent.setUserAgent(userAgent);
				httpComponent.setAcceptLanguage(acceptLanguage);
				httpComponent.setUseProxy(useProxy);
				httpComponent.setProxyHost(proxyHost);
				httpComponent.setProxyPort(proxyPort);
				httpComponent.setMaxContent(maxContent);
			}
			return httpComponent.getResponse(url, datum, redirect, realHostUrl);
		}
		
		// 默认处理
		httpComponent = HttpComponentManager.getHttpComponent(HttpComponentInit.DEFAULT);
		if (null == httpComponent)
		{
			httpComponent = HttpComponentInit.initHttpComponentClient(
					HttpComponentInit.DEFAULT, this.conf);
			httpComponent.setTimeout(timeout);
			httpComponent.setBUFFER_SIZE(BUFFER_SIZE);
			httpComponent.setUserAgent(userAgent);
			httpComponent.setAcceptLanguage(acceptLanguage);
			httpComponent.setUseProxy(useProxy);
			httpComponent.setProxyHost(proxyHost);
			httpComponent.setProxyPort(proxyPort);
			httpComponent.setMaxContent(maxContent);
		}
		return httpComponent.getResponse(url, datum, redirect, null);
	}

	public void setConf(Configuration conf)
	{
		super.setConf(conf);
		this.conf = conf;
		String authFile = conf.get("http.auth.file", "");
		HttpComponentInit.init(authFile, this.conf);
	}
	
	/**
	 * Main method.
	 * @param args Command line arguments
	 */
	public static void main(String[] args) throws Exception
	{
		HttpControl http = new HttpControl();
		http.setConf(NutchConfiguration.create());
		main(http, args);
	}
}
