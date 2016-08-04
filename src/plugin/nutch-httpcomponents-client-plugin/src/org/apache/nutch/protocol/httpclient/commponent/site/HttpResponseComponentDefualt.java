package org.apache.nutch.protocol.httpclient.commponent.site;

import java.io.IOException;
import java.net.URL;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.protocol.httpclient.bean.CredentialsBean;
import org.apache.nutch.protocol.httpclient.commponent.HttpComponent;
import org.apache.nutch.protocol.httpclient.commponent.HttpResponseComponent;

public class HttpResponseComponentDefualt extends HttpResponseComponent
{
	public HttpResponseComponentDefualt(HttpComponent http, CredentialsBean cb, URL url,
			CrawlDatum datum, boolean followRedirects, String host)
	{
		super(http, cb, url, datum, followRedirects, host);
	}

	@Override
	public boolean login(HttpClient client,CredentialsBean credentialsBean) throws ClientProtocolException,
			IOException
	{
		return true;
	}

	@Override
	public boolean isLogin(HttpClient client, CredentialsBean credentialsBean)
	{
		return false;
	}
}
