package org.apache.nutch.protocol.httpclient.commponent.site;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.protocol.httpclient.bean.CredentialsBean;
import org.apache.nutch.protocol.httpclient.commponent.HttpComponent;
import org.apache.nutch.protocol.httpclient.commponent.HttpResponseComponent;

public class HttpResponseComponentServlet extends HttpResponseComponent
{
	public HttpResponseComponentServlet(HttpComponent http, CredentialsBean cb,
			URL url, CrawlDatum datum, boolean followRedirects, String host)
	{
		super(http, cb, url, datum, followRedirects, host);
	}

	@SuppressWarnings("deprecation")
	@Override
	public boolean login(HttpClient client, CredentialsBean credentialsBean)
			throws ClientProtocolException, IOException
	{
		if (null == credentialsBean)
		{
			return false;
		}
		
		this.setLoginTimes(credentialsBean);
		String loginaction = credentialsBean.getLoginaction();
		Map<String, String> paraMap = credentialsBean.getParaMap();
		if (null == loginaction || loginaction.trim().length() <= 0)
		{
			return false;
		}

		HttpPost httpPost2 = new HttpPost(loginaction);
		List<NameValuePair> params = new ArrayList<NameValuePair>();
		Iterator<String> iterator = paraMap.keySet().iterator();
		while (iterator.hasNext())
		{
			String key = iterator.next();
			String value = paraMap.get(key);
			NameValuePair pair = new BasicNameValuePair(key,
					value);
			params.add(pair);
		}
		
		httpPost2.setEntity(new UrlEncodedFormEntity(params, HTTP.UTF_8));
		HttpResponse response = client.execute(httpPost2);
		int code = response.getStatusLine().getStatusCode();
		return HttpStatus.SC_OK == code ? true : false;
	}

	@Override
	public boolean isLogin(HttpClient client, CredentialsBean credentialsBean)
	{
		return false;
	}
}
