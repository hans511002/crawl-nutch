package org.apache.nutch.protocol.httpclient.site;

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
import org.apache.nutch.protocol.httpclient.IHttpLogin;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.urlfilter.UrlPathMatch.UrlNodeConfig;

public class HttpResponseComponentServlet implements IHttpLogin {

	@SuppressWarnings("deprecation")
	@Override
	public boolean login(HttpClient client, WebPage page) throws Exception {
		if (null == page || null == client) {
			return false;
		}

		// 解析地址参数
		UrlNodeConfig urlNodeConfig = page.nodeConfig;
		URL url = new URL(urlNodeConfig.loginAddress);
		HttpPost httpPost2 = new HttpPost(urlNodeConfig.loginAddress);
		Map<String, String> param = urlNodeConfig.loginParam;
		List<NameValuePair> nameParams = new ArrayList<NameValuePair>();
		if (null != param && param.size() > 0) {
			Iterator<?> iterator = param.keySet().iterator();
			while (iterator.hasNext()) {
				Object key = iterator.next();
				if (key != null && param.get(key) != null) {
					NameValuePair pair = new BasicNameValuePair(key.toString(), param.get(key).toString());
					nameParams.add(pair);
				}
			}
		}

		httpPost2.setEntity(new UrlEncodedFormEntity(nameParams, HTTP.UTF_8));
		HttpResponse response = null;
		if ("https".equals(url.getProtocol())) {
			response = HttpClientUtils.sendSSLPostRequest(client, httpPost2);
		} else {
			response = client.execute(httpPost2);
		}

		int code = response.getStatusLine().getStatusCode();
		return HttpStatus.SC_OK == code ? true : false;
	}

	@Override
	public boolean isLogin(HttpClient client, WebPage page, String loginValidateAddress) {
		if (null == loginValidateAddress || loginValidateAddress.trim().length() <= 0) {
			return false;
		}
		HttpPost getMethod = new HttpPost(loginValidateAddress);
		try {
			HttpResponse response = client.execute(getMethod);
			if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
				System.out.println("true");
				return true;
			}
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return false;
	}
}
