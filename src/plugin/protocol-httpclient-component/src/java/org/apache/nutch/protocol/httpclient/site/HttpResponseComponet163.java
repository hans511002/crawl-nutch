package org.apache.nutch.protocol.httpclient.site;

import org.apache.http.client.HttpClient;
import org.apache.nutch.protocol.httpclient.IHttpLogin;
import org.apache.nutch.storage.WebPage;

public class HttpResponseComponet163 implements IHttpLogin {

	@Override
	public boolean login(HttpClient client, WebPage page) throws Exception {
		return false;
	}

	@Override
	public boolean isLogin(HttpClient client, WebPage page, String loginValidateAddress) {
		return false;
	}
}
