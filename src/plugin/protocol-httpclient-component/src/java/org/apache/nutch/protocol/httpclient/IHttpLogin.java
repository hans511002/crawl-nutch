package org.apache.nutch.protocol.httpclient;

import org.apache.http.client.HttpClient;
import org.apache.nutch.storage.WebPage;

public interface IHttpLogin {

    public boolean login(HttpClient client, WebPage page) throws Exception;

    public boolean isLogin(HttpClient client, WebPage page, String loginValidateAddress);
}
