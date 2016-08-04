package org.apache.nutch.protocol.httpclient.test.netBase;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

public class Mail163Test {
    public static final String SESSION_INIT = "http://mail.163.com";
    public static final String LOGIN_URL = "https://ssl.mail.163.com/entry/coremail/fcg/ntesdoor2?df=webmail163&from=web&funcid=loginone&iframe=1&language=-1&net=t&passtype=1&product=mail163&race=-2_-2_-2_db&style=-1&uid=";
    public static final String MAIL_LIST_URL = "http://twebmail.mail.163.com/js4/s?sid={0}&func=mbox:listMessages";

    /**
     * @param args
     */
    public static void main(String[] args) {
	HttpClientHelper hc = new HttpClientHelper(true);
	HttpResult lr = hc.get(SESSION_INIT);// 目的是得到 csrfToken 类似
	// 拼装登录信息
	Map<String, String> data = new HashMap<String, String>();
	data.put("url2", "http://mail.163.com/errorpage/err_163.htm");
	data.put("savelogin", "0");
	data.put("username", "wanghaoms@163.com");
	data.put("password", "pb01200230");
	lr = hc.post(LOGIN_URL, data, setHeader());// 执行登录
	Document doc = Jsoup.parse(lr.getHtml());
	String sessionId = doc.select("script").html().split("=")[2];
	sessionId = sessionId.substring(0, sessionId.length() - 2);
	data.clear();
	data.put(
		"var",
		"<?xml version=\"1.0\"?><object><int name=\"fid\">1</int><boolean name=\"skipLockedFolders\">false</boolean><string name=\"order\">date</string><boolean name=\"desc\">true</boolean><int name=\"start\">0</int><int name=\"limit\">50</int><boolean name=\"topFirst\">true</boolean><boolean name=\"returnTotal\">true</boolean><boolean name=\"returnTag\">true</boolean></object>");
	lr = hc.post(MessageFormat.format(MAIL_LIST_URL, sessionId), data, setQueryHeader(sessionId));// 执行登录
	System.out.println(lr.getHtml());
    }

    public static Header[] setHeader() {
	Header[] result = { new BasicHeader("User-Agent", "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)"),
		new BasicHeader("Accept-Encoding", "gzip, deflate"), new BasicHeader("Accept-Language", "zh-CN"),
		new BasicHeader("Cache-Control", "no-cache"), new BasicHeader("Connection", "Keep-Alive"),
		new BasicHeader("Content-Type", "application/x-www-form-urlencoded"), new BasicHeader("Host", "ssl.mail.163.com"),
		new BasicHeader("Referer", "http://mail.163.com/"), new BasicHeader("Accept", "text/html, application/xhtml+xml, */*")

	};
	return result;
    }

    public static Header[] setQueryHeader(String sessionId) {
	Header[] result = { new BasicHeader("User-Agent", "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)"),
		new BasicHeader("Accept-Encoding", "gzip, deflate"), new BasicHeader("Accept-Language", "zh-CN"),
		new BasicHeader("Cache-Control", "no-cache"), new BasicHeader("Connection", "Keep-Alive"),
		new BasicHeader("Content-Type", "application/x-www-form-urlencoded"), new BasicHeader("Host", "twebmail.mail.163.com"),
		new BasicHeader("Referer", "http://twebmail.mail.163.com/js4/index.jsp?sid=" + sessionId),
		new BasicHeader("Accept", "text/javascript")

	};
	return result;
    }
}