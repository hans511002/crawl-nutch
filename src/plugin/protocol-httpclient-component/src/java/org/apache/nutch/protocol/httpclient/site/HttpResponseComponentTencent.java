package org.apache.nutch.protocol.httpclient.site;

import java.io.FileReader;
import java.io.IOException;
import java.net.URL;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.util.EntityUtils;
import org.apache.nutch.protocol.httpclient.IHttpLogin;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.urlfilter.UrlPathMatch.UrlNodeConfig;

public class HttpResponseComponentTencent implements IHttpLogin {

	@Override
	public boolean login(HttpClient client, WebPage page) throws Exception {
		if (null == page || null == client || null == page.nodeConfig) {
			return false;
		}
		UrlNodeConfig nodeConfig = page.nodeConfig;
		String username = nodeConfig.loginUserName;
		String password = nodeConfig.loginPassword;
		String loginValidatePath = nodeConfig.loginValidateAddress;

		try {
			/********************* 获取验证码 ***********************/
			HttpGet get = new HttpGet("http://check.ptlogin2.qq.com/check?uin=" + username + "&appid=46000101&ptlang=2052&r="
					+ Math.random());
			get.setHeader("Host", "check.ptlogin2.qq.com");
			get.setHeader("Referer", "http://t.qq.com/?from=11");

			get.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 5.1; rv:13.0) Gecko/20100101 Firefox/13.0");
			HttpResponse response = client.execute(get);

			String entity = EntityUtils.toString(response.getEntity());
			String[] checkNum = entity.substring(entity.indexOf("(") + 1, entity.lastIndexOf(")")).replace("'", "").split(",");
			// System.out.println(checkNum[0]);
			// System.out.println(checkNum[1]);
			// System.out.println(checkNum[2].trim());
			// System.out.println(checkNum[2].trim().replace("\\x", ""));
			String pass = "";

			/******************** *加密密码 ***************************/
			ScriptEngineManager manager = new ScriptEngineManager();
			ScriptEngine engine = manager.getEngineByName("javascript");
			URL js = ClassLoader.getSystemResource("js/qqmd5.js");
			// String jsFileName = "E:/myeclipse/workspace/apache-nutch-2.1/src/plugin/protocol-httpclient-component/src/java/js/qqmd5.js";
			// // 指定md5加密文件

			// 读取js文件
			FileReader reader = null;
			try {
				reader = new FileReader(js.getPath());
				engine.eval(reader);
				if (engine instanceof Invocable) {
					Invocable invoke = (Invocable) engine;
					// 调用preprocess方法，并传入两个参数密码和验证码
					pass = invoke.invokeFunction("QXWEncodePwd", checkNum[2].trim(), password, checkNum[1].trim()).toString();
					System.out.println("c = " + pass);
				}
			} catch (Exception e) {
				throw e;
			} finally {
				if (null != reader) {
					reader.close();
				}
			}

			/************************* 登录 ****************************/
			get = new HttpGet(
					"http://ptlogin2.qq.com/login?ptlang=2052&u="
							+ username
							+ "&p="
							+ pass
							+ "&verifycode="
							+ checkNum[1]
							+ "&aid=46000101&u1=http%3A%2F%2Ft.qq.com&ptredirect=1&h=1&from_ui=1&dumy=&fp=loginerroralert&action=4-12-14683&g=1&t=1&dummy=");
			get.setHeader("Connection", "keep-alive");
			get.setHeader("Host", "ptlogin2.qq.com");
			get.setHeader("Referer", "http://t.qq.com/?from=11");
			get.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 5.1; rv:13.0) Gecko/20100101 Firefox/13.0");
			response = client.execute(get);
			entity = EntityUtils.toString(response.getEntity());
			System.out.println(response.getStatusLine());
			System.out.println(entity);
			return this.isLogin(client, page, loginValidatePath);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
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

	public static void main(String[] args) {
		URL js = ClassLoader.getSystemResource("js/qqmd52.js");
		System.out.println(js.getPath());
	}
}
