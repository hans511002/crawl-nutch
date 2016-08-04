package org.apache.nutch.protocol.httpclient.commponent.site;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.protocol.httpclient.bean.CredentialsBean;
import org.apache.nutch.protocol.httpclient.commponent.HttpComponent;
import org.apache.nutch.protocol.httpclient.commponent.HttpResponseComponent;

public class HttpResponseComponentSina extends HttpResponseComponent
{
	public HttpResponseComponentSina(HttpComponent http, CredentialsBean cb, URL url,
			CrawlDatum datum, boolean followRedirects, String host)
	{
		super(http, cb, url, datum, followRedirects, host);
	}

	@SuppressWarnings("deprecation")
	@Override
	public boolean login(HttpClient client, CredentialsBean credentialsBean)
	throws ClientProtocolException, IOException
	{
		this.setLoginTimes(credentialsBean);
		HttpPost post = new HttpPost(
				"http://login.sina.com.cn/sso/login.php?client=ssologin.js");

		String data = getServerTime();
		String nonce = makeNonce(6);
		List<NameValuePair> nvps = new ArrayList<NameValuePair>();
		nvps.add(new BasicNameValuePair("entry", "weibo"));
		nvps.add(new BasicNameValuePair("gateway", "1"));
		nvps.add(new BasicNameValuePair("from", ""));
		nvps.add(new BasicNameValuePair("savestate", "7"));
		nvps.add(new BasicNameValuePair("useticket", "1"));
		nvps.add(new BasicNameValuePair("ssosimplelogin", "1"));
		nvps.add(new BasicNameValuePair("su", encodeAccount(credentialsBean.getUsername())));
		nvps.add(new BasicNameValuePair("service", "miniblog"));
		nvps.add(new BasicNameValuePair("servertime", data));
		nvps.add(new BasicNameValuePair("nonce", nonce));
		nvps.add(new BasicNameValuePair("pwencode", "wsse"));
		nvps.add(new BasicNameValuePair("sp", new SinaSSOEncoder().encode(
				credentialsBean.getPassword(), data, nonce)));
		nvps.add(new BasicNameValuePair(
				"url",
				"http://weibo.com/ajaxlogin.php?framelogin=1&callback=parent.sinaSSOController.feedBackUrlCallBack"));
		nvps.add(new BasicNameValuePair("returntype", "META"));
		nvps.add(new BasicNameValuePair("encoding", "UTF-8"));
		nvps.add(new BasicNameValuePair("vsnval", ""));

		post.setEntity(new UrlEncodedFormEntity(nvps, HTTP.UTF_8));
		HttpResponse response = client.execute(post);
		String entity = EntityUtils.toString(response.getEntity());
		String urls = entity.substring(
				entity.indexOf("http://weibo.com/ajaxlogin.php?"),
				entity.indexOf("code=0") + 6);

		//login real url
		HttpGet getMethod = new HttpGet(urls);
		response = client.execute(getMethod);
		entity = EntityUtils.toString(response.getEntity());
		entity = entity.substring(entity.indexOf("userdomain") + 13,
				entity.lastIndexOf("\""));
		
		int code = response.getStatusLine().getStatusCode();
		return HttpStatus.SC_OK == code ? true : false;
	}
	
	@Override
	public boolean isLogin(HttpClient client, CredentialsBean credentialsBean)
	{
		HttpPost getMethod = new HttpPost("http://account.weibo.com/");
		try
		{
			HttpResponse response = client.execute(getMethod);
			if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode())
			{
				return true;
			}
		}
		catch (ClientProtocolException e)
		{
			e.printStackTrace();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		return false;
	}

	private static String encodeAccount(String account)
	{
		String userName = "";
		try
		{
			userName = Base64.encodeBase64String(URLEncoder.encode(account,
					"UTF-8").getBytes());
		}
		catch (UnsupportedEncodingException e)
		{
			e.printStackTrace();
		}
		return userName;
	}

	private static String makeNonce(int len)
	{
		String x = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
		String str = "";
		for (int i = 0; i < len; i++)
		{
			str += x.charAt((int) (Math.ceil(Math.random() * 1000000) % x
					.length()));
		}
		return str;
	}

	private static String getServerTime()
	{
		long servertime = new Date().getTime() / 1000;
		return String.valueOf(servertime);
	}

	class SinaSSOEncoder
	{
		private boolean i = false;
		private int g = 8;

		public SinaSSOEncoder()
		{

		}

		public String encode(String psw, String servertime, String nonce)
		{
			String password;
			password = hex_sha1("" + hex_sha1(hex_sha1(psw)) + servertime
					+ nonce);
			return password;
		}

		private String hex_sha1(String j)
		{
			return h(b(f(j, j.length() * g), j.length() * g));
		}

		private String h(int[] l)
		{
			String k = i ? "0123456789ABCDEF" : "0123456789abcdef";
			String m = "";
			for (int j = 0; j < l.length * 4; j++)
			{
				m += k.charAt((l[j >> 2] >> ((3 - j % 4) * 8 + 4)) & 15) + ""
						+ k.charAt((l[j >> 2] >> ((3 - j % 4) * 8)) & 15);
			}
			return m;
		}

		private int[] b(int[] A, int r)
		{
			A[r >> 5] |= 128 << (24 - r % 32);
			A[((r + 64 >> 9) << 4) + 15] = r;
			int[] B = new int[80];
			int z = 1732584193;
			int y = -271733879;
			int v = -1732584194;
			int u = 271733878;
			int s = -1009589776;
			for (int o = 0; o < A.length; o += 16)
			{
				int q = z;
				int p = y;
				int n = v;
				int m = u;
				int k = s;
				for (int l = 0; l < 80; l++)
				{
					if (l < 16)
					{
						B[l] = A[o + l];
					}
					else
					{
						B[l] = d(B[l - 3] ^ B[l - 8] ^ B[l - 14] ^ B[l - 16], 1);
					}
					int C = e(e(d(z, 5), a(l, y, v, u)), e(e(s, B[l]), c(l)));
					s = u;
					u = v;
					v = d(y, 30);
					y = z;
					z = C;
				}
				z = e(z, q);
				y = e(y, p);
				v = e(v, n);
				u = e(u, m);
				s = e(s, k);
			}
			return new int[] { z, y, v, u, s };
		}

		private int a(int k, int j, int m, int l)
		{
			if (k < 20)
			{
				return (j & m) | ((~j) & l);
			}
			;
			if (k < 40)
			{
				return j ^ m ^ l;
			}
			;
			if (k < 60)
			{
				return (j & m) | (j & l) | (m & l);
			}
			;
			return j ^ m ^ l;
		}

		private int c(int j)
		{
			return (j < 20) ? 1518500249 : (j < 40) ? 1859775393
					: (j < 60) ? -1894007588 : -899497514;
		}

		private int e(int j, int m)
		{
			int l = (j & 65535) + (m & 65535);
			int k = (j >> 16) + (m >> 16) + (l >> 16);
			return (k << 16) | (l & 65535);
		}

		private int d(int j, int k)
		{
			return (j << k) | (j >>> (32 - k));
		}

		private int[] f(String m, int r)
		{
			int[] l;
			int j = (1 << this.g) - 1;
			int len = ((r + 64 >> 9) << 4) + 15;
			int k;
			for (k = 0; k < m.length() * g; k += g)
			{
				len = k >> 5 > len ? k >> 5 : len;
			}
			l = new int[len + 1];
			for (k = 0; k < l.length; k++)
			{
				l[k] = 0;
			}
			for (k = 0; k < m.length() * g; k += g)
			{
				l[k >> 5] |= (m.charAt(k / g) & j) << (24 - k % 32);
			}
			return l;
		}
	}
}
