package org.apache.nutch.protocol.httpclient.commponent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.SpellCheckedMetadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.http.api.HttpBase;
import org.apache.nutch.protocol.httpclient.bean.CredentialsBean;
import org.apache.nutch.protocol.httpclient.bean.ProxyUrlBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HttpResponseComponent implements Response
{
	public static final Logger LOG = LoggerFactory
	       .getLogger("org.apache.hadoop.protocl.httpclient.component");
	private URL url;
	private byte[] content;
	private int code;
	private Metadata headers = new SpellCheckedMetadata();
	private HttpClient httpClient;
	
	public HttpResponseComponent(HttpComponent httpComponent,CredentialsBean cb, URL url, CrawlDatum datum,
			boolean followRedirects, String host)
	{
		// Prepare GET method for HTTP request
		this.url = url;
		this.httpClient= httpComponent.getClient();
		
		if (null != cb && !cb.isLogin())
		{
			// 登录
			httpComponent.login(this, cb, url.getHost());
		}

		// 获取代理信息
		ProxyUrlBean  proxyUrlBean = null;
		String proxyAddress = null;
		if (null != HttpComponentInit.proxyServerBean)
		{
			proxyUrlBean = HttpComponentInit.getProxyUrlBean(host);
			proxyAddress = HttpComponentInit.getProxyAddress();
		}
		
		// 代理获取
		boolean isPassProxy = isPassProxy(httpComponent, url.toString(), proxyAddress, proxyUrlBean);
		if (isPassProxy)
		{
			httpClientProxy(httpComponent, url, proxyAddress);
			
			// 强制代理
			if (null != proxyUrlBean && proxyUrlBean.isIsforce())
			{
				return;
			}
		}
		
		// httpClient获取
		// 情况一：不通过代理获取
		
		if (!isPassProxy)
		{
			httpClient(httpComponent, url);
			return;
		}
		// 情况二：// 通过代理，但是结果为空，满足配置要求
		if (isPassProxy && null != proxyUrlBean && proxyUrlBean.isIsblankcontent()
				&& (null == this.content || this.content.length <=0))
		{
			httpClient(httpComponent, url);
			LOG.warn("proxy fail, content is null, so by httpcomponentclient get again!!");
		}
	}

	private void httpClient(HttpComponent httpComponent, URL url)
	{
		HttpGet httpGet = null;
		HttpResponse response = null;
		try
		{
			httpGet = new HttpGet(url.toString());
			response = this.httpClient.execute(httpGet);
			packageResponse(httpComponent, url, httpGet, null, response);
		}
		catch (Exception e)
		{
			System.err.println("Exceptions:" + e.getMessage()+", url:"+url);
		}
		finally
		{
			// 关闭连接
			if (null != httpGet)
			{
				httpGet.releaseConnection();
			}
		}
	}

	@SuppressWarnings("deprecation")
	private boolean httpClientProxy(HttpComponent httpComponent, URL url,
			String proxyAddress)
	{
		HttpResponse response = null;
		HttpPost httpPost = null;
		try
		{
				httpPost = new HttpPost(proxyAddress);
				List<NameValuePair> nvps = new ArrayList<NameValuePair>();
				nvps.add(new BasicNameValuePair("url", url.toString()));
				httpPost.setEntity(new UrlEncodedFormEntity(nvps, HTTP.UTF_8));
				response = httpClient.execute(httpPost);
				packageResponse(httpComponent, url, null, httpPost, response);
		}
		catch (Exception e)
		{
			LOG.error("[*HttpClientProxy*] Exceptions:" + e.getMessage()+", url:"+url);
			return false;
		}
		finally
		{
			// 关闭连接
			if (null != httpPost)
			{
				httpPost.releaseConnection();
			}
		}
		return true;
	}


	private boolean isPassProxy(HttpComponent httpComponent, String host,
			String proxyAddress, ProxyUrlBean proxyUrlBean)
	{
		// 需要判断图片等内容
		String tempSuffix = host.substring(host.lastIndexOf(".")+1,
				host.length());
		
		if (HttpComponentInit.notSupportSuffixMmap.containsKey(tempSuffix))
		{
			return false;
		}
		
		return null != proxyAddress && null != proxyUrlBean;
	}

	private void packageResponse(HttpComponent httpComponent, URL url,
			HttpGet httpGet, HttpPost httpPost, HttpResponse response)
			throws HttpException, IOException
	{
		this.code = response.getStatusLine().getStatusCode();
		Header[] heads = response.getAllHeaders();
		for (int i = 0; i < heads.length; i++)
		{
			this.headers.set(heads[i].getName(), heads[i].getValue());
		}

		// Limit download size
		int contentLength = Integer.MAX_VALUE;
		String contentLengthString = this.headers.get(Response.CONTENT_LENGTH);
		if (contentLengthString != null)
		{
			try
			{
				contentLength = Integer.parseInt(contentLengthString.trim());
			}
			catch (NumberFormatException ex)
			{
				throw new HttpException("bad content length: "
						+ contentLengthString);
			}
		}
		if (httpComponent.getMaxContent() >= 0
				&& contentLength > httpComponent.getMaxContent())
		{
			contentLength = httpComponent.getMaxContent();
		}

		// always read content. Sometimes content is useful to find a cause
		// for error.
		InputStream in = response.getEntity().getContent();
		try
		{
			byte[] buffer = new byte[HttpBase.BUFFER_SIZE];
			int bufferFilled = 0;
			int totalRead = 0;
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			while ((bufferFilled = in.read(buffer, 0, buffer.length)) != -1
					&& totalRead + bufferFilled <= contentLength)
			{
				totalRead += bufferFilled;
				out.write(buffer, 0, bufferFilled);
			}

			this.content = out.toByteArray();
		}
		catch (Exception e)
		{
			if (this.code == 200)
			{
				throw new IOException(e.toString());
			}
			// for codes other than 200 OK, we are fine with empty content
		}
		finally
		{
			if (in != null)
			{
				in.close();
			}
			if (null != httpGet)
			{
				httpGet.abort();
			}
			if (null != httpPost)
			{
				httpPost.abort();
			}
		}

		StringBuilder fetchTrace = null;
		if (HttpComponent.LOG.isTraceEnabled())
		{
			// Trace message
			fetchTrace = new StringBuilder("url: " + url
					+ "; status code: " + this.code + "; bytes received: "
					+ this.content.length);
			if (getHeader(Response.CONTENT_LENGTH) != null)
				fetchTrace.append("; Content-Length: "
						+ getHeader(Response.CONTENT_LENGTH));
			if (getHeader(Response.LOCATION) != null)
				fetchTrace.append("; Location: "
						+ getHeader(Response.LOCATION));
		}
		// Extract gzip, x-gzip and deflate content
		if (this.content != null)
		{
			// check if we have to uncompress it
			String contentEncoding = this.headers.get(Response.CONTENT_ENCODING);
			if (contentEncoding != null && HttpComponent.LOG.isTraceEnabled())
				fetchTrace.append("; Content-Encoding: " + contentEncoding);
			if ("gzip".equals(contentEncoding)
					|| "x-gzip".equals(contentEncoding))
			{
				this.content = httpComponent.processGzipEncoded(this.content, url);
				if (HttpComponent.LOG.isTraceEnabled())
					fetchTrace.append("; extracted to " + this.content.length
							+ " bytes");
			}
			else if ("deflate".equals(contentEncoding))
			{
				this.content = httpComponent.processDeflateEncoded(this.content, url);
				if (HttpComponent.LOG.isTraceEnabled())
					fetchTrace.append("; extracted to " + this.content.length
							+ " bytes");
			}
		}

		// Logger trace message
		if (HttpComponent.LOG.isTraceEnabled())
		{
			HttpComponent.LOG.trace(fetchTrace.toString());
		}
	}
	
	public abstract boolean login(HttpClient client, CredentialsBean credentialsBean)
			throws ClientProtocolException, IOException;
	
	public abstract boolean isLogin(HttpClient client, CredentialsBean credentialsBean);
	
	/**
	 * 设置登录次数
	 * @param credentialsBean
	 * @return
	 */
	public boolean setLoginTimes(CredentialsBean credentialsBean)
	{
		int logintime = credentialsBean.getLogintimes();
		if (credentialsBean.getLogintimes() > credentialsBean.getLoginmaxnumber())
		{
			return false;
		}
		
		credentialsBean.setLogintimes(logintime+1);
		HttpComponentInit.setLoginTimes(credentialsBean);
		return true;
	}

	@Override
	public int getCode()
	{
		return this.code;
	}

	@Override
	public byte[] getContent()
	{
		return this.content;
	}

	@Override
	public String getHeader(String name)
	{
		return this.headers.get(name);
	}

	@Override
	public Metadata getHeaders()
	{
		return this.headers;
	}

	@Override
	public URL getUrl()
	{
		return this.url;
	}
}
