package org.apache.nutch.protocol.httpclient.commponent.site;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.nutch.protocol.http.api.HttpBase;

public class Test
{
	protected int maxContent = 64 * 1024;
	private static PoolingClientConnectionManager poolingClientConnectionManager = new PoolingClientConnectionManager();
	private static DefaultHttpClient httpClient = new DefaultHttpClient(
			poolingClientConnectionManager);
	private static BufferedWriter writer;
	public static void main(String[] args)
	{
		OutputStreamWriter out;
		try
		{
			out = new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(new File("d://log.log"))));
			writer = new BufferedWriter(out, 1024*5);
		}
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
		
//		Thread t1 = getThread("http://weibo.com/u/3001894730");
//		Thread t2 = getThread("http://weibo.com/jolinlove520");
//		Thread t3 = getThread("http://weibo.com/jingruqingge");
//		Thread t4 = getThread("http://e.weibo.com/sinaqing");
//		Thread t5 = getThread("http://e.weibo.com/photo");
//		Thread t6 = getThread("http://weibo.com/hangeng");
//		Thread t7 = getThread("http://weibo.com/oomoo");
//		Thread t8 = getThread("http://e.weibo.com/paike");
//		Thread t9 = getThread("http://weibo.com/n/%E8%94%A1%E4%BE%9D%E6%9E%97");
//		Thread t10 = getThread("http://huati.weibo.com/k/正能量?from=501&order=time");
//		Thread t11 = getThread("http://weibo.com/kennykwanjr");  
		
//		Thread t12 = getThread("http://vwbeetle.cn/gene/mobile/images/b_13.png");
		
//		Thread t1 = getThread("http://home.hi.mop.com/Home.do?ss=10101");
		Thread t2 = getThread("http://blog.hi.mop.com/GetBlog.do?id=13616771");
//		Thread t3 = getThread("http://weibo.com/jingruqingge");
//		Thread t4 = getThread("http://e.weibo.com/sinaqing");
//		Thread t5 = getThread("http://e.weibo.com/photo");
//		Thread t6 = getThread("http://weibo.com/hangeng");
//		Thread t7 = getThread("http://weibo.com/oomoo");
//		Thread t8 = getThread("http://e.weibo.com/paike");
//		Thread t9 = getThread("http://weibo.com/n/%E8%94%A1%E4%BE%9D%E6%9E%97");
//		Thread t10 = getThread("http://huati.weibo.com/k/正能量?from=501&order=time");
//		Thread t11 = getThread("http://weibo.com/kennykwanjr");  
//
//		t1.start(); 
		t2.start(); 
//		t3.start(); 
//		t4.start(); 
//		t5.start(); 
//		t6.start(); 
//		t7.start(); 
//		t8.start(); 
//		t9.start(); 
//		t10.start();
//		t11.start();
//		t12.start();
		
//		List<Thread> lst = new ArrayList<Thread>();
//		for (int i = 0; i < 50; i++)
//		{
//			Thread t = getThread("http://weibo.com/u/3001894730");
//			lst.add(t);
//		}
//		for (Thread t : lst)
//		{
//			t.start();
//		}
	}
	
	public static Thread getThread(final String url)
	{
		return new Thread()
		{
			@Override
			public void run()
			{
				execute(url);
			}
		};
	}

	private static void execute(String url)
	{
		// 抓取数据
		HttpPost httpPost = null;
		try
		{
			httpPost = new HttpPost("http://localhost:8080/CrawlProxyApp/servlet/data");
			List<NameValuePair> nvps = new ArrayList<NameValuePair>();
			nvps.add(new BasicNameValuePair("username", "123456"));
			nvps.add(new BasicNameValuePair("password", "123456"));
			nvps.add(new BasicNameValuePair("url", url));
			httpPost.setEntity(new UrlEncodedFormEntity(nvps, HTTP.UTF_8));
			HttpResponse response = httpClient.execute(httpPost);
			Header[] heads = response.getAllHeaders();

			InputStream in = response.getEntity().getContent();
			try
			{
				byte[] buffer = new byte[HttpBase.BUFFER_SIZE];
				int bufferFilled = 0;
				int totalRead = 0;
				ByteArrayOutputStream out = new ByteArrayOutputStream();
				while ((bufferFilled = in.read(buffer, 0, buffer.length)) != -1
						&& totalRead + bufferFilled <= Integer.MAX_VALUE)
				{
					totalRead += bufferFilled;
					out.write(buffer, 0, bufferFilled);
				}
				String content = new String(out.toByteArray());
				System.out.println(content);
				writer.write(content);
				writer.flush();
			}
			catch (Exception e)
			{
			}
			finally
			{
				if (in != null)
				{
					in.close();
				}
				httpPost.abort();
			}
		}
		catch (Exception e)
		{
		}
		finally
		{
			// 关闭连接
			httpPost.releaseConnection();
		}
	}
}
