package org.apache.nutch.protocol.httpclient.site;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.SpellCheckedMetadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.http.api.HttpBase;
import org.apache.nutch.util.DeflateUtils;
import org.apache.nutch.util.GZIPUtils;

public class HttpClientUtils {

	/**
	 * 向HTTPS地址发送POST请求
	 * 
	 * @param httpClient
	 * @param httpPost
	 * @return 响应内容
	 * @throws Exception
	 */
	public static HttpResponse sendSSLPostRequest(HttpClient httpClient, HttpRequestBase httpBase) throws Exception {
		X509TrustManager xtm = new X509TrustManager() { // 创建TrustManager
			public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
			}

			public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
			}

			public X509Certificate[] getAcceptedIssuers() {
				return null;
			}
		};

		// TLS1.0与SSL3.0基本上没有太大的差别，可粗略理解为TLS是SSL的继承者，但它们使用的是相同的SSLContext
		SSLContext ctx = SSLContext.getInstance("TLS");

		// 使用TrustManager来初始化该上下文，TrustManager只是被SSL的Socket所使用
		ctx.init(null, new TrustManager[] { xtm }, null);

		// 创建SSLSocketFactory
		SSLSocketFactory socketFactory = new SSLSocketFactory(ctx);

		// 通过SchemeRegistry将SSLSocketFactory注册到我们的HttpClient上
		httpClient.getConnectionManager().getSchemeRegistry().register(new Scheme("https", 443, socketFactory));
		HttpResponse response = httpClient.execute(httpBase); // 执行POST请求
		// HttpEntity entity = response.getEntity(); // 获取响应实体
		//
		// if (null != entity) {
		// responseLength = entity.getContentLength();
		// responseContent = EntityUtils.toString(entity, "UTF-8");
		// EntityUtils.consume(entity); // Consume response content
		// }
		// System.out.println("请求地址: " + httpBase.getURI());
		// System.out.println("响应状态: " + response.getStatusLine());
		// System.out.println("响应长度: " + responseLength);
		// System.out.println("响应内容: " + responseContent);
		return response;
	}

	public static byte[] getResponseContent(HttpResponse response) throws IOException {
		byte[] content = null;
		InputStream in = response.getEntity().getContent();
		try {
			byte[] buffer = new byte[HttpBase.BUFFER_SIZE];
			int bufferFilled = 0;
			int totalRead = 0;
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			while ((bufferFilled = in.read(buffer, 0, buffer.length)) > 0 && totalRead + bufferFilled <= Long.MAX_VALUE) {
				totalRead += bufferFilled;
				out.write(buffer, 0, bufferFilled);
			}
			content = out.toByteArray();
		} catch (Exception e) {
			e.printStackTrace(System.out);
		} finally {
			if (in != null) {
				in.close();
			}
		}
		return content;
	}

	public static byte[] extractContent(HttpResponse response, byte[] content) throws IOException {
		Header[] heads = response.getAllHeaders();
		Metadata headers = new SpellCheckedMetadata();
		for (int i = 0; i < heads.length; i++) {
			headers.set(heads[i].getName(), heads[i].getValue());
		}

		String contentType = headers.get(Response.CONTENT_TYPE);
		String charSet = HttpBase.parseCharacterEncoding(contentType);

		// Extract gzip, x-gzip and deflate content
		if (content != null) {
			// check if we have to uncompress it
			String contentEncoding = headers.get(Response.CONTENT_ENCODING);
			if ("gzip".equals(contentEncoding) || "x-gzip".equals(contentEncoding)) {
				content = processGzipEncoded(content, 6 * 1024);
			} else if ("deflate".equals(contentEncoding)) {
				content = processDeflateEncoded(content, 6 * 1024);
			}
		}

		if (charSet != null && !charSet.equals("utf-8")) {
			if (contentType != null)
				headers.set(Response.CONTENT_TYPE, contentType.replace(charSet, "utf-8"));
			if (null == content) {
				return new byte[0];
			}
			String txt = new String(content, charSet);
			content = txt.getBytes("utf-8");
		}

		return content;
	}

	public static byte[] processGzipEncoded(byte[] compressed, int maxContent) throws IOException {
		byte[] content;
		if (maxContent >= 0) {
			content = GZIPUtils.unzipBestEffort(compressed, maxContent);
		} else {
			content = GZIPUtils.unzipBestEffort(compressed);
		}

		if (content == null)
			throw new IOException("unzipBestEffort returned null");

		return content;
	}

	public static byte[] processDeflateEncoded(byte[] compressed, int maxContent) throws IOException {
		byte[] content = DeflateUtils.inflateBestEffort(compressed, maxContent);
		if (content == null)
			throw new IOException("inflateBestEffort returned null");

		return content;
	}
}
