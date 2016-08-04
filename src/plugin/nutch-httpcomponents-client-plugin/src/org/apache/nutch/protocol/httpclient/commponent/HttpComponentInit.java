package org.apache.nutch.protocol.httpclient.commponent;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.protocol.httpclient.bean.CredentialsBean;
import org.apache.nutch.protocol.httpclient.bean.LoggerInfo;
import org.apache.nutch.protocol.httpclient.bean.ProxyServerBean;
import org.apache.nutch.protocol.httpclient.bean.ProxyUrlBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class HttpComponentInit
{
	public static final Logger LOG = LoggerFactory
			.getLogger("org.apache.hadoop.protocl.httpclient.component");
	public static final String NOT_SUPPORT_FILE_SUFFIX = "swf|SWF|doc|DOC|mp3|MP3|WMV|wmv|txt|TXT|rtf|RTF|avi|AVI|m3u|M3U|flv|FLV|WAV|wav|mp4|MP4|avi|AVI|rss|RSS|xml|XML|pdf|PDF|js|JS|gif|GIF|jpg|JPG|png|PNG|ico|ICO|css|sit|eps|wmf|zip|ppt|mpg|xls|gz|rpm|tgz|mov|MOV|exe|jpeg|JPEG|bmp|BMP";
	public static final Map<String , String> notSupportSuffixMmap = new HashMap<String, String>();
	public static Map<String, CredentialsBean> credentialsMap;
	public static ProxyServerBean proxyServerBean;
	public static boolean isHttpComponentClient = false;
	public static int HTTPCONNECTIONTIMEOUT = 10000;
	public static int HTTPSOCKETTIMEOUT = 20000;
	public static final String DEFAULT = "default";

	public static void init(String authFile, Configuration conf)
	{
		try
		{
			initAuthFile(authFile, conf);
			// 初始化不支持的文件后缀
			String[] suffix = NOT_SUPPORT_FILE_SUFFIX.split("\\|");
			for (String s : suffix)
			{
				notSupportSuffixMmap.put(s.toLowerCase(), null);
			}
			LOG.info(new LoggerInfo("HttpComponentInit'authFile init success!")
					.getInfo());
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static synchronized HttpComponent initHttpComponentClient(
			String host, Configuration conf)
	{
		HttpComponent httpComponent = HttpComponentManager
				.getHttpComponent(host);
		if (null != httpComponent)
		{
			return httpComponent;
		}

		httpComponent = new HttpComponent();
		httpComponent.setConf(conf);
		if (HttpComponentInit.DEFAULT.equals(host))
		{
			HttpComponentManager.register(host, httpComponent);
			LOG.info(new LoggerInfo("HttpComponentManager.register:" + host
					+ " HttpComponent:" + httpComponent + " ,direc.t")
					.getInfo());
			return httpComponent;
		}

		for (String temp : getSameCredentials(host))
		{
			HttpComponentManager.register(temp, httpComponent);
			LOG.info(new LoggerInfo("HttpComponentManager.register:" + temp
					+ " HttpComponent:" + httpComponent + " ,indirect.")
					.getInfo());
		}

		return httpComponent;
	}
	
	/**
	 * 读取权限配置信息
	 */
	private static synchronized void initAuthFile(String authFile,
			Configuration conf) throws ParserConfigurationException,
			SAXException, IOException
	{
		if (null != credentialsMap)
		{
			return;
		}

		credentialsMap = new HashMap<String, CredentialsBean>();
		InputStream is = conf.getConfResourceAsInputStream(authFile);
		if (is != null)
		{
			Document doc = DocumentBuilderFactory.newInstance()
					.newDocumentBuilder().parse(is);
			Element rootElement = doc.getDocumentElement();
			if (!"auth-configuration".equals(rootElement.getTagName()))
			{
				if (LOG.isWarnEnabled())
					LOG.warn(new LoggerInfo("Bad auth conf file: root element <"
							+ rootElement.getTagName() + "> found in "
							+ authFile + " - must be <auth-configuration>").getInfo());
			}

			// For each set of credentials
			NodeList credList = rootElement.getChildNodes();
			for (int i = 0; i < credList.getLength(); i++)
			{
				Node credNode = credList.item(i);
				if (!(credNode instanceof Element))
				{
					continue;
				}

				Element credElement = (Element) credNode;
				
				if ("isHttpComponentClient".equals(credElement.getNodeName()))
				{
					isHttpComponentClient = Boolean.valueOf(credElement.getTextContent());
					try
					{
						HTTPCONNECTIONTIMEOUT = Integer.parseInt(credElement.getAttribute("httpconnectiontimeout"));
					}
					catch (Exception e)
					{
					}
					
					try
					{
						HTTPSOCKETTIMEOUT = Integer.parseInt(credElement.getAttribute("httpsockettimeout"));
					}catch (Exception e)
					{
					}
					continue;
				}
				
				if ("credentials".equals(credElement.getNodeName()))
				{
					loadCredentials(credElement);
					continue;
				}
				
				if ("proxyserver".equals(credElement.getNodeName()))
				{
					loadProxyserver(credElement);
				}
			}
		}
		if (null != is)
		{
			is.close();
		}
	}

	private static void loadProxyserver(Element credElement)
	{
		String proxyAddress = credElement.getAttribute("address");
		NodeList child = credElement.getChildNodes();
		Element cElement = null;
		for (int j = 0; j < child.getLength(); j++)
		{
			Node lstNode = child.item(j);
			if (lstNode instanceof Element)
			{
				cElement = (Element) lstNode;
				break;
			}
		}
		
		if (null == cElement)
		{
			return;
		}
		
		List<ProxyUrlBean> lstString = new ArrayList<ProxyUrlBean>();
		NodeList urlNodeLst = cElement.getChildNodes();
		for (int j = 0; j < urlNodeLst.getLength(); j++)
		{
			Node urlNode = urlNodeLst.item(j);
			if (urlNode instanceof Element)
			{
				Element urlElement = (Element) urlNode;
				ProxyUrlBean urlBean= new ProxyUrlBean(); 
				urlBean.setIsblankcontent(Boolean.valueOf(urlElement.getAttribute("isblankcontent")));
				urlBean.setIsforce(Boolean.valueOf(urlElement.getAttribute("isforce")));
				urlBean.setUrl(urlElement.getTextContent());
				lstString.add(urlBean);
			}
		}
		
		if (null == proxyAddress || proxyAddress.length() <= 0)
		{
			return;
		}
		
		proxyServerBean = new ProxyServerBean();
		proxyServerBean.setAddress(proxyAddress);
		proxyServerBean.setLstUrlBean(lstString);
	}

	private static void loadCredentials(Element credElement)
	{
		String username = credElement.getAttribute("username");
		String password = credElement.getAttribute("password");
		String handle = credElement.getAttribute("handle");
		int loginmaxnumber = 0;
		try
		{
			loginmaxnumber = Integer.parseInt(credElement.getAttribute("loginmaxnumber"));
		}catch (Exception e)
		{
		}

		// For each credentials
		NodeList scopeList = credElement.getChildNodes();
		String host = "";
		String loginaction = "";
		Map<String, String> paraMap = new HashMap<String, String>();
		for (int j = 0; j < scopeList.getLength(); j++)
		{
			Node scopeNode = scopeList.item(j);
			if (!(scopeNode instanceof Element))
			{
				continue;
			}

			Element cElement = (Element) scopeNode;
			if ("authscope".equals(cElement.getTagName()))
			{
				host = cElement.getAttribute("host");
				NodeList asNodeChild = cElement.getChildNodes();
				for (int i = 0; i < asNodeChild.getLength(); i++)
				{
					Node asNode = asNodeChild.item(i);
					if (!(asNode instanceof Element))
					{
						continue;
					}
					Element aslement = (Element) asNode;
					if ("loginaction".equals(aslement.getTagName()))
					{
						loginaction = aslement.getTextContent();
						continue;
					}
					
					if ("listparam".equals(aslement.getTagName()))
					{
						NodeList node = aslement.getChildNodes();
						for (int h = 0; h < node.getLength(); h++)
						{
							Node nd = node.item(h);
							if (!(nd instanceof Element))
							{
								continue;
							}
							Element ndElement = (Element) nd;
							paraMap.put(ndElement.getAttribute("name"), ndElement.getTextContent());
						}
					}
				}
				credentialsMap.put(host, new CredentialsBean(username,
						password, handle, loginmaxnumber, loginaction,
						paraMap));
			}
		}
	}

	/**
	 * set login in
	 * @param cb2
	 */
	public synchronized static void setLogin(CredentialsBean cb2)
	{
		if (null == cb2)
		{
			return;
		}

		Iterator<String> iterator = credentialsMap.keySet().iterator();
		while (iterator.hasNext())
		{
			String key = iterator.next();
			CredentialsBean cb = credentialsMap.get(key);
			if (cb != null && cb2.getUsername().equals(cb.getUsername())
					&& cb2.getPassword().equals(cb.getPassword())
					&& cb2.getHandle().equals(cb.getHandle()))
			{
				cb.setLogin(cb2.isLogin());
			}
		}
	}
	
	/**
	 * set login times
	 * @param cb2
	 */
	public synchronized static void setLoginTimes(CredentialsBean cb2)
	{
		if (null == cb2)
		{
			return;
		}

		Iterator<String> iterator = credentialsMap.keySet().iterator();
		while (iterator.hasNext())
		{
			String key = iterator.next();
			CredentialsBean cb = credentialsMap.get(key);
			if (cb != null && cb2.getUsername().equals(cb.getUsername())
					&& cb2.getPassword().equals(cb.getPassword())
					&& cb2.getHandle().equals(cb.getHandle()))
			{
				cb.setLogintimes(cb2.getLogintimes());
			}
		}
	}

	/**
	 * @param host
	 * @return
	 */
	private synchronized static List<String> getSameCredentials(String host)
	{
		List<String> lst = new ArrayList<String>();
		CredentialsBean cb2 = credentialsMap.get(host);
		if (null == cb2)
		{
			return lst;
		}
		Iterator<String> iterator = credentialsMap.keySet().iterator();
		while (iterator.hasNext())
		{
			String key = iterator.next();
			CredentialsBean cb = credentialsMap.get(key);
			if (cb != null && cb2.getUsername().equals(cb.getUsername())
					&& cb2.getPassword().equals(cb.getPassword())
					&& cb2.getHandle().equals(cb.getHandle()))
			{
				lst.add(key);
			}
		}

		return lst;
	}
	
	public static synchronized ProxyUrlBean getProxyUrlBean(String host)
	{
		return proxyServerBean.getProxyUrlBean(host);
	}
	
	public static synchronized String getProxyAddress()
	{
		return proxyServerBean.getAddress();
	}
}
