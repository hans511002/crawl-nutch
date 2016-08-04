package org.apache.nutch.protocol.httpclient.bean;

import java.util.List;

public class ProxyServerBean
{
	private String address;
	private List<ProxyUrlBean> lstUrlBean;
	public String getAddress()
	{
		return address;
	}
	
	public void setAddress(String address)
	{
		this.address = address;
	}
	public List<ProxyUrlBean> getLstUrlBean()
	{
		return lstUrlBean;
	}
	public void setLstUrlBean(List<ProxyUrlBean> lstUrlBean)
	{
		this.lstUrlBean = lstUrlBean;
	}
	
	public ProxyUrlBean getProxyUrlBean(String url)
	{
		if (null == this.lstUrlBean)
		{
			return null;
		}
		
		for (ProxyUrlBean proxy : this.lstUrlBean)
		{
			if (proxy.getUrl().equals(url))
			{
				return proxy;
			}
		}
		
		return null;
	}
}
