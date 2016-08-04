package org.apache.nutch.protocol.httpclient.bean;

public class ProxyUrlBean
{
	private String url;
	private boolean isblankcontent;
	private boolean isforce;
	public String getUrl()
	{
		return url;
	}
	public void setUrl(String url)
	{
		this.url = url;
	}
	public boolean isIsblankcontent()
	{
		return isblankcontent;
	}
	public void setIsblankcontent(boolean isblankcontent)
	{
		this.isblankcontent = isblankcontent;
	}
	public boolean isIsforce()
	{
		return isforce;
	}
	public void setIsforce(boolean isforce)
	{
		this.isforce = isforce;
	}
}
