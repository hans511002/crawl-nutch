package org.apache.nutch.protocol.httpclient.bean;

import java.util.Map;

public class CredentialsBean
{
	public static int LOGINMAXTIME = 5;
	private int loginCurrentTime = 0;
	private String username;
	private String password;
	private String handle;
	private int loginmaxnumber;
	private int logintimes;
	private boolean isLogin;
	private String loginaction;
	private Map<String, String> paraMap;

	public CredentialsBean()
	{
		
	}

	public CredentialsBean(String username, String password, String handle,int loginmaxnumber,
			String loginaction, Map<String, String> paraMap)
	{
		this.username = username;
		this.password = password;
		this.loginmaxnumber = loginmaxnumber;
		this.handle = handle;
		this.loginaction = loginaction;
		this.paraMap = paraMap;
	}

	public String getUsername()
	{
		return username;
	}

	public void setUsername(String username)
	{
		this.username = username;
	}

	public String getPassword()
	{
		return password;
	}

	public void setPassword(String password)
	{
		this.password = password;
	}

	public String getHandle()
	{
		return handle;
	}

	public void setHandle(String handle)
	{
		this.handle = handle;
	}

	public int getLoginmaxnumber()
	{
		return loginmaxnumber;
	}

	public void setLoginmaxnumber(int loginmaxnumber)
	{
		this.loginmaxnumber = loginmaxnumber;
	}

	public int getLogintimes()
	{
		return logintimes;
	}

	public void setLogintimes(int logintimes)
	{
		this.logintimes = logintimes;
	}

	public boolean isLogin()
	{
		return isLogin;
	}

	public void setLogin(boolean isLogin)
	{
		this.isLogin = isLogin;
	}

	public String getLoginaction()
	{
		return loginaction;
	}

	public void setLoginaction(String loginaction)
	{
		this.loginaction = loginaction;
	}

	public int getLoginCurrentTime()
	{
		return loginCurrentTime;
	}

	public void setLoginCurrentTime(int loginCurrentTime)
	{
		this.loginCurrentTime = loginCurrentTime;
	}

	public Map<String, String> getParaMap()
	{
		return paraMap;
	}
}
