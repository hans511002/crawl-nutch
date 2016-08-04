package org.apache.nutch.protocol.httpclient.commponent;

import java.util.HashMap;
import java.util.Map;

public class HttpComponentManager
{
	private static final Map<String, HttpComponent> HTTP_COMPONENT_MAP = new HashMap<String, HttpComponent>();
	
	public static synchronized void register(String host, HttpComponent httpComponent)
	{
		HTTP_COMPONENT_MAP.put(host, httpComponent);
	}
	
	public static synchronized HttpComponent getHttpComponent(String host)
	{
		return HTTP_COMPONENT_MAP.get(host);
	}
}
