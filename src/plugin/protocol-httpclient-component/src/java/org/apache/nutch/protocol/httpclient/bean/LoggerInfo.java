package org.apache.nutch.protocol.httpclient.bean;

public class LoggerInfo {
    private static final String logger_prefix = "[*HttpComponent*] ";
    private String info;

    public LoggerInfo(String info) {
	this.info = logger_prefix + info;
    }

    public String getInfo() {
	return info;
    }
}
