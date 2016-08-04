package org.apache.nutch.util;

public class TestLogUtil {
	public static void log(String msg) {
		System.out.println("------STEVE-----, msg:" + msg);
	}

	public static void logPrefixBlank(String msg) {
		System.out.println("                  msg:" + msg);
	}

	public static void log(Object msg) {
		System.out.println("------STEVE-----, msg:" + msg);
	}
}
