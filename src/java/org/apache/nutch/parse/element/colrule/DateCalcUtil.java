package org.apache.nutch.parse.element.colrule;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.nutch.storage.WebPage;

public class DateCalcUtil {
	static java.text.SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static String dateCalc(String val, String dateType, WebPage page) {
		if (val == null){
			return "";
		}
		try {
			if (dateType.equals("1")) {
				val = val.trim();
				Date cur = new Date(page.getFetchTime());
				if (val.indexOf("分钟前") > 0) {
					Calendar now = Calendar.getInstance();  
				    now.setTime(cur);  
				    int s = (int)Integer.parseInt(val.substring(0, val.indexOf("分钟前")));
				    now.set(Calendar.MINUTE, now.get(Calendar.MINUTE) - s);
				    val = df.format(now.getTime());
				} else if (val.indexOf("小时前") > 0) {
					Calendar now = Calendar.getInstance();  
				    now.setTime(cur);  
				    int s = (int)Integer.parseInt(val.substring(0, val.indexOf("小时前")));
				    now.set(Calendar.HOUR, now.get(Calendar.HOUR) - s);
				    val = df.format(now.getTime());
				} else if (val.indexOf("天前") > 0) {
					Calendar now = Calendar.getInstance();  
				    now.setTime(cur); 
				    int s = (int)Integer.parseInt(val.substring(0, val.indexOf("天前")));
				    now.set(Calendar.DATE, now.get(Calendar.DATE) - s);
				    val = df.format(now.getTime());
				} else if (val.indexOf("周前") > 0) {
					Calendar now = Calendar.getInstance();  
				    now.setTime(cur);  
				    int s = (int)Integer.parseInt(val.substring(0, val.indexOf("周前")));
				    now.set(Calendar.WEEK_OF_YEAR, now.get(Calendar.WEEK_OF_YEAR) - s);
				    val = df.format(now.getTime());
				} else if (val.indexOf("个月前") > 0) {
					Calendar now = Calendar.getInstance();  
				    now.setTime(cur);  
				    int s = (int)Integer.parseInt(val.substring(0, val.indexOf("个月前")));
				    now.set(Calendar.MONTH, now.get(Calendar.MONTH) - s);
				    val = df.format(now.getTime());
				} else if (val.indexOf("年前") > 0) {
					Calendar now = Calendar.getInstance();  
				    now.setTime(cur);  
				    int s = (int)Integer.parseInt(val.substring(0, val.indexOf("年前")));
				    now.set(Calendar.YEAR, now.get(Calendar.YEAR) - s);
				    val = df.format(now.getTime());
				} else if (val.matches("[0-2]{1}[0-9]{1}:\\d{2}\\s*/\\s*[0-3]{1}[0-9]{1}")){//  09:09/18
					String tmp[] = val.split("/");
					SimpleDateFormat dfs = new SimpleDateFormat("yyyy/MM/dd HH:mm");
					Calendar now = Calendar.getInstance();  
					now.setTime(cur);  
					Date val1 = dfs.parse(now.get(Calendar.YEAR)+"/"+now.get(Calendar.MONTH)+"/"+tmp[1].trim()+" "+tmp[0].trim());
					val = df.format(val1);
				} else if (val.matches("\\d{2}/\\d{2}\\s+[0-2]{1}[0-9]{1}:\\d{2}")){// 06/09 12:00
					String tmp[] = val.split("\\s+");
					SimpleDateFormat dfs = new SimpleDateFormat("yyyy/MM/dd HH:mm");
					Calendar now = Calendar.getInstance();  
					now.setTime(cur); 
					Date val1 = dfs.parse(now.get(Calendar.YEAR)+"/"+tmp[0]+" "+tmp[1]);
					val = df.format(val1);
				} else if (val.matches("\\d{4}/[0-1]{1}[0-9]{1}/[0-3]{1}[0-9]{1}\\s[0-2]{1}[0-9]{1}:\\d{2}")){// 2014/04/25 15:47
					val = val.replaceAll("/", "-");
					val += ":00";
				} else if (val.matches("\\d{4}-[0-1]{1}[0-9]{1}-[0-3]{1}[0-9]{1}T[0-2]{1}[0-9]{1}:\\d{2}:\\d{2}\\+\\d{2}:\\d{2}")){// 2013-03-11T17:05:44+00:00
					String tmp[] = val.split("[T\\+]");
					if (tmp.length==3||tmp.length==2){
						val = tmp[0]+" "+tmp[1];
					}
				} else if (val.matches("\\d{4}-[0-1]{1}[0-9]{1}-[0-3]{1}[0-9]{1}\\s[0-2]{1}[0-9]{1}:\\d{2}")){//  2014-06-18 12:07 
					val += ":00";
				}
			} else if (dateType.equals("2")) {
				val += ":00";
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return val;
	}

	static Pattern dateFun = Pattern.compile("(?i)sysdate\\((.*)?\\)");

	public static String sysdate(String exp) {
		java.text.DateFormat df = new SimpleDateFormat(exp);
		return df.format(new Date());
	}

	public static String dateFunCalc(String exp) {
		Matcher m = dateFun.matcher(exp);
		if (m.find()) {
			try {
				String format = m.group(1);
				String val = sysdate(format);
				return exp.replaceAll("(?i)sysdate\\((.*)?\\)", val);
			} catch (Exception e) {
			}
		}
		return exp;
	}
}
