package org.apache.nutch.urlfilter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.nutch.crawl.NutchConstant;

import com.googlecode.aviator.AviatorEvaluator;

public class DateFilter extends ExpFilter implements java.io.Serializable {
	private static final long serialVersionUID = 6692078400937354611L;
	Pattern pattern = null;
	String substitution;
	String val = "";
	boolean calc = false;

	public DateFilter(String regex, String substitution, long ago, String format, boolean calc) {
		this.pattern = Pattern.compile(regex);
		this.substitution = substitution;
		java.text.DateFormat df = new SimpleDateFormat(format);
		long l = System.currentTimeMillis() - ago;
		Date dt = new Date(l);
		val = df.format(dt);
		this.calc = calc;
	}

	public DateFilter(String regex, String substitution, String val, boolean calc) {
		this.pattern = Pattern.compile(regex);
		this.substitution = substitution;
		this.val = val;
		this.calc = calc;
	}

	@Override
	public boolean filter(String url) {
		Matcher matcher = pattern.matcher(url);
		if (matcher.find()) {
			// String date = matcher.replaceAll(substitution);
			String date = NutchConstant.ReplaceRegex(matcher, substitution);
			if (date == null)
				return true;
			// System.err.println("date=" + date + " val=" + val);
			if (calc) {
				try {
					date = AviatorEvaluator.exec(date).toString();
				} catch (Exception e) {
					return true;
				}
			}
			if (date.length() != val.length())// 当不匹配
				return true;
			return date.compareTo(val) >= 0;
		} else {
			return true;
		}
	}

	public String toString() {
		return "DateFilter[regex:" + pattern.pattern() + " substitution:" + substitution + " val:" + val + "]";
	}

	@Override
	public boolean equals(ExpFilter e) {
		return this.toString().equals(e.toString());
	}
}
