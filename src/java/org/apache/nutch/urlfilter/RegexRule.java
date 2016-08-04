package org.apache.nutch.urlfilter;

import java.io.Serializable;
import java.util.regex.Pattern;

public class RegexRule implements Serializable {
	private static final long serialVersionUID = -2036849449444299974L;
	public Pattern pattern;
	public String subStr;

	public RegexRule(String pattern, String subStr) {
		this.pattern = Pattern.compile(pattern);
		this.subStr = subStr;
	}

	public String replace(String str) {
		if (this.pattern != null) {
			return this.pattern.matcher(str).replaceAll(this.subStr);
		}
		return str;
	}
}
