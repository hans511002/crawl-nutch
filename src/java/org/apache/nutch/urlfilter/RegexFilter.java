package org.apache.nutch.urlfilter;

import java.util.regex.Pattern;

public class RegexFilter extends ExpFilter implements java.io.Serializable {
	private static final long serialVersionUID = 829724512303149188L;
	Pattern pattern;
	public boolean sign;

	public RegexFilter(String regex, boolean sign) {
		this.sign = sign;
		this.pattern = Pattern.compile(regex);
	}

	@Override
	public boolean filter(String url) {
		if (pattern.matcher(url).find()) {
			return (this.sign);
		} else {
			return !this.sign;
		}
	}

	public String toString() {
		return "RegexFilter[sign:" + sign + " regex:" + pattern.pattern() + "]";
	}

	@Override
	public boolean equals(ExpFilter e) {
		return this.toString().equals(e.toString());
	}
}
