package org.apache.nutch.urlfilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.DbUpdaterJob;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.net.URLFilter;
import org.apache.nutch.urlfilter.UrlPathMatch.UrlNodeConfig;

/** Creates and caches {@link URLFilter} implementing plugins. */
public class SubURLFilters {
	Configuration conf = null;

	public SubURLFilters(Configuration conf) throws IOException {
		this.conf = conf;
		NutchConstant.urlcfg = NutchConstant.getUrlConfig(conf);
	}

	public static boolean filter(String urlString, List<ExpFilter> subFilters) {
		if (subFilters == null)
			return true;
		boolean regHasTrueFlag = false;// reg +
		boolean regHasFalseFlag = false;// reg +
		for (ExpFilter filter : subFilters) {
			if (!filter.filter(urlString)) {// notMatch + Match -
				if (filter instanceof RegexFilter) {
					boolean regFlag = ((RegexFilter) filter).sign;
					if (regFlag == true) {
						regHasFalseFlag = true;
						continue;
					} else {
						return false;
					}
				}
				DbUpdaterJob.LOG.info("规则：" + filter + " 过滤URL:" + urlString);
				return false;
			} else {// match+ notmatch-
				if (filter instanceof RegexFilter) {
					boolean regFlag = ((RegexFilter) filter).sign;
					if (regFlag == true) {
						regHasTrueFlag = true;
					}
				}
			}
		}
		if (regHasTrueFlag)
			return true;
		if (regHasFalseFlag)
			return false;
		return true;
	}

	/** Run all defined filters. Assume logical AND. */
	public boolean filter(String urlString) {
		System.out.println("filter:" + urlString);
		UrlPathMatch urlSegment = NutchConstant.urlcfg.match(urlString);// 获取配置项
		if (urlSegment == null) {
			return false;
		}
		System.out.println("config:" + urlSegment.getConfigPath());
		UrlNodeConfig value = (UrlNodeConfig) urlSegment.getNodeValue();
		return filter(urlString, value.subFilters);
	}

	// regex: +pattern
	// regex: -pattern
	// datecalc:pattern $1$2 -long format
	public static List<ExpFilter> buildExp(String urlString) {
		if (urlString == null || urlString.equals(""))
			return null;
		List<ExpFilter> filters = new ArrayList<ExpFilter>();
		String[] tmps = urlString.split("\n");// replaceAll("\\r", "").
		for (String rule1 : tmps) {
			try {
				rule1 = rule1.trim();
				if (rule1.equals(""))
					continue;
				if (rule1.startsWith("regex:")) {
					String rule = rule1.substring(6);
					if (rule.charAt(0) == '+') {
						filters.add(new RegexFilter(rule.substring(1), true));
					} else if (rule.charAt(0) == '-') {
						filters.add(new RegexFilter(rule.substring(1), false));
					}
				} else if (rule1.startsWith("datecalc:")) {
					String rule = rule1.substring(9);
					String[] rs = rule.split(" ");
					long ago = Long.parseLong(rs[2]);
					if (ago < 60000) {
						NutchConstant.LOG.warn("subfilter rule error:ago is small=>" + rule1);
					}
					boolean calc = false;
					if (rs.length > 4)
						calc = Boolean.parseBoolean(rs[4]);
					filters.add(new DateFilter(rs[0], rs[1], Long.parseLong(rs[2]), rs[3], calc));
				} else if (rule1.startsWith("datespec:")) {
					String rule = rule1.substring(9);
					String[] rs = rule.split(" ");
					boolean calc = false;
					if (rs.length > 3)
						calc = Boolean.parseBoolean(rs[3]);
					else if (rs.length < 3)
						throw new Exception("指定时间过虑字段数不足3 datespec:.+?\\/(\\d{8})\\/.+ $1 20130101");
					filters.add(new DateFilter(rs[0], rs[1], rs[2], calc));
				} else if (rule1.charAt(0) == '#') {
					continue;
				} else {
					NutchConstant.LOG.warn("subfilter rule error:" + rule1);
				}
			} catch (Exception e) {
				NutchConstant.LOG.warn("subfilter rule error:" + e.getMessage() + " rule:" + rule1);
			}
		}
		return filters.size() > 0 ? filters : null;
	}
}
