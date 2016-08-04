package org.apache.nutch.parse.element;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.element.colrule.ColSegmentParse;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.apache.nutch.storage.WebPageSegment;

public class ExprCalcParses extends SegMentParse {
	private static final long serialVersionUID = 829724512303149188L;
	Configuration conf = null;
	private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();
	public DomParser htmlParse = null;
	private float colRigthPerent = 0.1f;

	// 配置地址,RegexParse
	public static class SegmentGroupRule implements java.io.Serializable {
		private static final long serialVersionUID = -6256086370303361301L;
		public long groupId = 0l;
		public Pattern pattern = null;
		public String groupName = "";
		public List<ColSegmentParse> segmentRuls;

		public String toString() {
			return "[" + groupName + ":" + pattern.pattern() + "] segmentRuls=" + segmentRuls;
		}
	};

	public static HashMap<String, List<SegmentGroupRule>> cfgSegGroupColRuleParses = null;

	public ExprCalcParses() {
	}

	@Override
	public boolean parseSegMent(String unreverseKey, WebPage page, WebPageSegment psg) throws IOException {
		if (ExprCalcParses.cfgSegGroupColRuleParses == null)
			NutchConstant.getSegmentParseRules(conf);
		if (ExprCalcParses.cfgSegGroupColRuleParses == null) {
			System.err.println("解析配置对象为空 cfgSegGroupColRuleParses URL:" + unreverseKey);
			return false;
		}
		Utf8 cfgu = psg.getConfigUrl();
		if (cfgu == null)
			return false;
		String configUrl = cfgu.toString();
		// 获取配置项
		List<SegmentGroupRule> groupRuls = ExprCalcParses.cfgSegGroupColRuleParses.get(configUrl);
		if (groupRuls == null) {
			if (configUrl.charAt(configUrl.length() - 1) == '/')
				groupRuls = ExprCalcParses.cfgSegGroupColRuleParses.get(configUrl.substring(0, configUrl.length() - 1));
			if (groupRuls == null) {
				System.err.println("解析失败：未匹配到解析规则  URL:" + unreverseKey + ",配置地址" + configUrl);
				return false;
			}
		}
		List<ColSegmentParse> segmentRuls = null;
		if (groupRuls.size() == 1) {
			if (groupRuls.get(0).pattern != null) {
				Matcher m = groupRuls.get(0).pattern.matcher(unreverseKey);
				if (m.matches()) {
					segmentRuls = groupRuls.get(0).segmentRuls;
				}
			} else {
				segmentRuls = groupRuls.get(0).segmentRuls;
			}
		} else {
			for (SegmentGroupRule grule : groupRuls) {
				Matcher m = grule.pattern.matcher(unreverseKey);
				if (m.find()) {
					segmentRuls = grule.segmentRuls;
					break;
				}
			}
		}
		if (segmentRuls == null) {
			System.err.println("解析失败：未匹配到对应的分组解析规则 URL:" + unreverseKey + ",配置地址" + configUrl + " 组规则:" + groupRuls);
			return false;
		}
		// List<ColSegmentParse> colParses = ExprCalcParses.expParses.get(configUrl);
		int rightCount = 0;
		for (ColSegmentParse parse : segmentRuls) {
			boolean r = false;
			try {
				r = parse.parseSegMent(unreverseKey, page, psg, util, htmlParse);
				if (r)
					rightCount++;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (rightCount > 3 || ((double) rightCount / segmentRuls.size()) >= colRigthPerent)
			return true;
		else
			return false;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		util = new ParseUtil(conf);
		htmlParse = new DomParser();
		htmlParse.setConf(conf);
		colRigthPerent = conf.getFloat(WebPageSegment.SegmentColParsePerentKey, 0.1f);
	}

	@Override
	public Collection<Field> getFields() {
		return FIELDS;
	}

}
