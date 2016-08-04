package org.apache.nutch.parse.element.colrule;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.util.Utf8;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.element.DomParser;
import org.apache.nutch.parse.element.ExprCalcRule;
import org.apache.nutch.parse.element.SegMentParse;
import org.apache.nutch.parse.element.SegParserJob;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;
import org.apache.nutch.util.TableUtil;

public class RegexListRule extends ExprCalcRule {
	private static final long serialVersionUID = 829724512303149188L;
	Pattern pattern = null;
	String substitution = null;
	boolean isDelTag = true;
	String indexSplitStartStr = null;
	String indexSplitEndStr = null;

	/**
	 * regexlist:indexSplitStartStr~indexSplitEndStr~rexgex~substitution~isDelTag
	 * 
	 * block rule rowkey-link url ?asfgs=&# val-rule
	 * 
	 * @param rule
	 * @throws Exception
	 */
	public RegexListRule(String rule) throws Exception {
		String[] parts = rule.split("~");
		parts[0] = parts[0].trim();

		String regex = null;
		indexSplitStartStr = parts[0].trim();
		indexSplitEndStr = parts[1].trim();

		regex = parts[2].trim();
		if (parts.length > 3)
			substitution = parts[3].trim();
		if (parts.length > 4)
			isDelTag = Boolean.parseBoolean(parts[4].trim());
		if (regex != null && !regex.equals(""))
			this.pattern = Pattern.compile(regex);
	}

	public String toString() {
		return "RegexRuleList[regex:" + "~" + indexSplitStartStr + "~" + indexSplitEndStr + "~" + pattern.pattern() + "~" + substitution
				+ "~" + isDelTag + "]";
	}

	public boolean parseSegMent(Utf8 colName, String unreverseKey, WebPage page, WebPageSegment psg, ParseUtil util, DomParser htmlParse)
			throws IOException, Exception {
		System.out.println("url:" + unreverseKey + " segmentName:" + colName + " RegexListRule:" + toString());
		psg.setContent(page);
		if (psg.content == null || psg.content.equals(""))
			return false;
		Matcher m = null;
		String content = psg.content;
		if (indexSplitStartStr != null && !indexSplitStartStr.equals("")) {
			// psg.content.indexOf(indexSplitStartStr);//
			int index = TableUtil.indexOf(psg.content, indexSplitStartStr, true);
			int eindex = psg.content.length();
			if (psg.content != null && !psg.content.equals("")) {
				eindex = TableUtil.indexOf(psg.content, indexSplitEndStr, true);
			}
			// eindex = psg.content.indexOf(indexSplitEndStr);
			if (index < 0)
				return false;
			if (eindex > index) {
				content = psg.content.substring(index, eindex);
			} else {
				// return false;
				content = psg.content.substring(index);
			}
		}
		m = pattern.matcher(content);
		String val = "";
		// System.err.println(content);
		while (m != null && m.find()) {
			int pos = m.end();
			String tmp = NutchConstant.ReplaceRegex(m, this.substitution);
			if (tmp != null)
				val += tmp;
			if (pos < 0 || pos >= content.length())
				break;
			content = content.substring(pos);
			m = pattern.matcher(content);
		}
		if ("".equals(val)) {
			// System.out.println("url:" + unreverseKey + " segmentName:" + colName + " is null \n pattern=" + pattern + " \n substitution="
			// + this.substitution);
			return false;
		}
		if (isDelTag) {// 去除HTML标签
			try {
				Parse parse = util.parse(unreverseKey, val, SegMentParse.contentType);
				if (parse != null)
					val = parse.getText();
			} catch (Exception e) {
				SegParserJob.LOG.error(e.getMessage());
			}
		}
		if (val != null) {
			val = val.replaceAll("&nbsp;", " ");
			val = val.replaceAll("\\s", " ");
			while (val.indexOf("  ") >= 0) {
				val = val.replaceAll("  ", " ");
			}
		}
		psg.putToSegMentCnt(colName, new Utf8(val));
		// System.out.println("url:" + unreverseKey + " segmentName:" + colName + " value:" + val);
		return true;
	}

}
