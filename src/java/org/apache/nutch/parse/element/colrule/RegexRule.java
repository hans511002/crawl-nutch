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
import org.apache.nutch.parse.element.SegMentParsers;
import org.apache.nutch.parse.element.SegParserJob;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;
import org.apache.nutch.util.TableUtil;

import com.googlecode.aviator.AviatorEvaluator;

public class RegexRule extends ExprCalcRule {
	private static final long serialVersionUID = 829724512303149188L;
	SegMentParsers.Field filed = null;
	private Utf8 keyName = null;
	private boolean calc = false;
	private boolean calcDate = false;
	String indexSplitStartStr = null;
	String indexSplitEndStr = null;
	Pattern pattern = null;
	String substitution = null;
	boolean isDelTag = true;

	/**
	 * regex:字段(如果是headers\meta需要连接二级字段:headers.Content-Type)~indexSplitStartStr~indexSplitEndStr~rexgex~substitution~isDelTag~calc
	 * 
	 * @param rule
	 * @throws Exception
	 */
	public RegexRule(String rule) throws Exception {
		String[] parts = rule.split("~");
		SegMentParsers.Field filed = null;
		String keyName = null;
		parts[0] = parts[0].trim();
		if (parts[0].indexOf('.') > 0) {
			filed = SegMentParsers.Field.getFiled(parts[0].substring(0, parts[0].indexOf('.')));
			keyName = parts[0].substring(parts[0].indexOf('.') + 1);
		} else {
			filed = SegMentParsers.Field.getFiled(parts[0]);
		}
		if (filed == null || parts.length < 3) {
			throw new Exception("segment parse regexRule error:" + rule);
		}
		indexSplitStartStr = parts[1].trim();
		indexSplitEndStr = parts[2].trim();
		String regex = null;
		if (parts.length > 3)
			regex = parts[3].trim();
		if (parts.length > 4)
			substitution = parts[4].trim();
		if (parts.length > 5)
			isDelTag = Boolean.parseBoolean(parts[5].trim());
		if (parts.length > 6)
			calc = Boolean.parseBoolean(parts[6].trim());
		if (parts.length > 7)
			calcDate = Boolean.parseBoolean(parts[7].trim());
		this.filed = filed;
		if (keyName != null && !keyName.equals(""))
			this.keyName = new Utf8(keyName);
		if (regex != null && !regex.equals(""))
			this.pattern = Pattern.compile(regex);
	}

	public String toString() {
		return "RegexRule[regex:" + filed + (keyName != null ? "." + keyName : "") + "~" + indexSplitStartStr + "~" + indexSplitEndStr
				+ "~" + pattern.pattern() + "~" + substitution + "~" + isDelTag + "~" + calc + "]";
	}

	@Override
	public boolean parseSegMent(Utf8 colName, String unreverseKey, WebPage page, WebPageSegment psg, ParseUtil util, DomParser htmlParse)
			throws IOException, Exception {
		Matcher m = null;
		System.out.println("url:" + unreverseKey + " segmentName:" + colName + " RegexListRule:" + toString());
		boolean isContent = false;
		if (SegMentParsers.Field.CONTENT.equals(filed)) {
			psg.setContent(page);
			if (psg.content == null || psg.content.equals(""))
				return false;
			if (indexSplitStartStr != null && !indexSplitStartStr.equals("")) {
				// int index = psg.content.indexOf(indexSplitStartStr);
				int index = TableUtil.indexOf(psg.content, indexSplitStartStr, true);
				int eindex = psg.content.length();
				if (psg.content != null && !psg.content.equals(""))
					// eindex = psg.content.indexOf(indexSplitEndStr);
					eindex = TableUtil.indexOf(psg.content, indexSplitEndStr, true);

				if (index > 0) {
					if (eindex > index)
						m = pattern.matcher(psg.content.substring(index, eindex));
					else
						m = pattern.matcher(psg.content.substring(index));
				} else {
					return false;// 不再匹配
					// m = pattern.matcher(psg.content);
				}
			} else {
				m = pattern.matcher(psg.content);
			}
			isContent = true;
		} else if (SegMentParsers.Field.BASE_URL.equals(filed)) {
			m = pattern.matcher(unreverseKey);
		} else if (SegMentParsers.Field.TITLE.equals(filed)) {
			m = pattern.matcher(page.getTitle().toString());
		} else if (SegMentParsers.Field.TEXT.equals(filed)) {
			m = pattern.matcher(page.getText().toString());
		} else if (SegMentParsers.Field.HEADERS.equals(filed)) {
			Utf8 c = page.getFromHeaders(this.keyName);
			if (c == null || c.toString().equals(""))
				return false;
			m = pattern.matcher(c.toString());
		} else if (SegMentParsers.Field.METADATA.equals(filed)) {
			Utf8 c = page.getFromHeaders(this.keyName);
			if (c == null || c.toString().equals(""))
				return false;
			m = pattern.matcher(c.toString());
		} else if (SegMentParsers.Field.CONFIGURL.equals(filed)) {
			m = pattern.matcher(page.getConfigUrl().toString());
		} else if (SegMentParsers.Field.CONTENT_TYPE.equals(filed)) {
			m = pattern.matcher(page.getContentType().toString());
		}
		String val = null;
		if (SegMentParsers.Field.FETCH_TIME.equals(filed)) {
			val = page.getPrevFetchTime() + "";
		} else if (SegMentParsers.Field.MODIFIED_TIME.equals(filed)) {
			val = page.getModifiedTime() + "";
		} else {
			if (m != null && m.find()) {
				val = NutchConstant.ReplaceRegex(m, this.substitution);
				if (val == null)
					return false;
				// System.err.println(" pattern=" + pattern + " substitution=" + this.substitution);
				// System.err.println("m.group(1)=" + m.group(1));
				// System.err.println("m.group(2)=" + m.group(2));
				// val = m.replaceAll(this.substitution);
				// if (!val.equals(content))
				// System.err.println(val.replaceAll("\n", "\\n").replaceAll("\r", ""));
			} else {
				// System.err.println(content);
				// System.out.println("url:" + unreverseKey + " segmentName:" + colName + " is null \n pattern=" + pattern
				// + " \n substitution=" + this.substitution);
				return false;
			}
		}
		if (isContent && isDelTag) {// 去除HTML标签
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
		if (calc) {
			try {
				val = DateCalcUtil.dateFunCalc(val);
				val = AviatorEvaluator.execute(val).toString();
			} catch (Exception e) {
				SegParserJob.LOG.error(e.getMessage());
			}
		}
		if (calcDate) {
			val = DateCalcUtil.dateCalc(val, "1", page);
		}
		psg.putToSegMentCnt(colName, new Utf8(val));
		System.out.println(unreverseKey + " ：" + colName + " segmentName:" + " value:" + val);
		return true;
	}
}
