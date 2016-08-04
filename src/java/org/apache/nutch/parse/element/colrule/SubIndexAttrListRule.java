package org.apache.nutch.parse.element.colrule;

import java.io.IOException;
import java.util.HashMap;
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
import org.apache.nutch.parse.element.SegParserReducer;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;
import org.apache.nutch.util.TableUtil;

public class SubIndexAttrListRule extends ExprCalcRule {
	private static final long serialVersionUID = 829724512303149188L;
	Pattern bigBlockPattern = null;
	String bigBlockSubstitution = null;
	int bigBlockIndex = 1;

	Pattern blockPattern = null;
	String blockSubstitution = null;
	Pattern urlkeyPattern = null;
	String urlkeySubstitution = null;

	Pattern colPattern[] = null;
	String colSubstitution[] = null;
	boolean isDelTag[] = null;
	String colName[] = null;

	/**
	 * subindexattrlist:大范围块正则~大范围块取值~循环索引位置~块正则~块取值~rowkey 正则~rowkey取值 [~rexgex~substitution~isDelTag~colname]
	 * subindexattrlist:indexSplitStartStr~indexSplitEndStr~rexgex~substitution~rexgex~substitution~rexgex~substitution~isDelTag~colname
	 * ~rexgex~substitution~isDelTag~colname
	 */
	public SubIndexAttrListRule(String rule) throws Exception {
		String[] parts = rule.split("~");
		for (int i = 0; i < parts.length; i++) {
			parts[i] = parts[i].trim();
		}
		bigBlockPattern = Pattern.compile(parts[0].trim());
		bigBlockSubstitution = parts[1].trim();
		bigBlockIndex = Integer.parseInt(parts[2].trim());
		this.blockPattern = Pattern.compile(parts[3]);
		blockSubstitution = parts[4];
		this.urlkeyPattern = Pattern.compile(parts[5]);
		urlkeySubstitution = parts[6];
		int colLen = ((parts.length - 7) + 3) / 4;
		if (colLen == 0) {
			throw new Exception("SubAttrListRule解析规则配置错误：" + rule);
		}
		colPattern = new Pattern[colLen];
		colSubstitution = new String[colLen];
		isDelTag = new boolean[colLen];
		colName = new String[colLen];

		int index = 7;
		for (int i = 0; i < colLen; i++) {
			colPattern[i] = Pattern.compile(parts[index++]);
			colSubstitution[i] = parts[index++];
			isDelTag[i] = Boolean.parseBoolean(parts[index++]);
			if (parts.length > index)
				colName[i] = parts[index++];
		}
	}

	public String toString() {
		return "RegexRuleList[regex:" + "~" + bigBlockPattern.pattern() + "~" + bigBlockSubstitution + "~" + bigBlockIndex+ "~" + blockPattern.pattern() + "~"
				+ blockSubstitution + "~" + isDelTag + "]";
	}

	public boolean parseSegMent(Utf8 colName, String unreverseKey, WebPage page, WebPageSegment psg, ParseUtil util, DomParser htmlParse)
			throws IOException, Exception {
		SegParserReducer.LOG.debug("url:" + unreverseKey + " segmentName:" + colName + " SubAttrListRule:" + toString());
		psg.setContent(page);
		if (psg.content == null || psg.content.equals(""))
			return false;
		Matcher m = null;
		String content = psg.content;
		m = bigBlockPattern.matcher(psg.content);
		int index = 1;
		String tempContent = psg.content;
		while (m != null && m.find()) {
			int pos = m.end();
			if (pos < 0 || pos >= tempContent.length())
				break;
			tempContent = tempContent.substring(m.end());
			content = NutchConstant.ReplaceRegex(m, this.bigBlockSubstitution);
			m = bigBlockPattern.matcher(tempContent);
			if (index == this.bigBlockIndex){
				break;
			}
			index++;
		}
		System.out.println(content);
		m = blockPattern.matcher(content);
		// System.err.println(content);
		while (m != null && m.find()) {
			int pos = m.end();
			if (pos < 0 || pos >= content.length())
				break;
			String val = "";
			content = content.substring(m.end());
			String blockVal = NutchConstant.ReplaceRegex(m, this.blockSubstitution);
			m = blockPattern.matcher(content);
			if (blockVal != null) {
				Matcher lm = urlkeyPattern.matcher(blockVal);
				if (lm != null && lm.find()) {
					String link = NutchConstant.ReplaceRegex(lm, this.urlkeySubstitution);
					if (link != null) {
						WebPageSegment spsg = null;

						try {
							if (psg.subWps != null) {
								spsg = psg.subWps.get(link);
							}
							if (spsg == null)
								spsg = new WebPageSegment();
							boolean sucess = false;
							for (int i = 0; i < colPattern.length; i++) {
								Matcher cm = colPattern[i].matcher(blockVal);
								if (cm != null && cm.find()) {
									String colVal = NutchConstant.ReplaceRegex(cm, this.colSubstitution[i]);
									if (isDelTag[i]) {
										try {
											Parse parse = util.parse(unreverseKey, colVal, SegMentParse.contentType);
											if (parse != null)
												colVal = parse.getText();
										} catch (Exception e) {
											SegParserJob.LOG.error(e.getMessage());
										}
									}
									sucess = true;
									Utf8 cName = null;
									if (this.colName[i] != null) {
										cName = new Utf8(this.colName[i]);
									} else {
										cName = colName;
									}
									spsg.putToSegMentCnt(new Utf8(this.colName[i]), new Utf8(colVal));
								}
							}
							if (psg.subWps == null) {
								psg.subWps = new HashMap<String, WebPageSegment>();
							}
							if (sucess && !psg.subWps.containsKey(link))
								psg.subWps.put(link, spsg);
						} catch (Exception e) {
							SegParserReducer.LOG.error("url:" + unreverseKey + " segmentName:" + colName + " 解析异常：" + toString());
						}
					}
				}
			}
		}
		return true;
	}
	
//	public static void main(String[] args) throws Exception {
//	rule ="<h2 class=\"lastest\">latest news</h2>~" +
//	"<div class=\"aside\">~" +
//	"<article class=\"posts post-1 cf\">([\\s\\S]*?)</article>~" +
//	"$1~" +
//	"<h1><a href=\"([\\s\\S]*?)\" data-no-turbolink~"+
//	"http://www.36kr.com$1~" +
//	
//	
//	"<a href=\"/category/cn-startups\" class=\"us-startups\">([\\s\\S]*?)</a>~" +
//	"$1~" +
//	"true~" +
//	"cateName~" +
//	
//	"<p>([\\s\\S]*?)</p>~" +
//	"$1~" +
//	"true~" +
//	"childTitle";
//		String s ="<h2 class=\"lastest\">latest news</h2>~<div class=\"aside\">~<article class=\"posts post-1 cf\">([\\s\\S]*?)</article>~<div class=\"info cf\">([\\s\\S]*?)</div><h1><a href=\"([\\s\\S]*?)\" data-no-turbolink~http://www.36kr.com$1~<a href=\"/category/cn-startups\" class=\"us-startups\">([\\s\\S]*?)</a>~$1~true~cateName~<p>([\\s\\S]*?)</p>~$1~true~childTitle";
//		SubAttrListRule r = new SubAttrListRule(s);
//	}
}
