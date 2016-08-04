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

public class SubAttrListRuleTopic extends ExprCalcRule {
	private static final long serialVersionUID = 829724512303149188L;
	String indexSplitStartStr = null;
	String indexSplitEndStr = null;

	Pattern blockPattern = null;
	String blockSubstitution = null;
	Pattern urlkeyPattern = null;
	String urlkeySubstitution = null;

	Pattern colPattern[] = null;
	String colSubstitution[] = null;
	boolean isDelTag[] = null;
	String colName[] = null;
	
	// 页面只有一个地方显示属性信息
	Pattern colTopicPattern[] = null;
	String colTopicSubstitution[] = null;
	boolean isTopicDelTag[] = null;
	String colTopicName[] = null;

	/**
	 * subattrlisttopic:[rexgex~substitution~isDelTag~colname]~~开始字符~结束字符~块正则~块取值~rowkey正则~rowkey取值 [~rexgex~substitution~isDelTag~colname]
	 * subattrlist:indexSplitStartStr~indexSplitEndStr~rexgex~substitution~rexgex~substitution~rexgex~substitution~isDelTag~colname
	 * ~rexgex~substitution~isDelTag~colname
	 */
	public SubAttrListRuleTopic(String rule) throws Exception {
		String ruleLst[] = rule.split("~~");
		if (ruleLst.length !=2){
			return;
		}
		
		// 统一字段规则
		String topicPart[] = ruleLst[0].split("~");
		int topicColLen = topicPart.length / 4;
		if (topicColLen == 0) {
			throw new Exception("SubAttrListRule解析规则配置错误：" + rule);
		}
		colTopicPattern = new Pattern[topicColLen];
		colTopicSubstitution = new String[topicColLen];
		isTopicDelTag = new boolean[topicColLen];
		colTopicName = new String[topicColLen];
		
		int topicIndex = 0;
		for (int i = 0; i < topicColLen; i++) {
			colTopicPattern[i] = Pattern.compile(topicPart[topicIndex++]);
			colTopicSubstitution[i] = topicPart[topicIndex++];
			isTopicDelTag[i] = Boolean.parseBoolean(topicPart[topicIndex++]);
			if (topicPart.length > topicIndex)
				colTopicName[i] = topicPart[topicIndex++];
		}
		
		// 模块字段规则
		String[] parts = ruleLst[1].split("~");
		for (int i = 0; i < parts.length; i++) {
			parts[i] = parts[i].trim();
		}
		indexSplitStartStr = parts[0].trim();
		indexSplitEndStr = parts[1].trim();
		this.blockPattern = Pattern.compile(parts[2]);
		blockSubstitution = parts[3];
		this.urlkeyPattern = Pattern.compile(parts[4]);
		urlkeySubstitution = parts[5];
		int colLen = ((parts.length - 6) + 3) / 4;
		if (colLen == 0) {
			throw new Exception("SubAttrListRule解析规则配置错误：" + rule);
		}
		colPattern = new Pattern[colLen];
		colSubstitution = new String[colLen];
		isDelTag = new boolean[colLen];
		colName = new String[colLen];

		int index = 6;
		for (int i = 0; i < colLen; i++) {
			colPattern[i] = Pattern.compile(parts[index++]);
			colSubstitution[i] = parts[index++];
			isDelTag[i] = Boolean.parseBoolean(parts[index++]);
			if (parts.length > index)
				colName[i] = parts[index++];
		}
	}

	public String toString() {
		return "RegexRuleList[regex:" + "~" + indexSplitStartStr + "~" + indexSplitEndStr + "~" + blockPattern.pattern() + "~"
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
		
		// 解析topic字段
		String[] topicName = new String[this.colTopicPattern.length];
		String[] topicValue = new String[this.colTopicPattern.length];
		for (int i = 0; i < this.colTopicPattern.length; i++) {
			Matcher cm = colTopicPattern[i].matcher(content);
			if (cm != null && cm.find()) {
				String colVal = NutchConstant.ReplaceRegex(cm, this.colTopicSubstitution[i]);
				if (isTopicDelTag[i]) {
					try {
						Parse parse = util.parse(unreverseKey, colVal, SegMentParse.contentType);
						if (parse != null)
							colVal = parse.getText();
					} catch (Exception e) {
						SegParserJob.LOG.error(e.getMessage());
					}
				}
				topicName[i]=this.colTopicName[i];
				topicValue[i]=colVal;
			}
		}
		
		// 解析块
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
							
							for (int i = 0; i < topicName.length; i++) {
								spsg.putToSegMentCnt(new Utf8(topicName[i]), new Utf8(topicValue[i]));
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
