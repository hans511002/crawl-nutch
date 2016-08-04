package org.apache.nutch.parse.element.colrule;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.util.Utf8;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.element.DomParser;
import org.apache.nutch.parse.element.ExprCalcRule;
import org.apache.nutch.parse.element.SegParserReducer;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;
import org.apache.nutch.util.TableUtil;

public class ListAttrListRule extends ExprCalcRule {
	private static final long serialVersionUID = 829724512303149188L;
	String indexSplitStartStr = null;
	String indexSplitEndStr = null;

	Pattern urlkeyPattern = null;
	String urlkeySubstitution = null;

	Pattern colPattern[] = null;
	String splitsign[] = null;
	String colName[] = null;

	/**
	 * listattrlist:开始字符~结束字符 [~rexgex~splitsign~~colname]
	 * listattrlist:indexSplitStartStr~indexSplitEndStr~rexgex~splitsign~colname
	 * ~rexgex~splitsign~colname
	 */
	public ListAttrListRule(String rule) throws Exception {
		String[] parts = rule.split("~");
		for (int i = 0; i < parts.length; i++) {
			parts[i] = parts[i].trim();
		}
		indexSplitStartStr = parts[0].trim();
		indexSplitEndStr = parts[1].trim();
		int colLen = ((parts.length - 2)) / 3;
		if (colLen == 0) {
			throw new Exception("sublistattrlist解析规则配置错误：" + rule);
		}
		colPattern = new Pattern[colLen];
		splitsign = new String[colLen];
		colName = new String[colLen];

		int index = 2;
		for (int i = 0; i < colLen; i++) {
			colPattern[i] = Pattern.compile(parts[index++]);
			splitsign[i] = parts[index++];
			if (parts.length > index)
				colName[i] = parts[index++];
		}
	}

	public String toString() {
		return "RegexRuleList[regex:" + "~" + indexSplitStartStr + "~" + indexSplitEndStr + "~"
				 + "~" + splitsign + "]";
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
		Set<String> list = new HashSet<String>();
		for (int i = 0; i < colPattern.length; i++) {
			Matcher cm = colPattern[i].matcher(content);
			StringBuilder sval = new StringBuilder();
			boolean flag = false;
			list.clear();
			while (cm != null && cm.find()) {
				int gl = cm.groupCount();
				if (gl<=0){
					break;
				}
				String gv = cm.group(0);
				if (list.contains(gv)){
					continue;
				}
				list.add(gv);
				if (flag){
					sval.append(this.splitsign[i]);
				}
				sval.append(gv);
				flag = true;
			}
			
			if (flag){
				psg.putToSegMentCnt(new Utf8(this.colName[i]), new Utf8(sval.toString()));
			}
		}
//		m = blockPattern.matcher(content);
//		// System.err.println(content);
//		while (m != null && m.find()) {
//			int pos = m.end();
//			if (pos < 0 || pos >= content.length())
//				break;
//			String val = "";
//			content = content.substring(m.end());
//			String blockVal = NutchConstant.ReplaceRegex(m, this.blockSubstitution);
//			m = blockPattern.matcher(content);
//			if (blockVal != null) {
//				Matcher lm = urlkeyPattern.matcher(blockVal);
//				if (lm != null && lm.find()) {
//					String link = NutchConstant.ReplaceRegex(lm, this.urlkeySubstitution);
//					if (link != null) {
//						WebPageSegment spsg = null;
//
//						try {
//							if (psg.subWps != null) {
//								spsg = psg.subWps.get(link);
//							}
//							if (spsg == null)
//								spsg = new WebPageSegment();
//							boolean sucess = false;
//							for (int i = 0; i < colPattern.length; i++) {
//								Matcher cm = colPattern[i].matcher(blockVal);
//								if (cm != null && cm.find()) {
//									String colVal = NutchConstant.ReplaceRegex(cm, this.colSubstitution[i]);
//									if (splitsign[i]) {
//										try {
//											Parse parse = util.parse(unreverseKey, colVal, SegMentParse.contentType);
//											if (parse != null)
//												colVal = parse.getText();
//										} catch (Exception e) {
//											SegParserJob.LOG.error(e.getMessage());
//										}
//									}
//									sucess = true;
//									Utf8 cName = null;
//									if (this.colName[i] != null) {
//										cName = new Utf8(this.colName[i]);
//									} else {
//										cName = colName;
//									}
//									spsg.putToSegMentCnt(new Utf8(this.colName[i]), new Utf8(colVal));
//								}
//							}
//							if (psg.subWps == null) {
//								psg.subWps = new HashMap<String, WebPageSegment>();
//							}
//							if (sucess && !psg.subWps.containsKey(link))
//								psg.subWps.put(link, spsg);
//						} catch (Exception e) {
//							SegParserReducer.LOG.error("url:" + unreverseKey + " segmentName:" + colName + " 解析异常：" + toString());
//						}
//					}
//				}
//			}
//		}
		return true;
	}
	
	public static void main(String[] args) throws Exception {
	String rule ="<h2 class=\"lastest\">latest news</h2>~" +
	"<div class=\"aside\">~" +
	"<article class=\"posts post-1 cf\">([\\s\\S]*?)</article>~" +
	"$1~" +
	"<h1><a href=\"([\\s\\S]*?)\" data-no-turbolink~"+
	"http://www.36kr.com$1~" +
	
	
	"<a href=\"/category/cn-startups\" class=\"us-startups\">([\\s\\S]*?)</a>~" +
	"$1~" +
	"true~" +
	"cateName~" +
	
	"<p>([\\s\\S]*?)</p>~" +
	"$1~" +
	"true~" +
	"childTitle";
		String s ="<h2 class=\"lastest\">latest news</h2>~<div class=\"aside\">~<article class=\"posts post-1 cf\">([\\s\\S]*?)</article>~<div class=\"info cf\">([\\s\\S]*?)</div><h1><a href=\"([\\s\\S]*?)\" data-no-turbolink~http://www.36kr.com$1~<a href=\"/category/cn-startups\" class=\"us-startups\">([\\s\\S]*?)</a>~$1~true~cateName~<p>([\\s\\S]*?)</p>~$1~true~childTitle";
		ListAttrListRule r = new ListAttrListRule(s);
	}
}
