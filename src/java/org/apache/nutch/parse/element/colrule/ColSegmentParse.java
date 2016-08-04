package org.apache.nutch.parse.element.colrule;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.avro.util.Utf8;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.element.DomParser;
import org.apache.nutch.parse.element.ExprCalcRule;
import org.apache.nutch.parse.element.SegParserJob;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;

public class ColSegmentParse implements Serializable {
	private static final long serialVersionUID = 829724512303149188L;
	Utf8 colName;
	List<ExprCalcRule> regRules;// 多个规则解析同一个值

	public ColSegmentParse(Utf8 colName, List<ExprCalcRule> rules) {
		this.colName = colName;
		this.regRules = rules;
	}

	public String toString() {
		return "colName:" + colName + " [regexRules:" + regRules + "]";
	}

	public boolean parseSegMent(String unreverseKey, WebPage page, WebPageSegment psg, ParseUtil util, DomParser htmlParse)
			throws Exception {
		long l = System.currentTimeMillis();
		boolean res = false;
		for (ExprCalcRule rule : regRules) {
			try {
				if (null == rule){
					continue;
				}
				res = rule.parseSegMent(colName, unreverseKey, page, psg, util, htmlParse);
				if (res) {
					break;
				}
			} catch (IOException ex) {
				SegParserJob.LOG.warn(unreverseKey);
			}
		}
		System.out.println(unreverseKey + " ：" + colName + " 用时:" + (System.currentTimeMillis() - l) + "(ms) res=" + res + "\t  rules："
				+ regRules + "\n value:" + psg.getSegMentCnt(colName));
		return res;
	}

	public Utf8 getColName() {
		return colName;
	}

}
