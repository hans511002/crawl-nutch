package org.apache.nutch.parse.element.wordfre;

import java.util.HashMap;

import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;

/**
 * 信息正负面解析规则
 * 
 * @author hans
 * 
 */
public abstract class WordFreqRule implements java.io.Serializable {
	public static enum ParseRuleType {
		POLICITIC_TYPE, ATTR_TYPE;
	}

	// 相似比较,出现词频前面多少个
	public static int wordTopNum = 50;

	private static final long serialVersionUID = -3090113192592334774L;

	public abstract int WordFreqCalc(String unreverseKey, WebPage page, ParseUtil util) throws Exception;

	public abstract boolean WordFreqCalc(String unreverseKey, String text, WebPage page, WebPageSegment psg, ParseUtil util)
			throws Exception;

	public abstract String toString();

	public static void genertWordFre(WebPage page) {
		if (page.wordPre != null)
			return;
		page.wordPre = new HashMap<String, Integer>();// 词频

	}

}
