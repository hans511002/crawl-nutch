package org.apache.nutch.parse.element.wordfre;

import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;

/**
 * 信息正负面解析规则
 * 
 * @author hans
 * 
 */
public class WordFreqAttrCalc extends WordFreqRule {

	private static final long serialVersionUID = -5188664147693580010L;
	WordFreqRule.ParseRuleType type;
	int attrId = 0;
	String rule;

	public WordFreqAttrCalc(WordFreqRule.ParseRuleType type, int attrId, String rule) throws Exception {
		this.type = type;
		this.attrId = attrId;
		this.rule = rule;
	}

	@Override
	public String toString() {
		return rule;
	}

	@Override
	public int WordFreqCalc(String unreverseKey, WebPage page, ParseUtil util) throws Exception {
		if (page.wordPre == null) {
			genertWordFre(page);
		}
		return 0;
	}

	@Override
	public boolean WordFreqCalc(String unreverseKey, String text, WebPage page, WebPageSegment psg, ParseUtil util) throws Exception {
		if (page.wordPre == null) {
			genertWordFre(page);
		}
		return false;
	}

}
