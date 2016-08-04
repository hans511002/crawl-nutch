package org.apache.nutch.parse.element;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.element.wordfre.WordFreqRule;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;
import org.apache.nutch.storage.WebPage.Field;

/**
 * 信息正负面解析接口
 * 
 * @author hans
 * 
 */
public class PolicticTypeParses extends SegMentParse {
	private static final long serialVersionUID = -1501883972712797986L;
	Configuration conf = null;
	public static WordFreqRule[] rules = null;// = new WordFreqRule[4];
	int[] sorce = new int[2];
	private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

	public PolicticTypeParses() {
	}

	@Override
	public boolean parseSegMent(String unreverseKey, WebPage page, WebPageSegment psg) throws IOException {
		psg.setPolicticTypeId(0);
		if (rules == null)
			NutchConstant.getSegmentParseRules(conf);
		if (rules == null)
			return false;
		for (int i = 0; i < 2; i++) {
			sorce[i] = 0;
			WordFreqRule parse = rules[i];
			if (parse != null) {
				try {
					sorce[i] = parse.WordFreqCalc(unreverseKey, page, util);
				} catch (Exception e) {

				}
			}
		}
		if (sorce[0] == 0 && sorce[1] == 0) {
			psg.setPolicticTypeId(0);// 未知
		} else if (sorce[0] - 1 > sorce[1]) {
			psg.setPolicticTypeId(1);// 正面
			return true;
		} else if (sorce[0] + 1 < sorce[1]) {
			psg.setPolicticTypeId(2);// 负面
			return true;
		} else {
			psg.setPolicticTypeId(3);// 无正负
			return true;
		}
		return false;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		this.conf = arg0;
		util = new ParseUtil(conf);
	}

	@Override
	public Collection<Field> getFields() {
		return FIELDS;
	}

}
