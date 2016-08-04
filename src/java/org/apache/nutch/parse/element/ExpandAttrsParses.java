package org.apache.nutch.parse.element;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.element.wordfre.WordFreqRule;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;

/**
 * 未实现 每一个属性设置一组规则或者一个特定的类解析
 * 
 * @author hans
 * 
 */
public class ExpandAttrsParses extends SegMentParse {
	private static final long serialVersionUID = -2059005394572393616L;
	Configuration conf = null;
	public static List<WordFreqRule> rules = new ArrayList<WordFreqRule>();
	private static final Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

	public ExpandAttrsParses() {
	}

	@Override
	public boolean parseSegMent(String unreverseKey, WebPage page, WebPageSegment psg) throws IOException {
		if (rules == null)
			NutchConstant.getSegmentParseRules(conf);
		if (rules == null)
			return false;
		boolean res = false;
		String text = page.getText().toString();
		for (WordFreqRule parse : rules) {
			boolean r = false;
			try {
				r = parse.WordFreqCalc(unreverseKey, text, page, psg, util);
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (res)
				res = r;
		}
		return res;
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

	public Collection<WebPage.Field> getFields() {
		return FIELDS;
	}

}
