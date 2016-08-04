package org.apache.nutch.parse.element.colrule;

import java.io.IOException;

import org.apache.avro.util.Utf8;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.element.DomParser;
import org.apache.nutch.parse.element.ExprCalcRule;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;

/**
 * 反射调用定制解析类来完成某一个字段值的解析
 * 
 * @author hans
 * 
 */
public class ClassRule extends ExprCalcRule {
	private static final long serialVersionUID = 829724512303149188L;
	String className;
	ExprCalcRule rule = null;

	public ClassRule(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		this.className = className;
		Class<?> cls = Class.forName(className);
		rule = (ExprCalcRule) cls.newInstance();
	}

	public String toString() {
		return "ClassRule[" + className + "]";
	}

	public boolean parseSegMent(Utf8 segmentColName, String unreverseKey, WebPage page, WebPageSegment psg, ParseUtil util,
			DomParser htmlParse) throws IOException, Exception {
		return rule.parseSegMent(segmentColName, unreverseKey, page, psg, util, htmlParse);
	}

}
