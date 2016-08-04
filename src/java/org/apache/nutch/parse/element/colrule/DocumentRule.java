package org.apache.nutch.parse.element.colrule;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.util.Utf8;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.element.DomParser;
import org.apache.nutch.parse.element.ExprCalcRule;
import org.apache.nutch.parse.element.SegParserJob;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;
import org.w3c.dom.Node;

import com.googlecode.aviator.AviatorEvaluator;

public class DocumentRule extends ExprCalcRule {
	private static final long serialVersionUID = 4124590198889164535L;
	private String domId = null;
	private String[][] tagPath = null;
	int valueType = 1;// 1:nodeValue 2:nodeAttrValue
	String attrName = null;
	Pattern pattern = null;
	String subpart = null;
	private boolean calc = false;

	/**
	 * dom:domid~valueType~attrName~regex~subpart~calc | dom:domid,tag.class.clsname~valueType~attrName~regex~subpart~calc
	 */
	public DocumentRule(String rule) throws Exception {
		String[] parts = rule.split("~");
		parts[0] = parts[0].trim();
		domId = parts[0].trim();
		String[] domIds = domId.split(",");
		if (domIds.length > 1) {
			domId = domIds[0];
			tagPath = new String[domIds.length - 1][3];
			for (int i = 1; i < domIds.length; i++) {
				String[] tmp = domIds[i].split("\\.");
				tagPath[i - 1][0] = tmp[0].toUpperCase();// 标签大写
				tagPath[i - 1][1] = tmp[1];// if == id ,replace("\\{rowid\\}",rowid);
				tagPath[i - 1][2] = tmp[2];
			}
		}
		if (parts.length > 1)
			valueType = Integer.parseInt(parts[1].trim());
		if (parts.length > 2)
			attrName = parts[2].trim();
		if (parts.length > 4) {
			pattern = Pattern.compile(parts[3].trim());
			subpart = parts[4].trim();
			if (parts.length > 5)
				calc = Boolean.parseBoolean(parts[5].trim());
		}
	}

	public String toString() {
		return "DocumentRule[domId:" + domId + " attrName:" + attrName + " regex:" + pattern.pattern() + " subpart:" + subpart + " calc:"
				+ calc + "]\n";
	}

	@Override
	public boolean parseSegMent(Utf8 colName, String unreverseKey, WebPage page, WebPageSegment psg, ParseUtil util, DomParser htmlParse)
			throws IOException, Exception {
		if (psg.rootNode == null) {
			psg.rootNode = htmlParse.getDomNode(page.getContent().array(), "utf-8");
		}
		if (psg.rootNode == null)
			return false;
		String val = "";
		Node node = null;
		node = htmlParse.getNodeById(psg.rootNode, domId);
		if (this.tagPath != null) {
			node = htmlParse.getNodeByTagPath(node, tagPath);
		}
		if (valueType == 1) {
			val = htmlParse.getNodeValue(node);
		} else if (valueType == 2) {
			val = htmlParse.getNodeAttrValue(node, attrName);
		}
		if (pattern != null) {
			if ("datecalc".equals(this.pattern.pattern())) {
				val = DateCalcUtil.dateCalc(val, this.subpart, page);
			} else {
				Matcher m = null;
				m = pattern.matcher(psg.content);
				if (m == null || !m.find())
					return false;
				// val = NutchConstant.ReplaceRegex(m, this.subpart);
				val = m.replaceAll(this.subpart);
				if (calc) {
					try {
						val = AviatorEvaluator.execute(val).toString();
					} catch (Exception e) {
						SegParserJob.LOG.error(e.getMessage());
						return false;
					}
				}
			}
		}
		psg.putToSegMentCnt(colName, new Utf8(val));
		// System.out.println("url:" + unreverseKey + "segmentName:" + colName + " value:" + val);
		return true;
	}

	// @Override
	// public boolean parseSegMent(String unreverseKey, WebPage page, WebPageSegment psg, ParseUtil util, DomParser domp) throws Exception {
	// String msg = this.getClass().getName() + " not support method[public boolean parseSegMent(String unreverseKey"
	// + ", String content, WebPage page, WebPageSegment psg, ParseUtil util) throws Exception]";
	// SegParserJob.LOG.error(msg);
	// throw new Exception(msg);
	// }
}
