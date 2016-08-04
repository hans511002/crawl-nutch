package org.apache.nutch.parse.element.siteimpl;

import java.io.IOException;

import org.apache.avro.util.Utf8;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.element.DomParser;
import org.apache.nutch.parse.element.ExprCalcRule;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;

public class QQNewsExprCalcRule extends ExprCalcRule {

	private static final long serialVersionUID = 8535591062705789656L;

	public boolean parseSegMent(Utf8 segmentColName, String unreverseKey, WebPage page, WebPageSegment psg, ParseUtil util,
			DomParser htmlParse) throws IOException, Exception {
		if (psg.rootNode == null) {
			psg.rootNode = htmlParse.getDomNode(page.getContent().array(), "utf-8");
		}
		if (psg.rootNode == null)
			return false;
		String sid = "<div id=\"C-Main-Article-QQ\">";
		int cindex = psg.content.indexOf(sid);
		if (cindex == -1)
			return false;
		String ncnt = psg.content.substring(cindex + sid.length());
		sid = "<div class=\"hd\">";
		cindex = ncnt.indexOf(sid);
		if (cindex == -1)
			return false;
		ncnt = ncnt.substring(cindex + sid.length());
		Utf8 key = new Utf8("title");
		Utf8 value = new Utf8(ncnt.substring(ncnt.indexOf("<h1>") + 4, ncnt.indexOf("</h1>")));
		psg.putToSegMentCnt(key, value);
		sid = "<span class=\"color-a-0\" bosszone=\"ztTopic\">";
		cindex = ncnt.indexOf(sid);
		if (cindex == -1)
			return false;
		// "<a target=\"_blank\".*?href=\"(.*?)\".*?>(.*?)</a>"

		// Node node = htmlParse.getNodeById(psg.rootNode, "C-Main-Article-QQ");
		return false;
	}

	public String toString() {
		return "QQNewsExprCalcRule";
	}

}
