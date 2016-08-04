package org.apache.nutch.parse.element;

import java.io.IOException;

import org.apache.avro.util.Utf8;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;

public abstract class ExprCalcRule implements java.io.Serializable {
	private static final long serialVersionUID = 389146371176864982L;

	/**
	 * 用于内容要素解析使用
	 * 
	 * @param segmentColName
	 * @param unreverseKey
	 * @param content
	 * @param page
	 * @param psg
	 * @param util
	 * @return
	 * @throws Exception
	 */
	public abstract boolean parseSegMent(Utf8 segmentColName, String unreverseKey, WebPage page, WebPageSegment psg, ParseUtil util,
			DomParser htmlParse) throws IOException, Exception;

	public abstract String toString();

}
