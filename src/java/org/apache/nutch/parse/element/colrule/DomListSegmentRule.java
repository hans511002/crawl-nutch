package org.apache.nutch.parse.element.colrule;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.MD5Hash;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.element.DomParser;
import org.apache.nutch.parse.element.ExprCalcRule;
import org.apache.nutch.parse.element.SegParserJob;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;
import org.codehaus.jackson.map.ObjectMapper;
import org.w3c.dom.Node;

import com.googlecode.aviator.AviatorEvaluator;

/**
 * 
 * @author hans
 * 
 */
public class DomListSegmentRule extends ExprCalcRule {
	private static final long serialVersionUID = -1480637835194938420L;
	List<blockRegexRule> gbColRule;
	List<String> busKeys;
	List<String> parBusKeys;
	boolean res = false;

	// domid,tag.attr.clsname,tag.attr.clsname
	public static class blockRegexRule implements Serializable {
		public String blockId = null;
		private String[][] tagPath = null;
		ListRowRegexRule[] subRowRule = null;// 多个标签组合成一行
	}

	// isRegexType, orgin_pos_9,prev
	public static class ListRowRegexRule implements Serializable {
		public boolean isRegexType = false;// 表示前缀匹配还是正则匹配
		public String subRowPri = null;// 存在{rid}宏变量 orgin_pos_9 orgin_pos_9:next orgin_pos_9:prev
		int offsetNum = 0;
		int directionType = 0;
		ColRegexRule[] colRules = null;
	}

	// colname~tag.class.clsname~valueType~attrName~regex~subpart~calc
	// colname~tag.attr.clsname
	public static class ColRegexRule implements Serializable {
		String colName = null;
		private String[][] tagPath = null;// 值中 存在{rid}宏变量 // if == id ,replace("\\{rowid\\}",rowid);
		public boolean isList = false;
		ListRowRegexRule listRule = null;// 子行规则使用列规则取子行
		// 格式化计算
		int valueType = 1;// 1:nodeValue 2:nodeAttrValue
		String attrName = null;
		Pattern pattern = null;// regex datecalc
		String subpart = null;// subpart datetype
		boolean calc = false;
	}

	/**
	 * @param rule
	 *            domlist :业务主键字段名称列表~父级主键 三层或以上MAP: KEY:块获取规则 domid,tag.class:clsname ValueMap: vKey:行获取规则 domid,tag.class:clsname rowpri
	 *            list vvArray: domid,tag.class:clsname~valueType~attrName~regex~subpart~calc //列中还存在子行规则，子行存在列规则
	 * 
	 * @param rule
	 *            domid tag.class:clsname
	 */
	@SuppressWarnings("unchecked")
	public DomListSegmentRule(String rule) throws Exception {
		gbColRule = new ArrayList<blockRegexRule>();
		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> blockRules = null;
		Map<String, Object> rowRules = null;
		Map<String, Object> colRules = null;
		String busKeyColsStr = rule.split(" ")[0];
		rule = rule.substring(busKeyColsStr.length()).trim();

		String[] busKeyCols = busKeyColsStr.split("~");
		for (String key : busKeyCols[0].split(","))
			busKeys.add(key);
		for (String key : busKeyCols[1].split(","))
			parBusKeys.add(key);

		blockRules = mapper.readValue(rule, Map.class);
		for (String blockKey : blockRules.keySet()) {
			blockRegexRule grule = new blockRegexRule();
			System.out.println("blockRule:" + blockKey);

			String[] parts = blockKey.split(",");
			grule.blockId = parts[0];
			if (parts.length > 1) {
				grule.tagPath = new String[parts.length - 1][3];
				for (int i = 1; i < parts.length; i++) {
					String[] tmp = parts[i].split("\\.");
					grule.tagPath[i - 1][0] = tmp[0].toUpperCase();// 标签大写
					grule.tagPath[i - 1][1] = tmp[1];
					grule.tagPath[i - 1][2] = tmp[2];
				}
			}
			rowRules = (Map<String, Object>) blockRules.get(blockKey);// 行规则
			grule.subRowRule = new ListRowRegexRule[rowRules.size()];
			int rowRuleIndex = -1;
			for (String rowKey : rowRules.keySet()) {
				rowRuleIndex++;
				System.err.println("rowKey:" + rowKey);// isRegexType,orgin_pos_9:next1:prev
				grule.subRowRule[rowRuleIndex] = new ListRowRegexRule();
				colRules = (Map<String, Object>) rowRules.get(rowKey);
				buildRowRule(grule.subRowRule[rowRuleIndex], rowKey);
				buildColRule(grule.subRowRule[rowRuleIndex], colRules);
			}
			gbColRule.add(grule);
		}
	}

	private void buildRowRule(ListRowRegexRule rowRule, String rowKey) {
		String[] parts = rowKey.split(",");
		rowRule.isRegexType = Boolean.parseBoolean(parts[0]);
		rowRule.subRowPri = parts[1];
		if (parts.length > 2) {
			String type = parts[2];
			if (type.startsWith("next")) {
				rowRule.directionType = 2;
				if (type.equals("next")) {
					rowRule.offsetNum = 1;
				} else {
					rowRule.offsetNum = Integer.parseInt(type.substring(4));
				}
			} else if (type.startsWith("prev")) {
				rowRule.directionType = 1;
				if (type.equals("prev")) {
					rowRule.offsetNum = 1;
				} else {
					rowRule.offsetNum = Integer.parseInt(type.substring(4));
				}
			}
		}

	}

	private void buildColRule(ListRowRegexRule rowRule, Map<String, Object> colRules) {
		rowRule.colRules = new ColRegexRule[colRules.size()];
		int n = -1;
		for (String colName : colRules.keySet()) {
			n++;
			System.err.println("colRule:" + colName);
			rowRule.colRules[n] = new ColRegexRule();
			String[] parts = colName.split("~");
			rowRule.colRules[n].colName = parts[0].trim();
			parts[1] = parts[1].trim();
			String[] domIds = parts[1].split(",");
			rowRule.colRules[n].tagPath = new String[domIds.length][3];
			for (int i = 1; i < domIds.length; i++) {
				String[] tmp = domIds[i].split("\\.");
				rowRule.colRules[n].tagPath[i - 1][0] = tmp[0].toUpperCase();// 标签大写
				rowRule.colRules[n].tagPath[i - 1][1] = tmp[1];// if == id
				// ,replace("\\{rowid\\}",rowid);
				rowRule.colRules[n].tagPath[i - 1][2] = tmp[2];
			}
			Map<String, Object> subList = (Map<String, Object>) colRules.get(colName);
			if (subList != null) {
				rowRule.colRules[n].isList = true;
				rowRule.colRules[n].listRule = new ListRowRegexRule();
				buildColRule(rowRule.colRules[n].listRule, subList);
			} else {
				if (parts.length > 2)
					rowRule.colRules[n].valueType = Integer.parseInt(parts[2].trim());
				if (parts.length > 3)
					rowRule.colRules[n].attrName = parts[3].trim();
				if (parts.length > 5) {
					rowRule.colRules[n].pattern = Pattern.compile(parts[4].trim());
					rowRule.colRules[n].subpart = parts[5].trim();
					if (parts.length > 6)
						rowRule.colRules[n].calc = Boolean.parseBoolean(parts[6].trim());
				}
			}
		}
	}

	public String toString() {
		return "ListSegmentRule[gbColRule:" + gbColRule + "]";
	}

	private void parseCol(HashMap<String, Map<String, String>> mapVal, blockRegexRule blRule, Node[] rowNode, DomParser htmlParse,
			WebPage page) {
		Map<String, String> trow = new HashMap<String, String>();
		for (int c = 0; c < blRule.subRowRule.length; c++) {
			ListRowRegexRule rRule = blRule.subRowRule[c];
			Node rNode = rowNode[c];
			for (int i = 0; i < rRule.colRules.length; i++) {
				ColRegexRule colRule = rRule.colRules[i];
				if (colRule.isList == false) {
					Node node = htmlParse.getNodeByTagPath(rNode, colRule.tagPath);
					if (node == null) {
						return;
					}
					String val = "";
					if (colRule.valueType == 1) {
						val = htmlParse.getNodeValue(node);
					} else if (colRule.valueType == 2) {
						val = htmlParse.getNodeAttrValue(node, colRule.attrName);
					}
					if (colRule.pattern != null) {
						if ("datecalc".equals(colRule.pattern.pattern())) {
							val = DateCalcUtil.dateCalc(val, colRule.subpart, page);
						} else {
							Matcher m = null;
							m = colRule.pattern.matcher(val);
							if (m == null || !m.find())
								return;
							// val = NutchConstant.ReplaceRegex(m, this.subpart);
							val = m.replaceAll(colRule.subpart);
							if (colRule.calc) {
								try {
									val = AviatorEvaluator.execute(val).toString();
								} catch (Exception e) {
									SegParserJob.LOG.error(e.getMessage());
									return;
								}
							}
						}
					}
					System.err.println(colRule.colName + "=" + val);
					trow.put(colRule.colName, val);
				}
			}
		}
		addToMap(mapVal, trow, false);
		Node[] rowListNode = rowNode;
		for (int c = 0; c < blRule.subRowRule.length; c++) {
			ListRowRegexRule rRule = blRule.subRowRule[c];
			Node rNode = rowNode[c];
			for (int i = 0; i < rRule.colRules.length; i++) {
				ColRegexRule colRule = rRule.colRules[i];
				if (colRule.isList && colRule.listRule != null) {
					Node lnode = rNode;
					Map<String, String> row = new HashMap<String, String>();
					row.putAll(trow);
					while (lnode != null) {// 循环取回复
						lnode = htmlParse.getNodeByTagPath(lnode, colRule.tagPath);
						if (lnode == null) {
							break;
						}
						ListRowRegexRule listRule = colRule.listRule;
						for (int j = 0; j < listRule.colRules.length; j++) {
							ColRegexRule _colRule = listRule.colRules[j];
							Node node = htmlParse.getNodeByTagPath(lnode, _colRule.tagPath);
							if (node == null) {
								continue;
							}
							String val = "";
							if (_colRule.valueType == 1) {
								val = htmlParse.getNodeValue(node);
							} else if (_colRule.valueType == 2) {
								val = htmlParse.getNodeAttrValue(node, _colRule.attrName);
							}
							if (_colRule.pattern != null) {
								if ("datecalc".equals(_colRule.pattern.pattern())) {
									DateCalcUtil.dateCalc(val, _colRule.subpart, page);
								} else {
									Matcher m = null;
									m = _colRule.pattern.matcher(val);
									if (m == null || !m.find())
										return;
									// val = NutchConstant.ReplaceRegex(m, this.subpart);
									val = m.replaceAll(_colRule.subpart);
									if (_colRule.calc) {
										try {
											val = AviatorEvaluator.execute(val).toString();
										} catch (Exception e) {
											SegParserJob.LOG.error(e.getMessage());
											return;
										}
									}
								}
							}
							System.err.println(_colRule.colName + "=" + val);
							row.put(_colRule.colName, val);
						}
						addToMap(mapVal, row, true);
					}
				}
			}
		}
	}

	private void addToMap(HashMap<String, Map<String, String>> mapVal, Map<String, String> row, boolean isPar) {
		String key = null;
		if (isPar) {
			for (int i = 0; i < busKeys.size(); i++) {
				String col = busKeys.get(i);
				String colVal = row.get(col);
				if (colVal == null)
					return;
				if (key == null)
					key = colVal;
				else
					key = "~" + colVal;
			}
			String subKeyId = new String(MD5Hash.digest(key).getDigest());
			key = null;
			for (int i = 0; i < parBusKeys.size(); i++) {
				String col = parBusKeys.get(i);
				String colVal = row.get(col);
				if (colVal == null)
					return;
				if (key == null)
					key = colVal;
				else
					key = "~" + colVal;
				row.remove(col);
				row.put(busKeys.get(i), colVal);// 回写主体内容字段
			}
			String keyId = new String(MD5Hash.digest(key).getDigest());
			if (!mapVal.containsKey(subKeyId)) {
				Map<String, String> subRow = new HashMap<String, String>();
				subRow.put("PSID", keyId);
				mapVal.put(subKeyId, subRow);
			} else {
				Map<String, String> subRow = mapVal.get(subKeyId);
				subRow.put("PSID", keyId);
			}
			if (!mapVal.containsKey(keyId)) {
				mapVal.put(keyId, row);
			} else {
				Map<String, String> subRow = mapVal.get(keyId);
				subRow.putAll(row);
			}
			res = true;

		} else {
			for (int i = 0; i < busKeys.size(); i++) {
				String col = busKeys.get(i);
				String colVal = row.get(col);
				if (colVal == null)
					return;
				if (key == null)
					key = colVal;
				else
					key = "~" + colVal;
			}
			String keyId = new String(MD5Hash.digest(key).getDigest());
			if (!mapVal.containsKey(keyId)) {
				mapVal.put(keyId, row);
			} else {
				Map<String, String> subRow = mapVal.get(keyId);
				subRow.putAll(row);
			}
			res = true;
		}

	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean parseSegMent(Utf8 segmentColName, String unreverseKey, WebPage page, WebPageSegment psg, ParseUtil util,
			DomParser htmlParse) throws IOException, Exception {
		res = false;
		if (psg.rootNode == null) {
			psg.rootNode = htmlParse.getDomNode(page.getContent().array(), "utf-8");
		}
		if (psg.rootNode == null)
			return false;

		Utf8 oldVal = psg.getSegMentCnt(segmentColName);
		HashMap<String, Map<String, String>> mapVal = null;
		if (oldVal == null) {
			mapVal = new HashMap<String, Map<String, String>>();
		} else {
			mapVal = (HashMap<String, Map<String, String>>) NutchConstant.deserializeObject(oldVal.toString(), true, false);
		}
		for (blockRegexRule blRule : gbColRule) {
			Node blNode = htmlParse.getNodeById(psg.rootNode, blRule.blockId);
			if (blRule.tagPath != null)
				blNode = htmlParse.getNodeByTagPath(blNode, blRule.tagPath);
			if (blNode == null)
				continue;
			List<Node[]> rows = new ArrayList<Node[]>();
			Object rowNodes[] = new Object[blRule.subRowRule.length];
			int rlen = -1;
			List<Node> _rows = null;
			for (int i = 0; i < blRule.subRowRule.length; i++) {
				boolean needPar = true;
				if (i > 0) {
					if (blRule.subRowRule[i].subRowPri.equals(blRule.subRowRule[i - 1].subRowPri))
						needPar = false;
				}
				if (needPar) {
					if (blRule.subRowRule[i].isRegexType) {
						_rows = htmlParse.getNodeListByRegexId(blNode, Pattern.compile(blRule.subRowRule[i].subRowPri));
					} else {
						_rows = htmlParse.getNodeListByIdPri(blNode, blRule.subRowRule[i].subRowPri);
					}
				}
				if (rlen == -1)
					rlen = _rows.size();
				if (_rows == null || rlen == 0 || rlen != _rows.size())
					throw new Exception("取数据错误或者记录为空");// 未捕获的错误
				rowNodes[i] = _rows;
			}
			for (int i = 0; i < rlen; i++) {// row
				Node[] nodes = new Node[blRule.subRowRule.length];
				for (int j = 0; j < rlen; j++) {// col
					List<Node> r = (List<Node>) rowNodes[j];
					nodes[j] = r.get(i);
					if (blRule.subRowRule[j].directionType == 1) {
						int num = 0;
						while (num++ < blRule.subRowRule[j].offsetNum) {
							if (nodes[j] == null)
								throw new Exception("url:" + unreverseKey + " segname:" + segmentColName + " 行：" + i + " 向前取节点失败");
							nodes[j] = nodes[j].getPreviousSibling();
						}
					} else if (blRule.subRowRule[j].directionType == 2) {
						int num = 0;
						while (num++ < blRule.subRowRule[j].offsetNum) {
							if (nodes[j] == null)
								throw new Exception("url:" + unreverseKey + " segname:" + segmentColName + " 行：" + i + " 向前取节点失败");
							nodes[j] = nodes[j].getNextSibling();
						}
					}
				}
				rows.add(nodes);
			}
			for (int i = 0; i < rlen; i++) {// row
				parseCol(mapVal, blRule, rows.get(i), htmlParse, page);
			}
		}
		psg.putToSegMentCnt(segmentColName, new Utf8(NutchConstant.serialObject(mapVal, true, false)));
		return true;
	}

}
