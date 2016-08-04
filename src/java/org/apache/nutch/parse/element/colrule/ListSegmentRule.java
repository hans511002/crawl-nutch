package org.apache.nutch.parse.element.colrule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.MD5Hash;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.element.DomParser;
import org.apache.nutch.parse.element.ExprCalcRule;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageSegment;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * 反射调用定制解析类来完成某一个字段值的解析
 * 
 * @author hans
 * 
 */
public class ListSegmentRule extends ExprCalcRule {
	private static final long serialVersionUID = -1480637835194938420L;
	List<blockRegexRule> gbColRule;
	List<String> busKeys;
	List<String> parBusKeys;
	boolean res = false;

	public static class blockRegexRule {
		public Pattern blockPattern = null;
		public String substitution = null;
		ListRowRegexRule subRowRule = null;
	}

	public static class ListRowRegexRule {
		public Pattern rowPattern = null;
		public String subStitution = null;
		boolean splitType = true;
		List<ColRegexRule> colRules = null;
	}

	public static class ColRegexRule {
		public Pattern colPattern = null;
		public boolean isList = false;
		public String[] colName = null;
		public String[] colSubStitution = null;
	}

	/**
	 * 业务主键字段名称列表~父级主键 三层MAP: KEY:块获取规则 regex~$1 ValueMap: vKey:行获取规则 regex~$1 vvMap:ckey:字段名 cn1~cn2 cVal: regex~$1~42
	 * 
	 * @param rule
	 */
	@SuppressWarnings("unchecked")
	public ListSegmentRule(String rule) throws Exception {
		gbColRule = new ArrayList<blockRegexRule>();
		ObjectMapper mapper = new ObjectMapper();
		Map<String, Object> blockRules = null;
		Map<String, Object> rowRules = null;
		Map<String, Object> colRule = null;
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
			System.err.println("blockRule:" + blockKey);
			String[] pats = blockKey.split("~");
			System.err.println("block Pattern:" + pats[0] + "  value:" + pats[1]);
			try {
				grule.blockPattern = Pattern.compile(pats[0]);
			} catch (PatternSyntaxException ex) {
				ex.printStackTrace();
				continue;
			}
			grule.substitution = pats[1];
			grule.subRowRule = new ListRowRegexRule();
			rowRules = (Map<String, Object>) blockRules.get(blockKey);// 行规则
			for (String rowKey : rowRules.keySet()) {
				System.err.println("rowKey:" + rowKey);
				pats = rowKey.split("~");
				try {
					if (pats[0].startsWith("regex:")) {
						grule.subRowRule.rowPattern = Pattern.compile(pats[0].substring(6));
						grule.subRowRule.splitType = false;
						grule.subRowRule.subStitution = pats[1];
					} else if (pats[0].startsWith("split:")) {
						grule.subRowRule.subStitution = pats[0].substring(6);
						grule.subRowRule.splitType = true;
					} else {
						grule.subRowRule.rowPattern = Pattern.compile(pats[0]);
						grule.subRowRule.splitType = false;
						grule.subRowRule.subStitution = pats[1];
					}
				} catch (PatternSyntaxException ex) {
					ex.printStackTrace();
					grule.subRowRule = null;
					continue;
				}
				grule.subRowRule.colRules = new ArrayList<ColRegexRule>();
				colRule = (Map<String, Object>) rowRules.get(rowKey);
				for (String colName : colRule.keySet()) {
					System.err.println("colName:" + colName + " regex:" + colRule.get(colName));
					String[] colPats = colName.split("~");
					ColRegexRule col = new ColRegexRule();
					if (colPats[0].equals("list:")) {
						col.isList = true;
					}
					pats = colRule.get(colName).toString().split("~");
					if ((col.isList == false && colPats.length != pats.length - 1) || (col.isList && colPats.length != pats.length)) {
						System.err.println("列与规则对应不上：" + colName + " " + colRule);
						continue;
					}
					try {
						col.colPattern = Pattern.compile(pats[0]);
					} catch (PatternSyntaxException ex) {
						ex.printStackTrace();
						int i = 0;
						if (col.isList)
							i++;
						for (; i < colPats.length; i++) {
							if (busKeys.contains(colPats[i]) || parBusKeys.contains(colPats[i])) {
								grule.subRowRule.colRules.clear();
								System.err.println("业务主键获取规则错误");
								break;
							}
						}
						continue;
					}
					col.colSubStitution = new String[colPats.length - 1];
					if (col.isList) {
						col.colName = new String[colPats.length - 1];
						for (int j = 1; j < colPats.length; j++) {
							col.colName[j - 1] = colPats[j];
							col.colSubStitution[j - 1] = pats[j];
						}
					} else {
						col.colName = colPats;
						for (int j = 1; j < colPats.length; j++) {
							col.colSubStitution[j - 1] = pats[j];
						}
					}
					grule.subRowRule.colRules.add(col);
				}
			}
			if (grule.subRowRule != null && grule.subRowRule.colRules.size() > 0)
				gbColRule.add(grule);
		}
	}

	public String toString() {
		return "ListSegmentRule[gbColRule:" + gbColRule + "]";
	}

	private void parseCol(HashMap<String, Map<String, String>> mapVal, blockRegexRule blRule, String rowText) {

		if (rowText == null || rowText.equals(""))
			return;
		Map<String, String> trow = new HashMap<String, String>();
		for (ColRegexRule colRule : blRule.subRowRule.colRules) {
			// for (String colName : colRule.keySet()) {
			System.err.println("colName:" + Arrays.toString(colRule.colName) + " regex:" + colRule.colPattern.pattern());
			if (colRule.isList == false) {// 循环取回复
				Matcher cm = colRule.colPattern.matcher(rowText);
				if (cm.find() == false)
					continue;
				for (int i = 0; i < colRule.colName.length; i++) {
					// String colText = cm.replaceAll(colRule.colSubStitution[i]);
					String colText = NutchConstant.ReplaceRegex(cm, colRule.colSubStitution[i]);
					System.err.println(colRule.colName[i] + "=" + colText);
					if (colText != null)
						trow.put(colRule.colName[i], colText);
				}
			}
		}
		addToMap(mapVal, trow, false);
		for (ColRegexRule colRule : blRule.subRowRule.colRules) {
			if (colRule.isList) {
				Matcher cm = colRule.colPattern.matcher(rowText);
				String conStr;
				while (cm.find()) {
					conStr = rowText.substring(0, cm.start(1));
					Map<String, String> row = new HashMap<String, String>();
					for (int i = 0; i < colRule.colName.length; i++) {
						// String colText = cm.replaceAll(colRule.colSubStitution[i]);
						String colText = NutchConstant.ReplaceRegex(cm, colRule.colSubStitution[i]);
						System.err.println(colRule.colName[i] + "=" + colText);
						row.put(colRule.colName[i], colText);
					}
					row.putAll(trow);
					addToMap(mapVal, row, true);
					cm = colRule.colPattern.matcher(conStr);
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
			DomParser htmlParse) throws Exception {
		res = false;
		if (psg.content == null) {
			psg.content = new String(page.getContent().array(), "utf-8");
		}
		if (psg.content == null || psg.content.equals(""))
			return false;
		Utf8 oldVal = psg.getSegMentCnt(segmentColName);
		HashMap<String, Map<String, String>> mapVal = null;
		if (oldVal == null) {
			mapVal = new HashMap<String, Map<String, String>>();
		} else {
			mapVal = (HashMap<String, Map<String, String>>) NutchConstant.deserializeObject(oldVal.toString(), true, false);
		}
		for (blockRegexRule blRule : gbColRule) {
			if (blRule.subRowRule == null || blRule.subRowRule.colRules == null || blRule.subRowRule.colRules.size() == 0)
				continue;
			Matcher bm = blRule.blockPattern.matcher(psg.content);
			if (bm.find() == false)
				continue;
			// String blockText = bm.replaceAll(blRule.substitution);
			String blockText = NutchConstant.ReplaceRegex(bm, blRule.substitution);

			if (blockText == null || blockText.equals(""))
				continue;
			if (blRule.subRowRule.splitType) {
				String[] rowList = blockText.split(blRule.subRowRule.subStitution);
				System.err.println("split rowList length:" + rowList.length);
				for (String rowText : rowList) {
					System.err.println("rowList value:" + rowText);
					parseCol(mapVal, blRule, rowText);
				}
			} else {
				Matcher rm = blRule.subRowRule.rowPattern.matcher(blockText);
				while (rm.find()) {
					blockText = blockText.substring(rm.end(1));
					String rowText = NutchConstant.ReplaceRegex(rm, blRule.subRowRule.subStitution);

					// String rowText = rm.replaceAll(blRule.subRowRule.subStitution);
					rm = blRule.subRowRule.rowPattern.matcher(blockText);
					System.err.println("rowText:" + rowText);
					parseCol(mapVal, blRule, rowText);
				}
			}
		}
		psg.putToSegMentCnt(segmentColName, new Utf8(NutchConstant.serialObject(mapVal, true, false)));
		return res;
	}

}
