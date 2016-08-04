package com.crawl.marklines;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.sobey.jcg.support.utils.Convert;

public class TableParseUtil {
	static Map<String, Pattern> cachePattern = new HashMap<String, Pattern>();
	static boolean tableContentType = false;// true list, false map

	public static List<ArrayList<String>> parseTableToArray(String cnt, boolean delTdTag) {
		ArrayList<ArrayList<String>> list = new ArrayList<ArrayList<String>>();
		String trowRule = "(?ism)<tr.*?>(.*?)</tr>";
		Pattern trow = Pattern.compile(trowRule);
		Matcher m = trow.matcher(cnt);
		String tdRule = "(?ism)(<th.*?>(.*?)</th>)|(<td.*?>(.*?)</td>)";
		String colRule = "(?ism)(rowspan=[\"']?(\\d+)[\"']?)|(colspan=[\"']?(\\d+)[\"']?)";
		Pattern td = Pattern.compile(tdRule);
		Pattern col = Pattern.compile(colRule);
		int maxCols = 0;
		int[] rowpan = new int[256];
		int rowLen = 0;
		while (m.find()) {
			ArrayList<String> row = null;
			if (list.size() > rowLen) {
				row = list.get(rowLen);
			} else {
				row = new ArrayList<String>();
				list.add(row);
			}
			String trRow = m.group(1);
			Matcher tdm = td.matcher(trRow);
			int thisCol = 0;
			while (tdm.find()) {
				String tdStr = Convert.toString(tdm.group(2)) + Convert.toString(tdm.group(4));
				int rowSize = 1;
				int colSize = 1;

				if (tdm.group(1) != null) {
					Matcher cm = col.matcher(tdm.group(1));
					if (cm.find()) {
						rowSize = Convert.toInt(cm.group(2), rowSize);
						colSize = Convert.toInt(cm.group(4), colSize);
					}
				} else {
					Matcher cm = col.matcher(tdm.group(3));
					if (cm.find()) {
						rowSize = Convert.toInt(cm.group(2), rowSize);
						colSize = Convert.toInt(cm.group(4), colSize);
					}
				}
				for (int i = thisCol; i < rowpan.length; i++) {
					if (rowpan[i] > 1 && thisCol <= i) {
						thisCol = i + 1;
						rowpan[i] = rowpan[i] - 1;
					} else {
						break;
					}
				}
				if (rowSize > 1) {
					rowpan[thisCol] = rowSize;
				}

				if (delTdTag) {
					String tagRule = "(?ism)</?\\w+.*?/?>";
					tdStr = tdStr.replaceAll(tagRule, "");
				}
				tdStr = tdStr.replaceAll("\\n", " ").trim().replaceAll("  ", " ").replaceAll("\t", " ");
				for (int c = 0; c < colSize; c++) {
					if (row.size() > thisCol + c) {
						row.set(thisCol + c, tdStr);
					} else {
						row.add(tdStr);
					}
				}
				if (rowSize > 1) {
					for (int i = 1; i < rowSize; i++) {
						int rowIndex = rowLen + i;
						ArrayList<String> nrow = null;
						if (list.size() > rowIndex) {
							nrow = list.get(rowIndex);
						} else {
							nrow = new ArrayList<String>();
							list.add(nrow);
						}
						while (nrow.size() < thisCol) {
							nrow.add("");
						}
						for (int c = 0; c < colSize; c++) {
							if (nrow.size() > thisCol + c) {
								nrow.set(thisCol + c, tdStr);
							} else {
								nrow.add(tdStr);
							}
						}
					}
				}
				thisCol += colSize;
			}
			if (maxCols == 0) {
				maxCols = thisCol;
			}
			rowLen++;

		}
		return list;
	}

	public static void addTable(Map<String, Object> doc, String mongoKey, String title, String thisPart) {
		String tableContent = mongoKey + ".TableContent";
		com.mongodb.BasicDBList dbTable = new BasicDBList();
		List<ArrayList<String>> rows = TableParseUtil.parseTableToArray(thisPart, true);
		String tableType = mongoKey + ".Type";
		doc.put(tableType, "Table");
		for (ArrayList<String> row : rows) {
			com.mongodb.BasicDBObject dbrow = new BasicDBObject();
			com.mongodb.BasicDBList dblist = new BasicDBList();
			if (tableContentType) {
				for (String col : row) {
					dblist.add(col);
				}
			} else {
				for (int i = 0; i < row.size(); i++) {
					dbrow.put("value" + (i + 1), row.get(i));
				}
			}
			if (tableContentType) {
				dbTable.add(dblist);
			} else {
				dbTable.add(dbrow);
			}
		}
		if (title != null && !title.equals("")) {
			doc.put(mongoKey + ".title", title);
		}
		doc.put(tableContent, dbTable);
	}

	public static String getUnitTitle(String str, String defaults) {
		String itemRule = "\\d{4}年";
		Pattern item = cachePattern.get(itemRule);
		if (item == null) {
			item = Pattern.compile(itemRule);
			cachePattern.put(itemRule, item);
		}
		Matcher _m = item.matcher(str);
		if (_m.find()) {
			return "Date";
		}
		return defaults;
	}

	public static int parseTable(Map<String, Object> doc, String mongoKey, String title, String type, String thisPart) {
		String tableContent = mongoKey + ".TableContent";
		String tableType = mongoKey + ".Type";
		String itemRule = "<td.*?(class=\"right\")|(align=\"right\").*?>(.*?)</td>";
		Pattern item = cachePattern.get(itemRule);
		if (item == null) {
			item = Pattern.compile(itemRule);
			cachePattern.put(itemRule, item);
		}
		Matcher _m = item.matcher(thisPart);

		itemRule = "(?ism)<table.*?>(.*?)</table>";
		item = cachePattern.get(itemRule);
		if (item == null) {
			item = Pattern.compile(itemRule);
			cachePattern.put(itemRule, item);
		}
		Matcher tm = item.matcher(thisPart);
		if (tm.find()) {
			if (_m.find() && type != null && !type.equals("") && _m.end() < tm.start()) {
				doc.put(mongoKey + "." + getUnitTitle(_m.group(3), type), _m.group(3));
			}
			doc.put(tableType, "Table");
			com.mongodb.BasicDBList dbTable = new BasicDBList();
			List<ArrayList<String>> rows = TableParseUtil.parseTableToArray(tm.group(1), true);
			for (ArrayList<String> row : rows) {
				com.mongodb.BasicDBObject dbrow = new BasicDBObject();
				com.mongodb.BasicDBList dblist = new BasicDBList();
				if (tableContentType) {
					for (String col : row) {
						dblist.add(col);
					}
				} else {
					for (int i = 0; i < row.size(); i++) {
						dbrow.put("value" + (i + 1), row.get(i));
					}
				}
				if (tableContentType) {
					dbTable.add(dblist);
				} else {
					dbTable.add(dbrow);
				}
			}
			if (title != null && !title.equals("")) {
				doc.put(mongoKey + ".title", title);
			}
			doc.put(tableContent, dbTable);
			return tm.end();
		} else {
			return 0;
		}
	}

	public static void parseTable(Map<String, Object> doc, String mongoKey, String title, String descType, String type,
			String thisPart) {
		String tableContent = mongoKey + ".TableContent";
		String tableType = mongoKey + ".Type";
		String tableTitle = mongoKey + "." + descType;
		String tableUnit = mongoKey + "." + type;
		String busRule = "(?ism)<table.*?>(.*?)</table>";
		Pattern item = cachePattern.get(busRule);
		if (item == null) {
			item = Pattern.compile(busRule);
			cachePattern.put(busRule, item);
		}
		Matcher m = item.matcher(thisPart);
		String table1 = null;
		String table2 = null;
		if (m.find()) {
			table1 = m.group(1);
			if (m.find())
				table2 = m.group(1);
		}
		String descStr = null;
		String tableStr = null;
		if (table2 != null) {
			descStr = table1;
			tableStr = table2;
		} else if (table1 != null) {
			tableStr = table1;
		}
		if (descStr != null) {
			List<ArrayList<String>> rows = TableParseUtil.parseTableToArray(descStr, true);
			if (rows.size() > 0) {
				ArrayList<String> head = rows.get(0);
				if (head.size() > 0 && descType != null && !descType.equals("")) {
					doc.put(tableTitle, head.get(0));
				}
				if (head.size() > 1) {
					doc.put(tableUnit, head.get(1));
				}
			}
		}
		if (tableStr != null) {
			doc.put(tableType, "Table");
			com.mongodb.BasicDBList dbTable = new BasicDBList();
			List<ArrayList<String>> rows = TableParseUtil.parseTableToArray(tableStr, true);
			for (ArrayList<String> row : rows) {
				com.mongodb.BasicDBObject dbrow = new BasicDBObject();
				com.mongodb.BasicDBList dblist = new BasicDBList();
				if (tableContentType) {
					for (String col : row) {
						dblist.add(col);
					}
				} else {
					for (int i = 0; i < row.size(); i++) {
						dbrow.put("value" + (i + 1), row.get(i));
					}
				}
				if (tableContentType) {
					dbTable.add(dblist);
				} else {
					dbTable.add(dbrow);
				}
			}
			if (title != null && !title.equals("")) {
				doc.put(mongoKey + ".title", title);
			}
			doc.put(tableContent, dbTable);
		}
	}

}
