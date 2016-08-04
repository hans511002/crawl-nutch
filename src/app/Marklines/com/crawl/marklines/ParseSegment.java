package com.crawl.marklines;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.util.Utf8;
import org.apache.gora.mongo.store.MongoStore;
import org.apache.gora.mongo.store.MongoUtil;
import org.apache.gora.query.Query;
import org.apache.gora.query.Result;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.CookieStore;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.nutch.crawl.FetchScheduleFactory;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.ParserJob;
import org.apache.nutch.parse.element.SegMentParsers;
import org.apache.nutch.protocol.ProtocolFactory;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.urlfilter.SubURLFilters;
import org.apache.nutch.urlfilter.UrlPathMatch;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TableUtil;

import com.crawl.App;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.sobey.jcg.support.log4j.LogUtils;

public class ParseSegment extends App {

	public void setCookies(DefaultHttpClient httpClient) {
		CookieStore cookieStore = httpClient.getCookieStore();
		cookieStore.clear();
		BasicClientCookie cook = new BasicClientCookie(
				"PLATFORM_SESSION",
				"f97eb86cfff075032f9dcf12e224869d42b65b4c-csrfToken=be8ae01d0c837b427d5a51418c4f03f867502233-1464318293929-0a21582adebfff7361de455e&_lsi=1499582&_lsh=b03c7c0908b4b41cadfaa3e6250d08341a8cb102");
		cookieStore.addCookie(cook);
		cook.setDomain("www.marklines.com");
		cook.setPath("/");
		// cook.setExpiryDate(new Date(2019, 5, 30, 8, 17, 11));
		cook = new BasicClientCookie("PLAY_LANG", "cn");
		cook.setDomain("www.marklines.com");
		cook.setPath("/");
		cookieStore.addCookie(cook);
		httpClient.setCookieStore(cookieStore);
	}

	static String destTableName;

	public static void parse(DataStore<String, WebPage> store, String key, WebPage page, int type) {
		if (page.getContent() == null) {
			LogUtils.error("=========content=null====================url=" + page.getBaseUrl());
			page.setText(new Utf8("null"));
			store.put(key, page);
			return;
		}
		if (type == 0) {// prod
			parseProd(store, key, page);
		} else if (type == 1) {// sale
			parseSale(store, key, page);
		} else if (type == 2) {// engine
			parseEngine(store, key, page);
		} else if (type == 3) {// hol
			parseHolcar(store, key, page);
		} else if (type == 4) {// part
			parsePart(store, key, page);
			// https://www.marklines.com/cn/top500/s500_278
		}
	}

	public static Map<String, MongoUtil> mongoCache = new HashMap<String, MongoUtil>();

	public static MongoUtil getMongo(String tb) {
		MongoUtil util = mongoCache.get(tb);
		if (util == null) {
			util = new MongoUtil("localhost", 27017, "testdb", tb);
			mongoCache.put(tb, util);
		}
		return util;
	}

	static String getCnt(WebPage page) {
		String cnt = new String(page.getContent().array());
		if (cnt.length() < 100) {
			LogUtils.error("==========content small===============url=" + page.getBaseUrl() + "=================");
			return null;
		}
		return cnt;
	}

	static Map<String, Pattern> cachePattern = TableParseUtil.cachePattern;
	static boolean tableContentType = TableParseUtil.tableContentType;// true list, false map

	public static void parseProd(DataStore<String, WebPage> store, String key, WebPage page) {
		String cnt = getCnt(page);
		MongoUtil mongo = getMongo(destTableName);
		Map<String, Object> term = new HashMap<String, Object>();
		term.put("DetailedInformation.SourceURL.PageURL", page.getBaseUrl().toString());
		Map<String, Object> doc = new HashMap<String, Object>();
		doc.putAll(term);
		int index = cnt.indexOf("<div class=\"box-header\">");
		int end = cnt.indexOf("<div id=\"side_menu\" class=\"frame_left\">");
		if (index == -1) {
			LogUtils.error("==========content not search <div class=\"box-header\"> ===============url="
					+ page.getBaseUrl() + "=================");
			page.setText(new Utf8("notmatch"));
			store.put(key, page);
			return;
		}
		if (end < 10)
			end = cnt.length();
		cnt = cnt.substring(index, end);
		int headend = cnt.indexOf("<div class=\"box-content \">");
		if (headend < 10) {
			LogUtils.error("==========content not search <div class=\"box-content \">===============url="
					+ page.getBaseUrl() + "=================");
			page.setText(new Utf8("notmatch"));
			store.put(key, page);
			return;
		}
		String header = cnt.substring(0, headend);
		String titleRule = "<h1><span>(.*)</span></h1>";
		Pattern head = cachePattern.get(titleRule);
		if (head == null) {
			head = Pattern.compile(titleRule);
			cachePattern.put(titleRule, head);
		}
		String prodRootKey = "KeyAttributes.Scale.Output.";
		doc.put(prodRootKey + "Type", "Table");
		Matcher m = head.matcher(header);
		if (m.find()) {
			doc.put(prodRootKey + "title", m.group(1));
		}
		String prodTableKey = prodRootKey + "TableContent";
		BasicDBList tableRows = new BasicDBList();

		String tbHeadRule = "(?ism)<table .*?>(.*)</table>";
		Pattern tbRow = cachePattern.get(tbHeadRule);
		if (tbRow == null) {
			tbRow = Pattern.compile(tbHeadRule);
			cachePattern.put(tbHeadRule, tbRow);
		}
		m = tbRow.matcher(cnt.substring(headend));
		if (m.find()) {
			cnt = m.group(1);
			com.mongodb.BasicDBList dbTable = new BasicDBList();
			List<ArrayList<String>> rows = TableParseUtil.parseTableToArray(cnt, true);
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
			doc.put(prodTableKey, dbTable);
		}
		mongo.put(term, doc);

	}

	public static void parseSale(DataStore<String, WebPage> store, String key, WebPage page) {
		String cnt = getCnt(page);
		MongoUtil mongo = getMongo(destTableName);
		Map<String, Object> term = new HashMap<String, Object>();
		term.put("DetailedInformation.SourceURL.PageURL", page.getBaseUrl().toString());
		Map<String, Object> doc = new HashMap<String, Object>();
		doc.putAll(term);
		int index = cnt.indexOf("<div class=\"box-header\">");
		int end = cnt.indexOf("<div id=\"side_menu\" class=\"frame_left\">");
		if (index == -1) {
			LogUtils.error("==========content not search <div class=\"box-header\"> ===============url="
					+ page.getBaseUrl() + "=================");
			page.setText(new Utf8("notmatch"));
			store.put(key, page);
			return;
		}
		if (end < 10)
			end = cnt.length();
		cnt = cnt.substring(index, end);
		int headend = cnt.indexOf("<div class=\"box-content \">");
		if (headend < 10) {
			LogUtils.error("==========content not search <div class=\"box-content \">===============url="
					+ page.getBaseUrl() + "=================");
			page.setText(new Utf8("notmatch"));
			store.put(key, page);
			return;
		}
		String header = cnt.substring(0, headend);
		String titleRule = "<h1><span>(.*)</span></h1>";
		Pattern head = cachePattern.get(titleRule);
		if (head == null) {
			head = Pattern.compile(titleRule);
			cachePattern.put(titleRule, head);
		}
		// String prodRootKey = "KeyAttributes.Scale.ScaleTable.";
		String prodRootKey = "DetailedInformation.Sales.SalesTable.";
		doc.put(prodRootKey + "Type", "Table");
		Matcher m = head.matcher(header);
		if (m.find()) {
			doc.put(prodRootKey + "title", m.group(1));
		}
		String prodTableKey = prodRootKey + "TableContent";
		String tbHeadRule = "(?ism)<table .*?>(.*)</table>";
		Pattern tbRow = cachePattern.get(tbHeadRule);
		if (tbRow == null) {
			tbRow = Pattern.compile(tbHeadRule);
			cachePattern.put(tbHeadRule, tbRow);
		}
		m = tbRow.matcher(cnt.substring(headend));
		if (m.find()) {
			cnt = m.group(1);
			com.mongodb.BasicDBList dbTable = new BasicDBList();
			List<ArrayList<String>> rows = TableParseUtil.parseTableToArray(cnt, true);
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
			doc.put(prodTableKey, dbTable);
		}
		mongo.put(term, doc);
	}

	public static void parseEngine(DataStore<String, WebPage> store, String key, WebPage page) {
		String cnt = getCnt(page);
		MongoUtil mongo = getMongo(destTableName);
		Map<String, Object> term = new HashMap<String, Object>();
		term.put("DetailedInformation.SourceURL.PageURL", page.getBaseUrl().toString());
		Map<String, Object> doc = new HashMap<String, Object>();
		doc.putAll(term);
		int index = cnt.indexOf("<div class=\"frame_center\">");
		int end = cnt.indexOf("<div id=\"side_menu\" class=\"frame_left\">");
		if (index == -1) {
			LogUtils.error("==========content not search <div class=\"box-header\"> ===============url="
					+ page.getBaseUrl() + "=================");
			page.setText(new Utf8("notmatch"));
			store.put(key, page);
			return;
		}
		if (end < 10)
			end = cnt.length();
		if (page.getBaseUrl().toString()
				.equals("https://www.marklines.com/cn/statistics/engineproduction/engine_prod/italy_2015")) {
			System.out.println("url=" + page.getBaseUrl());
		}
		cnt = cnt.substring(index, end);
		int headend = cnt.indexOf("<div class=\"engine_prod\">");
		headend = cnt.indexOf("<table", headend);
		String header = cnt.substring(0, headend);
		String titleRule = "<h1>(.*)</h1>";
		Pattern head = cachePattern.get(titleRule);
		if (head == null) {
			head = Pattern.compile(titleRule);
			cachePattern.put(titleRule, head);
		}
		Matcher m = head.matcher(header);
		String title = "";
		if (m.find()) {
			title = m.group(1);
		}
		doc.put("KeyAttributes.Scale.Output", parseObject(title, cnt.substring(headend)));

		// String prodRootKey = "KeyAttributes.Scale.Output.";
		// doc.put(prodRootKey + "Type", "Table");
		// Matcher m = head.matcher(header);
		// if (m.find()) {
		// doc.put(prodRootKey + "title", m.group(1));
		// }
		// String prodTableKey = prodRootKey + "TableContent";
		// String tbHeadRule = "(?ism)<table .*?>(.*)</table>";
		// Pattern tbRow = cachePattern.get(tbHeadRule);
		// if (tbRow == null) {
		// tbRow = Pattern.compile(tbHeadRule);
		// cachePattern.put(tbHeadRule, tbRow);
		// }
		// m = tbRow.matcher(cnt.substring(headend));
		// if (m.find()) {
		// cnt = m.group(1);
		// com.mongodb.BasicDBList dbTable = new BasicDBList();
		// List<ArrayList<String>> rows = TableParseUtil.parseTableToArray(cnt, true);
		// for (ArrayList<String> row : rows) {
		// com.mongodb.BasicDBObject dbrow = new BasicDBObject();
		// com.mongodb.BasicDBList dblist = new BasicDBList();
		// if (tableContentType) {
		// for (String col : row) {
		// dblist.add(col);
		// }
		// } else {
		// for (int i = 0; i < row.size(); i++) {
		// dbrow.put("value" + (i + 1), row.get(i));
		// }
		// }
		// if (tableContentType) {
		// dbTable.add(dblist);
		// } else {
		// dbTable.add(dbrow);
		// }
		// }
		// doc.put(prodTableKey, dbTable);
		// }
		mongo.put(term, doc);
		page.setText(new Utf8("parsed"));
		store.put(key, page);
	}

	public static void parseHolcar(DataStore<String, WebPage> store, String key, WebPage page) {
		String cnt = getCnt(page);
		MongoUtil mongo = getMongo(destTableName);
		Map<String, Object> term = new HashMap<String, Object>();
		if (page.getBaseUrl() == null) {
			page.setBaseUrl(new Utf8(TableUtil.unreverseUrl(key)));
		}
		term.put("DetailedInformation.SourceURL.PageURL", page.getBaseUrl().toString());
		Map<String, Object> doc = new HashMap<String, Object>();
		int index = cnt.indexOf("<div id=\"factory_outline\" class=\"col-md-12\">");
		int end = cnt.indexOf("<div id=\"side_menu\" class=\"frame_left\">");
		if (index == -1) {
			LogUtils.error("==========content not search <div class=\"box-header\"> ===============url="
					+ page.getBaseUrl() + "=================");
			page.setText(new Utf8("notmatch"));
			store.put(key, page);
			return;
		}
		if (end < 10)
			end = cnt.length();
		cnt = cnt.substring(index, end);
		int headend = cnt.indexOf("<div data-app=\"openableBox\" class=\"box sub-box\">");
		if (headend < 10) {
			headend = cnt.indexOf("<div class=\"box-content\">");
			LogUtils.warn("==========content not search <div class=\"box-content\">===============url="
					+ page.getBaseUrl() + "=================");
			if (headend < 10) {
				headend = cnt.length();
			}
		}
		String header = cnt.substring(0, headend);
		// <div class="box-header">
		// <h2> <img src='/statics/national_flag/CHN?isSmall=false' alt='image'> <span>[中国]</span></h2>
		// </div>
		String titleRule = "<h2>(.*?)(<img src=.*?>.*?)?(<span>(.*)</span>)?</h2>";
		Pattern head = cachePattern.get(titleRule);
		if (head == null) {
			head = Pattern.compile(titleRule);
			cachePattern.put(titleRule, head);
		}
		String mongoKey = "KeyAttributes.Location.OriginalLocation.Country";
		String companyKey = "CompanyName.Name.NameEng";
		Matcher m = head.matcher(header);
		if (m.find()) {
			doc.put(companyKey, m.group(1));
			doc.put(mongoKey, m.group(4));
		}

		int par1StartIndex = cnt.indexOf("<div data-app=\"openableBox\" class=\"box sub-box\">");
		if (par1StartIndex == -1) {
			page.setText(new Utf8("notmatch"));
			store.put(key, page);
			return;
		}
		int par1EndIndex = cnt.indexOf("<div data-app=\"openableBox\" class=\"box sub-box\">", par1StartIndex + 50);
		if (par1EndIndex == -1) {
			par1EndIndex = cnt.indexOf("<div id=\"trend\" data-app=\"openableBox\" class=\"box sub-box\">",
					par1StartIndex + 50);
			if (par1EndIndex == -1) {
				par1EndIndex = cnt.length();
			}
		}

		String part1 = cnt.substring(par1StartIndex, par1EndIndex);
		int part2StartIndex = par1EndIndex;
		if (part1.indexOf("<h1><span>位置") >= 0) {//
			int sindex = part1.indexOf("<div class=\"col-md-6\">");
			int endIndex = part1.indexOf("<div class=\"col-md-6\">", sindex + 22);
			String pt1 = part1.substring(sindex + 22, endIndex);
			// /////////////////////////////基础数据/////////////////////////////
			String itemTitle = "(?ism)<h4>(.*?)</h4>";
			head = cachePattern.get(itemTitle);
			if (head == null) {
				head = Pattern.compile(itemTitle);
				cachePattern.put(itemTitle, head);
			}
			Matcher pm = head.matcher(pt1);
			List<String> itemTypes = new ArrayList<String>();
			List<Integer> itemTypeIndex = new ArrayList<Integer>();
			itemTypes.add("");
			while (pm.find()) {
				itemTypes.add(pm.group(1).trim().replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\?|\\.|。|・", ""));
				itemTypeIndex.add(pm.end());
			}
			itemTypeIndex.add(pt1.length());
			String itemRule = "(?ism)<div class=\"profile-area\">(.*?)</div>";
			Pattern item = cachePattern.get(itemRule);
			if (item == null) {
				item = Pattern.compile(itemRule);
				cachePattern.put(itemRule, item);
			}
			for (int i = 1; i < itemTypeIndex.size(); i++) {
				pm = item.matcher(pt1.substring(itemTypeIndex.get(i - 1), itemTypeIndex.get(i)));
				if (itemTypes.get(i).startsWith("所在地")) {// i==1
					if (pm.find()) {
						mongoKey = "KeyAttributes.Location.OriginalLocation.Address";
						doc.put(mongoKey, pm.group(1).replaceAll("(?ism)</?\\w+.*?/?>", "").trim());
					}
				} else if (itemTypes.get(i).startsWith("电话")) {// i==2
					if (pm.find()) {
						mongoKey = "BasicAttributes.Contact.Telephone";
						doc.put(mongoKey, pm.group(1).replaceAll("(?ism)</?\\w+.*?/?>", "").trim());
					}
				} else if (itemTypes.get(i).startsWith("传真")) {// i==3
					if (pm.find()) {
						mongoKey = "BasicAttributes.Contact.Fax";
						doc.put(mongoKey, pm.group(1).replaceAll("(?ism)</?\\w+.*?/?>", "").trim());
					}
				} else if (itemTypes.get(i).startsWith("网址")) {// i==4
					mongoKey = "BasicAttributes.Website";// list
					BasicDBList dblist = new BasicDBList();
					while (pm.find()) {
						dblist.add(pm.group(1).replaceAll("(?ism)</?\\w+.*?/?>", "").trim());
					}
					doc.put(mongoKey, dblist);
				} else if (itemTypes.get(i).startsWith("资本关系")) {// i==5
					if (pm.find()) {
						mongoKey = "DetailedInformation.RelatedCompany.Parents";
						doc.put(mongoKey, pm.group(1).replaceAll("(?ism)</?\\w+.*?/?>", "").trim());
					}
				}
			}
			// /////////////////////////////地图相关,不处理/////////////////////////////
			sindex = endIndex;
			endIndex = part1.indexOf("<div class=\"col-md-12\">", sindex + 22);
			// pt1 = part1.substring(sindex + 22, endIndex);
			// /////////////////////////////////
			sindex = endIndex;
			pt1 = part1.substring(sindex);
			// /////////////////////////////基础数据/////////////////////////////
			// itemTitle = "(?ism)<h4>(.*?)</h4>";
			head = cachePattern.get(itemTitle);
			pm = head.matcher(pt1);
			itemTypes.clear();
			itemTypeIndex.clear();
			itemTypes.add("");
			while (pm.find()) {
				itemTypes.add(pm.group(1).trim());
				itemTypeIndex.add(pm.end());
			}
			itemTypeIndex.add(pt1.length());
			// itemRule = "<dev class=\"profile-area\">(.*?)</div>";
			item = cachePattern.get(itemRule);
			for (int i = 1; i < itemTypeIndex.size(); i++) {
				pm = item.matcher(pt1.substring(itemTypeIndex.get(i - 1), itemTypeIndex.get(i)));
				if (itemTypes.get(i).startsWith("生产车型")) {// i==1
					if (pm.find()) {
						mongoKey = "BasicAttributes.Products.ProductName";// list
						BasicDBList dblist = new BasicDBList();
						String autoModel = pm.group(1);
						String autoModels[] = autoModel.split("<br ?/?>");
						if (autoModels.length == 1) {
							autoModels = autoModels[0].split(",");
						}
						for (int j = 0; j < autoModels.length; j++) {
							String val = autoModels[j].replaceAll("　", "").replaceAll("\\n", " ").trim();
							if (val.endsWith("：") || val.endsWith(":")) {
								if (j < autoModels.length - 1) {
									j++;
									val += autoModels[j];
								}
							}
							dblist.add(val);
						}
						doc.put(mongoKey, dblist);
					}
				} else if (itemTypes.get(i).startsWith("产能")) {// i==2
					if (pm.find()) {
						mongoKey = "KeyAttributes.Scale.Capacity";
						doc.put(mongoKey, pm.group(1).replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ")
								.trim());
					}
				} else if (itemTypes.get(i).startsWith("总产量")) {// i==3
					if (pm.find()) {
						mongoKey = "KeyAttributes.Scale.TotalOutput";
						doc.put(mongoKey, pm.group(1).replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ")
								.trim());
					}
				}
			}
		}

		int part2EndIndex = part2StartIndex;
		part2StartIndex = cnt.indexOf("<div data-app=\"openableBox\" class=\"box sub-box\">", part2StartIndex);
		if (part2StartIndex != -1) {
			part2EndIndex = cnt.indexOf("<div id=\"trend\" data-app=\"openableBox\" class=\"box sub-box\">",
					part2StartIndex);
			if (part2EndIndex == -1) {
				part2EndIndex = cnt.length();
			}
			String part2 = cnt.substring(part2StartIndex, part2EndIndex);
			int AdditionalInformationIndex = part2.indexOf("</table>");
			if (AdditionalInformationIndex == -1) {
				AdditionalInformationIndex = part2.indexOf("<div class=\"box-content \">");
			}
			String AdditionalRule = "(?ism).*>(.*?\\d{4}年\\d{1,2}月.*?)</div>";
			Pattern additional = cachePattern.get(AdditionalRule);
			if (additional == null) {
				additional = Pattern.compile(AdditionalRule);
				cachePattern.put(AdditionalRule, additional);
			}
			m = additional.matcher(part2.substring(AdditionalInformationIndex));
			if (m.find()) {
				mongoKey = "DetailedInformation.OtherInformation.AdditionalInformation";
				doc.put(mongoKey, m.group(1).replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\?|\\.|。|・", ""));
			}

			String tbHeadRule = "(?ism)<table .*?>(.*)</table>";
			Pattern tbRow = cachePattern.get(tbHeadRule);
			if (tbRow == null) {
				tbRow = Pattern.compile(tbHeadRule);
				cachePattern.put(tbHeadRule, tbRow);
			}
			m = tbRow.matcher(part2);
			if (m.find()) {
				mongoKey = "DetailedInformation.OtherInformation.CapacityTable";
				String capacityTableKey = mongoKey + ".TableContent";
				doc.put(mongoKey + ".Type", "Table");
				String tcnt = m.group(1);
				com.mongodb.BasicDBList dbTable = new BasicDBList();
				List<ArrayList<String>> rows = TableParseUtil.parseTableToArray(tcnt, true);
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
				doc.put(capacityTableKey, dbTable);
			}
		}

		if (cnt.indexOf("<div id=\"trend\" data-app=\"openableBox\" class=\"box sub-box\">") > 0) {
			String part3 = cnt.substring(part2EndIndex);
			int busIndex = part3.indexOf("<div class=\"box-content\">");
			part3 = part3.substring(busIndex);
			String busRule = "(?ism)<h3.*?>(.*?)</h3>.*?<div.*?>(.*?)</div>";
			Pattern bugitional = cachePattern.get(busRule);
			if (bugitional == null) {
				bugitional = Pattern.compile(busRule);
				cachePattern.put(busRule, bugitional);
			}
			m = bugitional.matcher(part3);
			com.mongodb.BasicDBList buslist = new BasicDBList();
			while (m.find()) {
				com.mongodb.BasicDBList rowlist = new BasicDBList();
				rowlist.add(m.group(1).replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ").trim());
				rowlist.add(m.group(2).replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ").trim());
				buslist.add(rowlist);
			}
			if (buslist.size() > 0) {
				mongoKey = "DetailedInformation.RelatedNews";// list 二级
				doc.put(mongoKey, buslist);
			}
		}
		doc.putAll(term);
		mongo.put(term, doc);
		page.setText(new Utf8("parsed"));
		store.put(key, page);
		LogUtils.info("{key:\"" + key + "\",baseUrl:\"" + page.getBaseUrl() + "\"}");
	}

	public static void parsePartInfo(DataStore<String, WebPage> store, String infoUrl, Map<String, Object> doc) {
		WebPage page = null;
		try {
			page = store.get(TableUtil.reverseUrl(infoUrl));
		} catch (MalformedURLException e) {
			LogUtils.debug("infoUrl:" + infoUrl, e);
		}
		String cnt = getCnt(page);
		String mongoKey = null;
		int part1SIndex = cnt.indexOf("<div class=\"col-md-5 col-mb\">");
		int part1EIndex = cnt.indexOf("<div class=\"col-md-7 maps col-mb\">");
		if (part1SIndex == -1) {
			LogUtils.error("infoUrl:" + infoUrl);
			return;
		}
		if (part1EIndex == -1) {
			part1EIndex = cnt.indexOf("<div class=\"row\">", part1SIndex);
			if (part1EIndex == -1) {
				part1EIndex = cnt.indexOf("<div class=\"supplier-code pull-right\">", part1SIndex);
				if (part1EIndex == -1) {
					LogUtils.error("infoUrl:" + infoUrl);
					return;
				}
			}
		}

		String part1 = cnt.substring(part1SIndex, part1EIndex);
		String titleRule = "(?ism)<h4>(.+?)</h4>";

		Pattern head = cachePattern.get(titleRule);
		if (head == null) {
			head = Pattern.compile(titleRule);
			cachePattern.put(titleRule, head);
		}
		Matcher m = head.matcher(part1);

		List<String> itemTypes = new ArrayList<String>();
		List<Integer> itemTypeIndex = new ArrayList<Integer>();
		itemTypes.add("");
		while (m.find()) {
			itemTypes.add(m.group(1).replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\?|\\.|。|・| ", "")
					.replaceAll("\\n", "").replaceAll("\\?", "").trim());
			itemTypeIndex.add(m.end());
		}
		itemTypeIndex.add(part1.length());

		String itemRule = "(?ism)<p.*?>(.*?)</p>";
		Pattern item = cachePattern.get(itemRule);
		if (item == null) {
			item = Pattern.compile(itemRule);
			cachePattern.put(itemRule, item);
		}
		for (int i = 1; i < itemTypeIndex.size(); i++) {
			String valStrHtml = part1.substring(itemTypeIndex.get(i - 1), itemTypeIndex.get(i));
			m = item.matcher(valStrHtml);
			String title = itemTypes.get(i);
			if (title.trim().startsWith("国家")) {
				mongoKey = "KeyAttributes.Location.OriginalLocation.Country";
				if (m.find()) {
					doc.put(mongoKey, m.group(1).trim());
				}
			} else if (title.trim().startsWith("地址")) {
				mongoKey = "KeyAttributes.Location.OriginalLocation.Address";
				if (m.find()) {
					doc.put(mongoKey, m.group(1).trim());
					String _itemRule = "<span.*?>(.*?)</span>(.*)";
					Pattern _item = cachePattern.get(_itemRule);
					if (_item == null) {
						_item = Pattern.compile(_itemRule);
						cachePattern.put(_itemRule, _item);
					}
					Matcher _m = _item.matcher(valStrHtml);
					while (_m.find()) {
						String ty = _m.group(1).trim();
						String val = _m.group(2);
						if (ty.trim().startsWith("TEL")) {
							mongoKey = "BasicAttributes.Contact.Telephone";
							doc.put(mongoKey, val.replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ")
									.replaceAll("&nbsp;", " ").trim());
						} else if (ty.trim().startsWith("FAX")) {
							mongoKey = "BasicAttributes.Contact.Fax";
							doc.put(mongoKey, val.replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ")
									.replaceAll("&nbsp;", " ").trim());
						}
					}
				}
			} else if (title.trim().startsWith("成立")) {
				mongoKey = "BasicAttributes.FoundedDate";
				if (m.find()) {
					doc.put(mongoKey, m.group(1).trim());
				}
			} else if (title.trim().startsWith("公司负责人")) {
				mongoKey = "DetailedInformation.ManagementTeam.Executives.Name";
				if (m.find()) {
					doc.put(mongoKey, m.group(1).trim());
				}
			} else if (title.trim().startsWith("网址")) {
				mongoKey = "BasicAttributes.Website";
				if (m.find()) {
					doc.put(mongoKey, m.group(1).replaceAll("(?ism)</?\\w+.*?/?>", "").trim());
				}
			} else if (title.trim().startsWith("员工")) {
				mongoKey = "KeyAttributes.Scale.EmployeeNumber";
				if (m.find()) {
					doc.put(mongoKey, m.group(1).trim());
				}
			} else if (title.trim().startsWith("年销售")) {
				mongoKey = "DetailedInformation.Revenue.Revenue";
				if (m.find()) {
					doc.put(mongoKey, m.group(1).trim());
				}
			} else if (title.trim().startsWith("注册资本")) {
				mongoKey = "DetailedInformation.RegisteredInformation.RegistCap";
				if (m.find()) {
					doc.put(mongoKey, m.group(1).trim());
				}
			} else if (title.trim().startsWith("质量认证") || title.trim().startsWith("环境认证")) {
				mongoKey = "DetailedInformation.Certificate.Standard";// list
				com.mongodb.BasicDBList dblist = (BasicDBList) doc.get(mongoKey);
				if (dblist == null) {
					dblist = new BasicDBList();
					doc.put(mongoKey, dblist);
				}
				while (m.find()) {
					dblist.add(m.group(1).trim());
				}
			}
		}

		int part2Eindex = cnt.indexOf("<div class=\"supplier-code pull-right\">");
		if (part2Eindex == -1) {
			part2Eindex = cnt.length();
		}
		int subPart1Sindex = cnt.indexOf("<div class=\"row product\">");
		if (subPart1Sindex > 0) {
			String part2 = cnt.substring(subPart1Sindex + 25, part2Eindex);
			String parts[] = part2.split("<div class=\"row product\">");
			for (String string : parts) {
				String _itemRule = "<div class=\"col-md-3.*?>(.*?)</div>";
				Pattern _item = cachePattern.get(_itemRule);
				if (_item == null) {
					_item = Pattern.compile(_itemRule);
					cachePattern.put(_itemRule, _item);
				}
				Matcher _m = _item.matcher(string);
				if (_m.find()) {
					String title = _m.group(1).replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\?|\\.|。|・| ", "")
							.replaceAll("\\n", "").replaceAll("\\?", "").trim();
					if (title.trim().matches("产品名")) {
						mongoKey = "BasicAttributes.Products.ProductName";// list
						com.mongodb.BasicDBList dblist = (BasicDBList) doc.get(mongoKey);
						if (dblist == null) {
							dblist = new BasicDBList();
							doc.put(mongoKey, dblist);
						}
						com.mongodb.BasicDBList list = new BasicDBList();
						itemRule = "(?ism)<div class=\"col-md-6.*?>(.*?)</div>";
						item = cachePattern.get(itemRule);
						if (item == null) {
							item = Pattern.compile(itemRule);
							cachePattern.put(itemRule, item);
						}
						m = item.matcher(string);
						while (m.find()) {
							list.add(m.group(1).trim());
						}
						dblist.add(list);
					} else if (title.trim().matches("其他产品.*?中.*")) {
						string = string.replaceAll("<span>,</span>", "");
						mongoKey = "BasicAttributes.Products.ProductName";// list
						com.mongodb.BasicDBList dblist = (BasicDBList) doc.get(mongoKey);
						if (dblist == null) {
							dblist = new BasicDBList();
							doc.put(mongoKey, dblist);
						}
						com.mongodb.BasicDBList list = new BasicDBList();
						itemRule = "(?ism)<span .*?>(.*?)</span>";
						item = cachePattern.get(itemRule);
						if (item == null) {
							item = Pattern.compile(itemRule);
							cachePattern.put(itemRule, item);
						}
						m = item.matcher(string);
						while (m.find()) {
							list.add(m.group(1).trim());
						}
						dblist.add(list);

					} else if (title.trim().matches("其他产品.*?英.*")) {
						string = string.replaceAll("<span>,</span>", "");
						mongoKey = "BasicAttributes.Products.ProductName";// list
						com.mongodb.BasicDBList dblist = (BasicDBList) doc.get(mongoKey);
						if (dblist == null) {
							dblist = new BasicDBList();
							doc.put(mongoKey, dblist);
						}
						com.mongodb.BasicDBList list = new BasicDBList();
						itemRule = "(?ism)<span .*?>(.*?)</span>";
						item = cachePattern.get(itemRule);
						if (item == null) {
							item = Pattern.compile(itemRule);
							cachePattern.put(itemRule, item);
						}
						m = item.matcher(string);
						while (m.find()) {
							list.add(m.group(1).trim());
						}
						dblist.add(list);
					} else if (title.trim().matches("客户")) {
						mongoKey = "DetailedInformation.Clients.MainClients";// list
						com.mongodb.BasicDBList dblist = (BasicDBList) doc.get(mongoKey);
						if (dblist == null) {
							dblist = new BasicDBList();
							doc.put(mongoKey, dblist);
						}
						itemRule = "(?ism)<div class=\"col-md-6.*?>(.*?)</div>";
						item = cachePattern.get(itemRule);
						if (item == null) {
							item = Pattern.compile(itemRule);
							cachePattern.put(itemRule, item);
						}
						m = item.matcher(string);
						while (m.find()) {
							dblist.add(m.group(1).trim());
						}
					} else if (title.trim().matches("配套情况")) {
						String tbHeadRule = "(?ism)<table .*?>(.*)</table>";
						Pattern tbRow = cachePattern.get(tbHeadRule);
						if (tbRow == null) {
							tbRow = Pattern.compile(tbHeadRule);
							cachePattern.put(tbHeadRule, tbRow);
						}
						m = tbRow.matcher(part2);
						if (m.find()) {
							mongoKey = "BasicAttributes.Products.AccessoryInformation";
							String capacityTableKey = mongoKey + ".TableContent";
							doc.put(mongoKey + ".Type", "Table");
							String tcnt = m.group(1);
							com.mongodb.BasicDBList dbTable = new BasicDBList();
							List<ArrayList<String>> rows = TableParseUtil.parseTableToArray(tcnt, true);
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
							doc.put(capacityTableKey, dbTable);
						}
					}
				}
			}
		}
	}

	public static void parsePartDetail(DataStore<String, WebPage> store, String detailUrl, Map<String, Object> doc) {
		WebPage page = null;
		try {
			page = store.get(TableUtil.reverseUrl(detailUrl));
		} catch (MalformedURLException e) {
			LogUtils.debug("infoUrl:" + detailUrl, e);
		}
		String mongoKey = null;
		String cnt = getCnt(page);
		int cntStartIndex = cnt.indexOf("<h1 class=\"title\"><span>");
		int cntEndIndex = cnt.indexOf("<div id=\"side_menu\" class=\"frame_left\">");
		cnt = cnt.substring(cntStartIndex, cntEndIndex);
		cntStartIndex = cnt.indexOf("<div id=\"internal-link\">");
		cntEndIndex = cnt.indexOf("<div class=\"col-md-12\">");

		String linkListStr = cnt.substring(cntStartIndex, cntEndIndex);
		cnt = cnt.substring(cntEndIndex);
		String linkRegStr = "</i><a href=\"#(.*?)\">(.*?)</a></li>";
		Pattern linkReg = cachePattern.get(linkRegStr);
		if (linkReg == null) {
			linkReg = Pattern.compile(linkRegStr);
			cachePattern.put(linkRegStr, linkReg);
		}
		Matcher m = linkReg.matcher(linkListStr);
		List<String> linkIds = new ArrayList<String>();
		List<String> linkNames = new ArrayList<String>();
		List<Integer> linkIndex = new ArrayList<Integer>();
		while (m.find()) {
			String linkId = m.group(1);
			String linkName = m.group(2);
			String idRule = " id\\s*=\\s*[\"']?" + linkId + "[\"']?";
			Pattern idreg = cachePattern.get(idRule);
			if (idreg == null) {
				idreg = Pattern.compile(idRule);
				cachePattern.put(idRule, idreg);
			}
			Matcher idm = idreg.matcher(cnt);
			if (idm.find()) {
				linkIds.add(linkId);
				linkNames.add(linkName);
				int index = cnt.indexOf("<", idm.start() - 5);
				linkIndex.add(index);
			} else if (!linkId.equals("suppliers_primaries_detail")) {
				LogUtils.error("error parsed page find " + linkId + ": " + detailUrl);
			}
		}
		linkIndex.add(cnt.length());
		for (int i = 0; i < linkIds.size(); i++) {
			String linkId = linkIds.get(i);
			String linkName = linkNames.get(i);
			String partStr = cnt.substring(linkIndex.get(i), linkIndex.get(i + 1));
			if (linkId.equals("suppliers_primaries_detail")) {// 公司概要
				continue;
			} else if (linkId.equals("trend")) {// 新闻
				parsePartDetailNews(doc, partStr);
			} else if (linkId.equals("businessScope")) {// 业务内容
				parsePartDetailBusinessScope(doc, partStr);
			} else if (linkId.equals("capitalStructure")) {// 资本构成
				parsePartDetailCapitalStructure(doc, partStr);
			} else if (linkId.equals("exhibitions")) {// 参展信息
				parsePartDetailExhibitions(doc, partStr);
			} else if (linkId.equals("products")) {// 主要产品
				// parsePartDetailProducts(doc, partStr);
			} else if (linkId.equals("delivers")) {// 主要客户
				parsePartDetailDelivers(doc, partStr);
			} else if (linkId.equals("delivery-status")) {// 配套情况
				parsePartDetailDeliverStatus(doc, partStr);
			} else if (linkId.equals("highlight")) {// 业务摘要
				parsePartDetailHighlight(doc, partStr);
			} else if (linkId.equals("subsidiary")) {// 子公司/关联公司/生产基地
				parsePartDetailSubsidiary(doc, partStr);
			} else if (linkId.equals("various-data")) {// 数据
				parsePartDetailVariousData(doc, partStr);
			} else if (linkId.equals("history")) {// 公司历程
				parsePartDetailHistory(doc, partStr);
			} else if (linkId.equals("information")) {// 补充 1
			} else if (linkId.equals("updated")) {// 更新历史
			}

		}

	}

	// 新闻
	public static void parsePartDetailNews(Map<String, Object> doc, String partStr) {
		String mongoKey = "DetailedInformation.RelatedNews";// list 二级
		String busRule = "(?ism)<div .*?>\\s*<h4.*?>(.*?)</h4>\\s*<p>(.*?)</p>\\s*</div>";
		Pattern item = cachePattern.get(busRule);
		if (item == null) {
			item = Pattern.compile(busRule);
			cachePattern.put(busRule, item);
		}
		Matcher m = item.matcher(partStr);
		com.mongodb.BasicDBList buslist = new BasicDBList();
		while (m.find()) {
			com.mongodb.BasicDBList rowlist = new BasicDBList();
			rowlist.add(m.group(1).replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ").trim());
			rowlist.add(m.group(2).replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ").trim());
			buslist.add(rowlist);
		}
		if (buslist.size() > 0) {
			doc.put(mongoKey, buslist);
		}
	}

	// 业务内容
	private static void parsePartDetailBusinessScope(Map<String, Object> doc, String partStr) {
		int endIndex = partStr.length();
		String busRule = "<h4>(.*?)</h4>";
		Pattern item = cachePattern.get(busRule);
		if (item == null) {
			item = Pattern.compile(busRule);
			cachePattern.put(busRule, item);
		}
		Matcher m = item.matcher(partStr);
		List<String> itemTypes = new ArrayList<String>();
		List<Integer> itemTypeIndex = new ArrayList<Integer>();
		itemTypes.add("");
		while (m.find()) {
			String suTitle = m.group(1).replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\?|\\.|。|・| | ", "")
					.replaceAll("\\n", "").replaceAll("\\?", "").trim();
			if (suTitle.length() > 100) {// 内容作标题显示了
				continue;//
			}
			itemTypes.add(suTitle);
			itemTypeIndex.add(m.end());
		}
		itemTypeIndex.add(partStr.length());
		if (itemTypeIndex.size() > 0) {
			endIndex = itemTypeIndex.get(0);
		}
		for (int l = 1; l < itemTypeIndex.size(); l++) {
			if (itemTypes.get(l).trim().equals("")) {
				continue;
			}
			String thisPart = partStr.substring(itemTypeIndex.get(l - 1), itemTypeIndex.get(l));
			thisPart = thisPart.replaceAll(busRule, "");
			// if (itemTypes.get(l).trim().indexOf("竞争企业") > -1) {
			// doc.put("DetailedInformation.Competitor", parseObject(itemTypes.get(l).trim(), thisPart));
			// // TableParseUtil.parseTable(doc, "DetailedInformation.Competitor", itemTypes.get(l).trim(), null,
			// // thisPart);
			// } else {
			parseCommon(doc, "DetailedInformation.Business.BusinessInformation", itemTypes.get(l), thisPart);
			// psOther("业务内容." + itemTypes.get(l), thisPart, doc.get("CompanyCode").toString(),
			// doc.get("DetailedInformation.SourceURL.DetailURL").toString());
			// }
		}
		String mongoKey = "DetailedInformation.Business.BusinessDescription";//
		busRule = "(?ism)<div .*?>(.*?)</div>";
		item = cachePattern.get(busRule);
		if (item == null) {
			item = Pattern.compile(busRule);
			cachePattern.put(busRule, item);
		}
		m = item.matcher(partStr.subSequence(0, endIndex));
		if (m.find()) {
			com.mongodb.BasicDBList rowlist = new BasicDBList();
			String busStr = m.group(1);
			String busStrs[] = busStr.split("<br/?>");
			for (String string : busStrs) {
				string = string.replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ").trim();
				if (!string.equals(""))
					rowlist.add(string);
			}
			if (rowlist.size() > 0) {
				doc.put(mongoKey, rowlist);
			}
		}
	}

	// 资本构成
	private static void parsePartDetailCapitalStructure(Map<String, Object> doc, String partStr) {
		// String mongoKey = "DetailedInformation.OtherFinancialInfo.CapitalStructure";// table
		// TableParseUtil.parseTable(doc, mongoKey, "资本构成", "Description", "Date", partStr);

		String dataParts[] = partStr.split("<h4.*?>(.*?)</h4>");
		String mongoKey = "DetailedInformation.OtherInformation.CapitalStructure";// table
		// TableParseUtil.parseTable(doc, mongoKey, "数据", null, dataParts[0]);// 数据
		String busRule = "<h4.*?>(.*?)</h4>";
		Pattern item = cachePattern.get(busRule);
		if (item == null) {
			item = Pattern.compile(busRule);
			cachePattern.put(busRule, item);
		}
		Matcher m = item.matcher(partStr);
		List<String> itemTypes = new ArrayList<String>();
		List<Integer> itemTypeIndex = new ArrayList<Integer>();
		itemTypes.add("");

		String dataRule = "(?ism)<table.*?>(.*?)</table>";
		Pattern dataItem = cachePattern.get(dataRule);
		if (dataItem == null) {
			dataItem = Pattern.compile(dataRule);
			cachePattern.put(dataRule, dataItem);
		}
		Matcher datam = dataItem.matcher(dataParts[0]);
		List<Integer> startIndex = new ArrayList<Integer>();
		List<Integer> endIndex = new ArrayList<Integer>();
		while (datam.find()) {
			startIndex.add(datam.start());
			endIndex.add(datam.end());
		}
		if (endIndex.size() > 0) {
			String lastTitle = "资本构成";
			for (int j = 0; j < startIndex.size(); j++) {
				List<ArrayList<String>> rows = TableParseUtil.parseTableToArray(
						dataParts[0].substring(startIndex.get(j), endIndex.get(j)), true);
				if (rows.size() > 1) {// 数据表
					if (lastTitle.equals("资本构成")) {
						itemTypes.add(lastTitle);
						itemTypeIndex.add(startIndex.get(j));
					}
					// if (j < startIndex.size() - 1) {
					// itemTypeIndex.add(startIndex.get(j + 1));
					// } else {
					// }
				} else if (rows.size() == 1) {
					lastTitle = (rows.get(0).get(0));
					itemTypes.add(lastTitle);
					itemTypeIndex.add(startIndex.get(j));
				}
			}
		} else {
			// itemTypeIndex.add(dataParts[0].indexOf("资本构成") + 4);
		}
		while (m.find()) {
			String suTitle = m.group(1).trim().replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\?|\\.|。|・| ", "")
					.replaceAll("\\n", "").replaceAll("\\?", "").trim();
			if (suTitle.length() > 50) {// 内容作标题显示了
				continue;//
			}

			itemTypes.add(suTitle);
			itemTypeIndex.add(m.end());
		}
		if (itemTypes.size() == 1) {
			itemTypes.add("资本构成");
			itemTypeIndex.add(dataParts[0].indexOf("资本构成") + 4);
		} else if (itemTypes.size() == 2) {
			itemTypes.set(1, "资本构成");
			itemTypeIndex.set(0, dataParts[0].indexOf("资本构成") + 4);
		}
		itemTypeIndex.add(partStr.length());
		for (int l = 1; l < itemTypeIndex.size(); l++) {
			if (itemTypes.get(l).trim().equals("")) {
				continue;
			}
			String thisPart = partStr.substring(itemTypeIndex.get(l - 1), itemTypeIndex.get(l));
			thisPart = thisPart.replaceAll(busRule, "");
			if (l == 1 && thisPart.trim().length() < 100) {
				continue;
			}
			if (itemTypes.get(l).trim().indexOf("资本构成") > -1) {
				doc.put("DetailedInformation.OtherFinancialInfo.CapitalStructure",
						parseObject(itemTypes.get(l).trim(), thisPart));
			} else {
				parseCommon(doc, "DetailedInformation.OtherInformation.CapitalStructureOther", itemTypes.get(l),
						thisPart);
				// psOther("资本构成." + itemTypes.get(l), thisPart, doc.get("CompanyCode").toString(),
				// doc.get("DetailedInformation.SourceURL.DetailURL").toString());
			}
		}
	}

	// 参展信息
	private static void parsePartDetailExhibitions(Map<String, Object> doc, String partStr) {
		String mongoKey = "DetailedInformation.OtherInformation.Exhibition";// list 二级
		String busRule = "<h4>(.*?)</h4>";
		Pattern item = cachePattern.get(busRule);
		if (item == null) {
			item = Pattern.compile(busRule);
			cachePattern.put(busRule, item);
		}
		Matcher m = item.matcher(partStr);
		List<String> itemTypes = new ArrayList<String>();
		List<Integer> itemTypeIndex = new ArrayList<Integer>();
		itemTypes.add("");
		while (m.find()) {
			itemTypes.add(m.group(1).replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\?|\\.|。|・| ", "")
					.replaceAll("\\n", "").replaceAll("\\?", "").trim());
			itemTypeIndex.add(m.end());
		}
		itemTypeIndex.add(partStr.length());

		String itemRule = "(?ism)<div .*?>(.*?)</div>";
		item = cachePattern.get(itemRule);
		if (item == null) {
			item = Pattern.compile(itemRule);
			cachePattern.put(itemRule, item);
		}
		com.mongodb.BasicDBList dbList = new BasicDBList();
		for (int i = 1; i < itemTypeIndex.size(); i++) {
			BasicDBObject group = new BasicDBObject();
			m = item.matcher(partStr.substring(itemTypeIndex.get(i - 1), itemTypeIndex.get(i)));
			com.mongodb.BasicDBList rowlist = new BasicDBList();
			while (m.find()) {
				rowlist.add(m.group(1).replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ")
						.replaceAll("\t", " ").trim());
			}
			if (rowlist.size() > 0) {
				group.put("exhibitionName", itemTypes.get(i));
				group.put("Content", rowlist);
				dbList.add(group);
			}
		}
		if (dbList.size() > 0) {
			doc.put(mongoKey + ".", dbList);
		}
	}

	// 主要客户，只取 分主要客户销售比例
	private static void parsePartDetailDelivers(Map<String, Object> doc, String partStr) {
		// String mongoKey = "DetailedInformation.Sales.MainClientsSalesProportion";// table
		// TableParseUtil.parseTable(doc, mongoKey, "分主要客户销售比例", null, "Unit", partStr);

		String dataParts[] = partStr.split("<h4.*?>(.*?)</h4>");
		// TableParseUtil.parseTable(doc, mongoKey, "数据", null, dataParts[0]);// 数据
		String busRule = "<h4.*?>(.*?)</h4>";
		Pattern item = cachePattern.get(busRule);
		if (item == null) {
			item = Pattern.compile(busRule);
			cachePattern.put(busRule, item);
		}
		Matcher m = item.matcher(partStr);
		List<String> itemTypes = new ArrayList<String>();
		List<Integer> itemTypeIndex = new ArrayList<Integer>();
		itemTypes.add("");

		String dataRule = "(?ism)<table.*?>(.*?)</table>";
		Pattern dataItem = cachePattern.get(dataRule);
		if (dataItem == null) {
			dataItem = Pattern.compile(dataRule);
			cachePattern.put(dataRule, dataItem);
		}
		Matcher datam = dataItem.matcher(dataParts[0]);
		List<Integer> startIndex = new ArrayList<Integer>();
		List<Integer> endIndex = new ArrayList<Integer>();
		while (datam.find()) {
			startIndex.add(datam.start());
			endIndex.add(datam.end());
		}
		if (endIndex.size() > 0) {
			String lastTitle = "主要客户";
			for (int j = 0; j < startIndex.size(); j++) {
				List<ArrayList<String>> rows = TableParseUtil.parseTableToArray(
						dataParts[0].substring(startIndex.get(j), endIndex.get(j)), true);
				if (rows.size() > 1) {// 数据表
					itemTypes.add(lastTitle);
					if (lastTitle.equals("主要客户")) {
						itemTypes.add(lastTitle);
						itemTypeIndex.add(startIndex.get(j));
					}
					// if (j < startIndex.size() - 1) {
					// itemTypeIndex.add(startIndex.get(j + 1));
					// } else {
					// }
				} else if (rows.size() == 1) {
					lastTitle = (rows.get(0).get(0));
					itemTypes.add(lastTitle);
					itemTypeIndex.add(startIndex.get(j));
				}
			}
		}
		while (m.find()) {
			String suTitle = m.group(1).trim().replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\?|\\.|。|・| ", "")
					.replaceAll("\\n", "").replaceAll("\\?", "").trim();
			if (suTitle.length() > 50) {// 内容作标题显示了
				continue;//
			}

			itemTypes.add(suTitle);
			itemTypeIndex.add(m.end());
		}
		if (itemTypes.size() == 1) {
			itemTypes.add("主要客户");
			itemTypeIndex.add(dataParts[0].indexOf("主要客户") + 4);
		}

		itemTypeIndex.add(partStr.length());
		for (int l = 1; l < itemTypeIndex.size(); l++) {
			if (itemTypes.get(l).trim().equals("")) {
				continue;
			}
			String thisPart = partStr.substring(itemTypeIndex.get(l - 1), itemTypeIndex.get(l));
			thisPart = thisPart.replaceAll(busRule, "");
			if (l == 1 && thisPart.trim().length() < 100) {
				continue;
			}
			// if (itemTypes.get(l).trim().indexOf("分主要客户销售比例") > -1) {
			// doc.put("DetailedInformation.Sales.MainClientsSalesProportion",
			// parseObject(itemTypes.get(l).trim(), thisPart));
			// } else {
			parseCommon(doc, "DetailedInformation.Clients.ClientsInformation", itemTypes.get(l), thisPart);

			// }
		}
	}

	// 配套情况
	private static void parsePartDetailDeliverStatus(Map<String, Object> doc, String partStr) {
		String mongoKey = "DetailedInformation.Proucts.AccessoryInformation";// table
		TableParseUtil.addTable(doc, mongoKey, "配套情况", partStr);
	}

	// 业务摘要DetailedInformation.Business.Summary
	private static void parsePartDetailHighlight(Map<String, Object> doc, String partStr) {
		String busRule = "<h4>(.*?)</h4>";
		Pattern item = cachePattern.get(busRule);
		if (item == null) {
			item = Pattern.compile(busRule);
			cachePattern.put(busRule, item);
		}
		Matcher m = item.matcher(partStr);
		List<String> itemTypes = new ArrayList<String>();
		List<Integer> itemTypeIndex = new ArrayList<Integer>();
		itemTypes.add("");
		while (m.find()) {
			String suTitle = m.group(1).replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\?|\\.|。|・| | ", "")
					.replaceAll("\\n", "").replaceAll("\\?", "").trim();
			if (suTitle.length() > 100) {// 内容作标题显示了
				continue;//
			}
			itemTypes.add(suTitle);
			itemTypeIndex.add(m.end());
		}
		itemTypeIndex.add(partStr.length());

		for (int l = 1; l < itemTypeIndex.size(); l++) {
			if (itemTypes.get(l).trim().equals("")) {
				continue;
			}
			String thisPart = partStr.substring(itemTypeIndex.get(l - 1), itemTypeIndex.get(l));
			thisPart = thisPart.replaceAll(busRule, "");
			parseCommon(doc, "DetailedInformation.Business.Summary", itemTypes.get(l), thisPart);

			// if (itemTypes.get(l).startsWith("业绩")) {// 业绩
			// String mongoKey = "BasicAttributes.Business.BusinessPerformance";// table
			// TableParseUtil.parseTable(doc, mongoKey, itemTypes.get(l), "Unit", thisPart);
			// } else if (itemTypes.get(l).startsWith("订单")) {
			// String mongoKey = "BasicAttributes.Business.Orders";
			// doc.put(mongoKey, thisPart.replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ").trim());
			// } else if (itemTypes.get(l).startsWith("业务转让")) {
			// String mongoKey = "BasicAttributes.Business.BusinessForSale";
			// doc.put(mongoKey, thisPart.replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ").trim());
			// } else if (itemTypes.get(l).startsWith("生产动向")) {
			// String mongoKey = "DetailedInformation.Manufacture.ManufactureTrend";
			// doc.put(mongoKey, thisPart.replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ").trim());
			// } else if (itemTypes.get(l).startsWith("业务计划")) {
			// String mongoKey = "BasicAttributes.Business.BusinessPlan";
			// doc.put(mongoKey, thisPart.replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ").trim());
			// } else if (itemTypes.get(l).startsWith("2016年")) {
			// String mongoKey = "DetailedInformation.OtherInformation.Anticipation2016";// table
			// TableParseUtil.parseTable(doc, mongoKey, itemTypes.get(l), "Unit", thisPart);
			// } else if (itemTypes.get(l).startsWith("研发费")) {
			// String mongoKey = "DetailedInformation.Research.ResearchBudget";// table
			// TableParseUtil.parseTable(doc, mongoKey, itemTypes.get(l), "Unit", thisPart);
			// } else if (itemTypes.get(l).startsWith("研发体制")) {
			// String mongoKey = "DetailedInformation.Research.ResearchSystem";
			// doc.put(mongoKey, thisPart.replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ").trim());
			// } else if (itemTypes.get(l).startsWith("研发活动")) {
			// String mongoKey = "DetailedInformation.Research.ResearchActivity";
			// doc.put(mongoKey, thisPart.replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ").trim());
			// } else if (itemTypes.get(l).startsWith("投资额")) {
			// String mongoKey = "DetailedInformation.Investment.InvestmentAmount";// table
			// int end = TableParseUtil.parseTable(doc, mongoKey, itemTypes.get(l), "Unit", thisPart);
			// String descStr = thisPart.substring(end);
			// doc.put(mongoKey + ".Description", thisPart.replaceAll("(?ism)</?\\w+.*?/?>", "")
			// .replaceAll("\\n", " ").trim());
			// } else if (itemTypes.get(l).startsWith("设备新增计划")) {
			// String mongoKey = "DetailedInformation.OtherInformation.FacilityPurchasingPlan";// table
			// int end = TableParseUtil.parseTable(doc, mongoKey, itemTypes.get(l), "Date", thisPart);
			// String descStr = thisPart.substring(end);
			// doc.put(mongoKey + ".Description", thisPart.replaceAll("(?ism)</?\\w+.*?/?>", "")
			// .replaceAll("\\n", " ").trim());
			// } else if (itemTypes.get(l).startsWith("海外业务")) {
			// String mongoKey = "BasicAttributes.Business.OverseaBusiness";
			// doc.put(mongoKey, thisPart.replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ").trim());
			// } else if (itemTypes.get(l).startsWith("订单")) {
			// String mongoKey = "BasicAttributes.Business.Orders";
			// doc.put(mongoKey, thisPart.replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ").trim());
			// } else {
			// if (itemTypes.get(l).trim().equals("")) {
			// LogUtils.error("null title");
			// } else {
			// String mongoKey = "业务摘要." + itemTypes.get(l);
			// doc.put(mongoKey, thisPart.replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ").trim());
			// psOther(mongoKey, thisPart, doc.get("CompanyCode").toString(), doc.get("detailUrl").toString());
			// }
			// }
		}
	}

	static void psOther(String mongoKey, String thisPart, String CompanyCode, String detailUrl) {
		List<String[]> urls = keys.get(mongoKey);
		if (urls == null) {
			urls = new ArrayList<String[]>();
			keys.put(mongoKey, urls);
		}
		if (detailUrl.equals("https://www.marklines.com/cn/top500/s500_137")) {
			System.out.println(detailUrl);
		}

		String type = "string";
		String itemRule = "<td.*?(class=\"right\")|(align=\"right\").*?>(.*?)</td>";
		Pattern item = cachePattern.get(itemRule);
		if (item == null) {
			item = Pattern.compile(itemRule);
			cachePattern.put(itemRule, item);
		}
		Matcher tm = item.matcher(thisPart);
		boolean tableTt = tm.find();
		itemRule = "(?ism)<table.*?>(.*?)</table>";
		item = cachePattern.get(itemRule);
		if (item == null) {
			item = Pattern.compile(itemRule);
			cachePattern.put(itemRule, item);
		}
		Matcher _tm = item.matcher(thisPart);
		boolean table = _tm.find();
		if (table) {
			type = "table1";
		}
		if (tableTt && table) {
			if (tableTt && type != null && !type.equals("") && tm.end() < _tm.start()) {
				type = "table2";
			}
		}
		urls.add(new String[] { CompanyCode, type, detailUrl });
	}

	public static void parseCommon(Map<String, Object> doc, String mongoKey, String title, String thisPart) {
		BasicDBList groupList = (BasicDBList) doc.get(mongoKey);
		if (groupList == null) {
			groupList = new BasicDBList();
			doc.put(mongoKey, groupList);
		}
		BasicDBObject label = parseObject(title, thisPart);
		groupList.add(label);
	}

	public static BasicDBObject parseObject(String title, String thisPart) {
		BasicDBObject label = new BasicDBObject();
		label.put("title", title);
		String busRule = "(?ism)<table.*?>(.*?)</table>";
		Pattern item = cachePattern.get(busRule);
		if (item == null) {
			item = Pattern.compile(busRule);
			cachePattern.put(busRule, item);
		}
		Matcher m = item.matcher(thisPart);
		int startIndex = -1;
		int endIndex = -1;
		int dataType = 1;// string

		String table1 = null;
		String table2 = null;
		if (m.find()) {
			table1 = m.group(1);
			startIndex = m.start();
			endIndex = m.end();
			dataType = 2;
			if (m.find()) {
				table2 = m.group(1);
				endIndex = m.end();
				dataType = 6;
			}
		}
		String descStr = null;
		String tableStr = null;
		if (table2 != null) {
			descStr = table1;
			tableStr = table2;
			dataType = 6;
		} else if (table1 != null) {
			tableStr = table1;
		}
		if ((dataType & 2) == 2) {
			if ((dataType & 4) != 4) {
				String itemRule = "<td.*?[(class=\"right\")|(align=\"right\")].*?>(.*?)</td>";
				item = cachePattern.get(itemRule);
				if (item == null) {
					item = Pattern.compile(itemRule);
					cachePattern.put(itemRule, item);
				}
				Matcher _m = item.matcher(thisPart);
				if (_m.find() && _m.end() < startIndex) {
					startIndex = _m.start();
					label.put(TableParseUtil.getUnitTitle(_m.group(1), "Unit"), _m.group(1));
				}
			} else {
				List<ArrayList<String>> rows = TableParseUtil.parseTableToArray(descStr, true);
				if (rows.size() > 0) {
					ArrayList<String> head = rows.get(0);
					if (head.size() > 0) {
						label.put("TableTitle", head.get(0));
					}
					if (head.size() > 1) {
						label.put(TableParseUtil.getUnitTitle(head.get(1), "Unit"), head.get(1));
					}
				}
			}
			if (tableStr != null) {
				label.put("type", "Table");
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
				label.put("TableContent", dbTable);
			}
		}
		if (startIndex == -1) {
			startIndex = thisPart.length();
		}
		if (endIndex == -1) {
			endIndex = startIndex;
		}
		String forTextStr = thisPart.substring(0, startIndex);
		String endTextStr = "";
		if (endIndex > 0 && endIndex < thisPart.length())
			endTextStr = thisPart.substring(endIndex, thisPart.length());
		String text = "";
		if (forTextStr.length() > 0) {
			text = forTextStr.replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ").trim();
		}
		if (endTextStr.length() > 0) {
			text += endTextStr.replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ").trim();
		}
		if (text.trim().length() > 0)
			label.put("text", text);
		return label;
	}

	// 子公司/关联公司/生产基地 DetailedInformation.RelatedCompany.Related
	private static void parsePartDetailSubsidiary(Map<String, Object> doc, String partStr) {
		String busRule = "<h4>(.*?)</h4>";
		Pattern item = cachePattern.get(busRule);
		if (item == null) {
			item = Pattern.compile(busRule);
			cachePattern.put(busRule, item);
		}
		Matcher m = item.matcher(partStr);
		List<String> itemTypes = new ArrayList<String>();
		List<Integer> itemTypeIndex = new ArrayList<Integer>();
		itemTypes.add("");
		while (m.find()) {
			String suTitle = m.group(1).replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\?|\\.|。|・| ", "")
					.replaceAll("\\n", "").replaceAll("\\?", "").trim();
			if (suTitle.length() > 100) {// 内容作标题显示了
				continue;//
			}
			itemTypes.add(suTitle);
			itemTypeIndex.add(m.end());
		}
		itemTypeIndex.add(partStr.length());
		for (int l = 1; l < itemTypeIndex.size(); l++) {
			String thisPart = partStr.substring(itemTypeIndex.get(l - 1), itemTypeIndex.get(l));
			thisPart = thisPart.replaceAll(busRule, "");
			parseCommon(doc, "DetailedInformation.RelatedCompany.Related", itemTypes.get(l), thisPart);

			// if (itemTypes.get(l).indexOf("生产基础") >= 0) {
			// String mongoKey = "DetailedInformation.RelatedCompany.JapanProductionBase";// table
			// TableParseUtil.addTable(doc, mongoKey, itemTypes.get(l), thisPart);
			// } else if (itemTypes.get(l).indexOf("子公司") > 0 || itemTypes.get(l).indexOf("关联公司") > 0) {
			// String mongoKey = "DetailedInformation.RelatedCompany.ChildrenAndRelatedCompany";// table
			// TableParseUtil.addTable(doc, mongoKey, itemTypes.get(l), thisPart);
			// } else {
			// String mongoKey = "子公司关联公司生产基地." + itemTypes.get(l);
			// doc.put(mongoKey, thisPart.replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ").trim());
			// psOther(mongoKey, thisPart, doc.get("CompanyCode").toString(), doc.get("detailUrl").toString());
			// }
		}
	}

	// 数据DetailedInformation.OtherInformation.Data
	private static void parsePartDetailVariousData(Map<String, Object> doc, String partStr) {
		String dataParts[] = partStr.split("<h4.*?>(.*?)</h4>");
		String mongoKey = "DetailedInformation.OtherInformation.Data";// table
		// TableParseUtil.parseTable(doc, mongoKey, "数据", null, dataParts[0]);// 数据
		String busRule = "<h4.*?>(.*?)</h4>";
		Pattern item = cachePattern.get(busRule);
		if (item == null) {
			item = Pattern.compile(busRule);
			cachePattern.put(busRule, item);
		}
		Matcher m = item.matcher(partStr);
		List<String> itemTypes = new ArrayList<String>();
		List<Integer> itemTypeIndex = new ArrayList<Integer>();
		itemTypes.add("");

		String dataRule = "(?ism)<table.*?>(.*?)</table>";
		Pattern dataItem = cachePattern.get(dataRule);
		if (dataItem == null) {
			dataItem = Pattern.compile(dataRule);
			cachePattern.put(dataRule, dataItem);
		}
		Matcher datam = dataItem.matcher(dataParts[0]);
		List<Integer> startIndex = new ArrayList<Integer>();
		List<Integer> endIndex = new ArrayList<Integer>();
		while (datam.find()) {
			startIndex.add(datam.start());
			endIndex.add(datam.end());
		}
		if (endIndex.size() > 0) {
			String lastTitle = "数据";
			for (int j = 0; j < startIndex.size(); j++) {
				List<ArrayList<String>> rows = TableParseUtil.parseTableToArray(
						dataParts[0].substring(startIndex.get(j), endIndex.get(j)), true);
				if (rows.size() > 1) {// 数据表
					itemTypes.add(lastTitle);
					if (lastTitle.equals("数据")) {
						itemTypes.add(lastTitle);
						itemTypeIndex.add(startIndex.get(j));
					}
					// if (j < startIndex.size() - 1) {
					// itemTypeIndex.add(startIndex.get(j + 1));
					// } else {
					// }
				} else if (rows.size() == 1) {
					lastTitle = (rows.get(0).get(0));
					itemTypes.add(lastTitle);
					itemTypeIndex.add(startIndex.get(j));
				}
			}
		} else {
			// itemTypeIndex.add(dataParts[0].indexOf("数据") + 4);
		}
		while (m.find()) {
			String suTitle = m.group(1).trim().replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\?|\\.|。|・| ", "")
					.replaceAll("\\n", "").replaceAll("\\?", "").trim();
			if (suTitle.length() > 50) {// 内容作标题显示了
				continue;//
			}

			itemTypes.add(suTitle);
			itemTypeIndex.add(m.end());
		}
		if (itemTypes.size() == 1) {
			itemTypes.add("数据");
			itemTypeIndex.add(dataParts[0].indexOf("数据") + 4);
		}
		// if (itemTypeIndex.size() > 2 && itemTypeIndex.get(0) + 100 > itemTypeIndex.get(1)) {
		// itemTypeIndex.remove(0);
		// }
		itemTypeIndex.add(partStr.length());
		for (int l = 1; l < itemTypeIndex.size(); l++) {
			if (itemTypes.get(l).trim().equals("")) {
				continue;
			}
			String thisPart = partStr.substring(itemTypeIndex.get(l - 1), itemTypeIndex.get(l));
			thisPart = thisPart.replaceAll(busRule, "");
			if (l == 1 && thisPart.trim().length() < 100) {
				continue;
			}
			parseCommon(doc, "DetailedInformation.OtherInformation.Data", itemTypes.get(l), thisPart);

			// if (itemTypes.get(l).indexOf("分部门销售数据") >= 0) {
			// mongoKey = "DetailedInformation.Sales.ProductionSystemSalesProportion";// table
			// TableParseUtil.parseTable(doc, mongoKey, itemTypes.get(l), "Unit", thisPart);
			// } else if (itemTypes.get(l).indexOf("分地区销售数据") > 0) {
			// mongoKey = "DetailedInformation.Sales.SalesByregion";// table
			// TableParseUtil.parseTable(doc, mongoKey, itemTypes.get(l), "Unit", thisPart);
			// } else if (itemTypes.get(l).startsWith("合并")) {
			// mongoKey = "DetailedInformation.Report.ConsolidatedReports";// table
			// TableParseUtil.addTable(doc, mongoKey, itemTypes.get(l), thisPart);
			// } else if (itemTypes.get(l).startsWith("不合并")) {
			// mongoKey = "DetailedInformation.Report.NoConsolidatedReports";// table
			// TableParseUtil.addTable(doc, mongoKey, itemTypes.get(l), thisPart);
			// } else {
			// mongoKey = "数据." + itemTypes.get(l);
			// doc.put(mongoKey, thisPart.replaceAll("(?ism)</?\\w+.*?/?>", "").replaceAll("\\n", " ").trim());
			// psOther(mongoKey, thisPart, doc.get("CompanyCode").toString(), doc.get("detailUrl").toString());
			// }
		}
	}

	// 公司历程
	private static void parsePartDetailHistory(Map<String, Object> doc, String partStr) {
		String mongoKey = "DetailedInformation.CompanyHistory";// table
		TableParseUtil.addTable(doc, mongoKey, "公司历程", partStr);
	}

	static Map<String, List<String[]>> keys = new HashMap<String, List<String[]>>();

	public static void parsePart(DataStore<String, WebPage> store, String key, WebPage page) {
		String cnt = getCnt(page);
		String titleRule = "(?ism)<div class=\"col-md-12 company-padding\">(.*?)"
				+ "</div>.*?(<div data-app=\"sub-ajax-container\" data-source=\"(/" + UrlList.langType
				+ "/supplier_db/.*?)\"></div>)";
		Pattern item = cachePattern.get(titleRule);
		if (item == null) {
			item = Pattern.compile(titleRule);
			cachePattern.put(titleRule, item);
		}
		Matcher m = item.matcher(cnt);
		MongoUtil mongo = getMongo(destTableName);
		while (m.find()) {
			String comId = "";
			String nameHtml = m.group(1);
			Map<String, Object> term = new HashMap<String, Object>();
			Map<String, Object> doc = new HashMap<String, Object>();

			if (m.group(3) != null) {// info
				titleRule = "(?ism)<a.*? href='(/" + UrlList.langType + "/top\\d+/.\\d+_\\d+)'>.*?</a>";
				item = cachePattern.get(titleRule);
				if (item == null) {
					item = Pattern.compile(titleRule);
					cachePattern.put(titleRule, item);
				}
				Matcher nm = item.matcher(nameHtml);
				String detailUrl = null;
				if (nm.find()) {
					detailUrl = "https://www.marklines.com" + nm.group(1);
				} else {
					// continue;// 只解析有详细报告的
				}
				// if (!detailUrl.equals("https://www.marklines.com/cn/top500/s500_207")) {
				// continue;
				// }

				String infoUrl = "https://www.marklines.com" + m.group(3);
				comId = infoUrl.replaceAll(".*?/cn/supplier_db/(.*?)/\\?no_frame.*", "$1");
				term.put("CompanyCode", comId);
				doc.putAll(term);
				nameHtml = nameHtml.replaceFirst("(?ism)<h2 .*?>(.*?)</h2>", "$1");
				nameHtml = nameHtml.replaceFirst("<br/?>", "____");
				nameHtml = nameHtml.replaceAll("(?ism)</?\\w+.*?/?>", "");
				nameHtml = nameHtml.replaceAll("\\n", " ").trim();
				nameHtml = nameHtml.replaceAll("详细报告", " ").trim();
				String names[] = nameHtml.split("____");
				String mongoKey = "CompanyName.Name.NameEng";
				doc.put(mongoKey, names[0].trim());
				if (names.length > 1) {
					mongoKey = "CompanyName.Name.NameNative";
					doc.put(mongoKey, names[1].replaceAll("\\[|\\]", "").trim());
				}
				doc.put("DetailedInformation.SourceURL.PageURL", infoUrl);
				parsePartInfo(store, infoUrl, doc);
				if (detailUrl != null) {// detail
					doc.put("DetailedInformation.SourceURL.DetailURL", detailUrl);
					parsePartDetail(store, detailUrl, doc);
				}
				mongo.put(term, doc);
			}
		}
		// page.setText(new Utf8("parsed"));
		// store.put(key, page);

	}

	static boolean checkParsed = false;

	public static void main(String[] args) throws Exception {
		// LogUtils.info("=================begin parse prod====================");
		// parse(0);
		// LogUtils.info("=================begin parse sale====================");
		// parse(1);
		// LogUtils.info("=================begin parse engine====================");
		// parse(2);
		// LogUtils.info("=================begin parse holcar====================");
		// parse(3);
		// LogUtils.info("=================begin parse part====================");
		// parse(4);

		// String keysCfg = new ObjectMapper().writeValueAsString(keys);
		// System.err.println(keysCfg);
		// File ftile = new File("t.csv");
		// FileOutputStream out = new FileOutputStream(ftile);
		// for (String string : keys.keySet()) {
		// out.write(("\"" + string.replaceAll(",", " ") + "\"").getBytes());
		// out.write(",".getBytes());
		// List<String[]> list = keys.get(string);
		// out.write("\"s".getBytes());
		// out.write(list.get(0)[0].getBytes());
		// out.write("\"".getBytes());
		// out.write(",\"".getBytes());
		// out.write(list.get(0)[1].getBytes());
		// out.write("\"".getBytes());
		// out.write(",".getBytes());
		// String urls = "\"";
		//
		// String type = list.get(0)[1];
		// String aaaa = (string + " " + list.get(0)[2] + "=" + type);
		// for (String[] strings : list) {
		// urls += strings[2] + "[" + strings[1] + "],";
		// if (!type.equals(strings[1])) {
		// aaaa += ("  " + strings[2] + "=" + strings[1]);
		// }
		// }
		// if (aaaa.length() > (string + " " + list.get(0)[2] + "=" + type).length())
		// System.out.println(aaaa);
		// out.write(urls.getBytes());
		// out.write("\"\n".getBytes());
		// }
		// out.close();
	}

	public static void parse(int index) throws Exception {
		ParseSegment app = new ParseSegment();
		Configuration conf = NutchConfiguration.create();
		schedule = FetchScheduleFactory.getFetchSchedule(conf);
		int step = 2;// 1 为抓取搜索入口 2为解析列表地址，抓取列表内容 3为解析简介及详细地址，抓取简介 4抓取详细
		String crawlKey[] = { "ml_prod", "ml_sale", "ml_engine", "ml_holcar", "ml_part" };
		boolean flags[] = new boolean[] { false, false, false, false, false };
		flags[index] = true;
		destTableName = UrlList.langType + "_" + crawlKey[index];

		conf.set(Nutch.CRAWL_ID_KEY, UrlList.langType + "_" + crawlKey[index]);
		UrlList.addUrls(flags);// boolean prods, boolean sales, boolean engine, boolean holcar, boolean part

		DataStore<String, WebPage> store = StorageUtils.createWebStore(conf, String.class, WebPage.class);
		if (store == null)
			throw new RuntimeException("Could not create datastore");
		Query<String, WebPage> query = store.newQuery();
		if ((query instanceof Configurable)) {
			((Configurable) query).setConf(conf);
		}

		Collection<WebPage.Field> fields = Arrays.asList(WebPage.Field.values());
		query.setFields(StorageUtils.toStringArray(fields));
		String startUrlKey = UrlList.getStartKey(index);// "https://www.marklines.com/cn/vehicle_production/";
		String endUrlKey = UrlList.getEndKey(index);// "https://www.marklines.com/cn/vehicle_production/yeaz";

		// startUrlKey;//
		// "http://tech.qq.com/a/20130824/004772.htm";
		query.setStartKey(TableUtil.reverseUrl(startUrlKey));
		query.setEndKey(TableUtil.reverseUrl(endUrlKey));

		UrlList.setUrlConfig(conf);
		UrlList.setSegmentParseRules(conf);

		SegMentParsers parses = new SegMentParsers(conf);
		long curTime = System.currentTimeMillis();
		UrlPathMatch urlcfg = NutchConstant.getUrlConfig(conf);
		boolean filter = conf.getBoolean(GeneratorJob.GENERATOR_FILTER, true);
		boolean normalise = conf.getBoolean(GeneratorJob.GENERATOR_NORMALISE, true);
		if (filter) {
			filters = new URLFilters(conf);
		}
		if (normalise) {
			normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
		}
		urlFilters = new URLFilters(conf);
		curTime = conf.getLong(GeneratorJob.GENERATOR_CUR_TIME, System.currentTimeMillis());

		scoringFilters = new ScoringFilters(conf);
		batchID = NutchConstant.getBatchId(conf);
		batchTime = "";
		ProtocolFactory protocolFactory = new ProtocolFactory(conf);
		skipTruncated = conf.getBoolean(ParserJob.SKIP_TRUNCATED, true);
		parseUtil = new ParseUtil(conf);
		subUrlFilter = new SubURLFilters(conf);

		System.out.println("批次ID：" + batchID + "  进入时间:" + (new Date().toLocaleString()) + " 批次时间：" + batchTime);

		if (index == 4 && step > 1) {
			startUrlKey = UrlList.getStartKey(index, step);
			endUrlKey = UrlList.getEndKey(index, step);
			query.setStartKey(TableUtil.reverseUrl(startUrlKey));
			query.setEndKey(TableUtil.reverseUrl(endUrlKey));
		}
		int retryMax = 3;

		BasicDBObject othTerm = new BasicDBObject();
		if (index == 3) {
			// othTerm.put("baseUrl", "https://www.marklines.com/cn/global/3615");
		} else if (index == 4) {
			if (step == 2) {
				// othTerm.put(
				// "baseUrl",
				// "https://www.marklines.com/cn/supplier_db/partDetail/top?_is=true&containsSub=true&isPartial=false&oth.place%5B%5D=a%2C21&oth.isPartial=true&page=0&size=10");
			} else if (step == 3) {
			} else if (step == 4) {
			}
		}

		((MongoStore) store).setOthterm(othTerm);
		Result<String, WebPage> rs = query.execute();
		while (rs.next()) {
			WebPage page = rs.get();
			MongoUtil mongo = getMongo(destTableName);
			if (checkParsed) {
				BasicDBObject qukey = new BasicDBObject();
				qukey.put("DetailedInformation.SourceURL.PageURL", page.getBaseUrl().toString());
				List<Map> map = mongo.get(qukey);
				if (map.size() == 0) {
					LogUtils.error("page=" + page);
					page.setText(new Utf8("notparsed"));
				}
			}

			String text = "";
			if (page.getText() != null)
				text = page.getText().toString();
			// !text.equals("parsed") &&
			if (!text.equals("null") && !text.equals("notmatch"))//
				parse(store, rs.getKey(), page, index);
		}
		rs.close();
	}
}
