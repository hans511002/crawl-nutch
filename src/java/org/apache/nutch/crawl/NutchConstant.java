package org.apache.nutch.crawl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.util.Utf8;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.nutch.hbase.tool.HBaseTool;
import org.apache.nutch.parse.element.ConfigAttrParses;
import org.apache.nutch.parse.element.ConfigAttrParses.TopicTypeRule;
import org.apache.nutch.parse.element.ExpandAttrsParses;
import org.apache.nutch.parse.element.ExprCalcParses;
import org.apache.nutch.parse.element.ExprCalcParses.SegmentGroupRule;
import org.apache.nutch.parse.element.ExprCalcRule;
import org.apache.nutch.parse.element.PolicticTypeParses;
import org.apache.nutch.parse.element.colrule.ClassRule;
import org.apache.nutch.parse.element.colrule.ColSegmentParse;
import org.apache.nutch.parse.element.colrule.DocumentRule;
import org.apache.nutch.parse.element.colrule.DomListSegmentRule;
import org.apache.nutch.parse.element.colrule.ListAttrListRule;
import org.apache.nutch.parse.element.colrule.ListSegmentRule;
import org.apache.nutch.parse.element.colrule.RegexListRule;
import org.apache.nutch.parse.element.colrule.RegexRule;
import org.apache.nutch.parse.element.colrule.SubAttrListRule;
import org.apache.nutch.parse.element.colrule.SubAttrListRuleTopic;
import org.apache.nutch.parse.element.colrule.SubIndexAttrListRule;
import org.apache.nutch.parse.element.wordfre.WordFreqAttrCalc;
import org.apache.nutch.parse.element.wordfre.WordFreqRule;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageIndex;
import org.apache.nutch.storage.WebPageSegment;
import org.apache.nutch.storage.WebPageSegmentIndex;
import org.apache.nutch.urlfilter.ExpFilter;
import org.apache.nutch.urlfilter.SubURLFilters;
import org.apache.nutch.urlfilter.UrlPathMatch;
import org.apache.nutch.urlfilter.UrlPathMatch.UrlNodeConfig;
import org.apache.nutch.util.GZIPUtils;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.zk.ZooKeeperServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sobey.jcg.support.jdbc.DataAccess;
import com.sobey.jcg.support.jdbc.DataSourceImpl;

public final class NutchConstant {

	public static int exitValue = 0;
	public static String webPageIndex = "webPageIndex";
	public static Set<String> loginoutSet = new HashSet<String>();
	public static String noSolrIndexNode = "step.solrindex.control.key";
	public static String configIdsKey = "batch.config.url.key";
	public static String configIdsNode = "cfgUrlIDs";

	public static enum BatchNode {
		injectNode("inject", "injecting", "injected"), generatNode("generat", "generating", "generated"), fetchNode(
				"fetch", "fetching", "fetched"), parseNode("parse", "parseing", "parsed"), dbUpdateNode("dbupdate",
				"dbupdating", "dbupdated"), solrIndexNode("solrindex", "indexing", "indexed"), segmentParsNode(
				"segmentParse", "segmentParsing", "segmentParsed"), segmentIndexNode("segmentIndex", "segmentIndexing",
				"segmentIndexed"), segmentAdvParseNode("segmentAdvParse", "segmentAdvParsing", "segmentAdvParsed"), segmentExportNode(
				"segmentExport", "segmentExporting", "segmentExported");
		public String nodeName;
		public String beginStatus;
		public String endStatus;

		BatchNode(String nodeName, String beginStatus, String endStatus) {
			this.nodeName = nodeName;
			this.beginStatus = beginStatus;
			this.endStatus = endStatus;
		}

		public String toString() {
			return nodeName + "[" + beginStatus + "," + endStatus + "]";
		}
	}

	public static String hbaseZookeeperQuorum = "hbase.zookeeper.quorum";
	public static final String BATCH_ID_KEY = "nutch.zk.batchid";
	public static final String STEPZKBATCHTIME = "nutch.zk.step.batchtime";
	public static final String STEP_FORCE_RUN_KEY = "nutch.step.force.run";

	public static final String injectorDbUrl = "inject.db.connecturl";
	public static final String injectorDbUser = "inject.db.connectuser";
	public static final String injectorDbPass = "inject.db.connectpass";
	public static final String injectorDbDriverName = "inject.db.driver";
	public static final String injectorSelectSql = "inject.db.selectsql";
	public static final String baseUrlConfigSelectSql = "base.url.config.sql";
	public static final String stepConfigUrl = "step.stepConfigUrl.key";

	public static final String zookeeperRootPath = "nutch.zk.root.node";
	// public static String injectInDateKey = "nutch.zk.inject.lastdate";
	public static final String streamZKstepEnd = "nutch.zk.stream.end";// segmentParse
	public static final String streamZKstepStart = "nutch.zk.stream.start";// generator

	public static final String stepHbaseStartRowKey = "step.hbase.start.rowkey";
	public static final String stepHbaseEndRowKey = "step.hbase.end.rowkey";
	public static final String serialStepKey = "nutch.serial.steps";
	public static final String mapSerialStepKey = "nutch.map.serial.step";
	public static final String reduceSerialStepKey = "nutch.reduce.serial.step";
	public static final String segmentGroupRuleSQL = "segment.group.rule.sql";
	public static final String segmentParseRuleSQL = "segment.parse.rule.sql";
	public static final String segmentParsePolicticSQL = "segment.parse.polictic.sql";
	public static final String segmentParseAttrSQL = "segment.parse.attr.sql";
	public static final String topicTypeIdParseSQL = "topic.parse.rule.sql";

	public static final String segmentRuleSerializKey = "segment.parse.rules.key";
	public static final String segmentRuleBaseConfigSql = "segment.parse.base.config.sql";
	public static final String segmentRuleBaseConfigKey = "segment.parse.base.config.key";
	public static final String wordFriTopNum = "word.freq.topnum";

	public static final String exporterDbUrl = "export.db.connecturl";
	public static final String exporterDbUser = "export.db.connectuser";
	public static final String exporterDbPass = "export.db.connectpass";
	public static final String exporterDbDriverName = "export.db.driver";
	public static final String exporterDbTable = "export.db.table";

	public static final String defaultNutchRootZkPath = "/nutch";
	public static final String serialZkNode = "serial";
	public static final String configDbUrl = "jdbc:oracle:thin:@192.168.11.11:1521:ora10";
	public static final String configDbUser = "hq";
	public static final String configDbPass = "hq";
	public static final String configDbDriverName = "oracle.jdbc.driver.OracleDriver";
	public static String stepZKInDateNode = "indate";
	public static String stepZKOutDateNode = "outdate";
	public static String stepZKOutRowCountNode = "outRowCount";
	public static UrlPathMatch urlcfg = null;
	public static String injectSelectSql = "select BASE_URL, CRAWL_TYPE ,FETCH_INTERVAL,SITE_SORCE ,META_DATA"
			+ ",to_char(MODIFY_DATE,'yyyy-mm-dd hh24:mi:ss') MODIFY_DATE,CONFIG_TYPE "
			+ " from  ERIT_KL_FETCH_BASE_URL where URL_STATE=1 and MODIFY_DATE>to_date('{LASTLOAD_DATE}','yyyy-mm-dd hh24:mi:ss')";
	public static String zkConnectString;
	public static ZooKeeperServer zk = null;
	public static BatchNode curBatchNode = null;
	public static BatchNode prevBatchNode = null;

	public static final Logger LOG = LoggerFactory.getLogger(NutchConstant.class);

	public static String getWebPageUrl(String batchID, String webPageIndexUrl) {
		if (webPageIndexUrl.startsWith(batchID)) {
			String url = webPageIndexUrl.substring(batchID.length());
			return url;
		} else {
			return null;
		}
	}

	// 生成WebPageIndex 的KEY
	public static String getWebPageIndexUrl(String batchID, String webPageUrl) {
		return batchID + webPageUrl;
	}

	public static String getGids(Configuration conf, String Default) throws IOException {
		String gids = getGids(conf);
		if (gids == null)
			return Default;
		gids = gids.substring(1, gids.length() - 1);
		return gids;
	}

	public static String getGids(Configuration conf) throws IOException {
		String batchID = conf.get(BATCH_ID_KEY);
		String batchZkNode = conf.get(NutchConstant.zookeeperRootPath, defaultNutchRootZkPath) + "/" + batchID;
		String batchCfgPath = batchZkNode + "/" + NutchConstant.configIdsNode;
		String cfgIds = conf.get(NutchConstant.configIdsKey);
		if (zk == null)
			zk = new ZooKeeperServer(conf.get(NutchConstant.hbaseZookeeperQuorum));
		try {
			if (cfgIds != null) {
				cfgIds = "," + cfgIds + ",";
				if (zk.exists(batchCfgPath) != null) {
					zk.setData(batchCfgPath, cfgIds);
				} else {
					zk.createPaths(batchCfgPath, cfgIds);
				}
			} else {
				if (zk.exists(batchCfgPath) != null) {
					cfgIds = zk.getString(batchCfgPath);
					if ("".equals(cfgIds))
						cfgIds = null;
					else
						conf.set(NutchConstant.configIdsKey, cfgIds.substring(1, cfgIds.length() - 1));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
		return cfgIds;
	}

	// 读取设置配置抓取地址
	public static void setUrlConfig(Configuration conf, int type) throws IOException {
		// 读取URL配置到HashMap中，序列化到conf中，各map反序列化
		List<String> reverseHosts = new ArrayList<String>();
		DataSourceImpl dataSource = null;
		try {
			dataSource = new DataSourceImpl(conf.get(NutchConstant.injectorDbDriverName, configDbDriverName), conf.get(
					NutchConstant.injectorDbUrl, configDbUrl), conf.get(NutchConstant.injectorDbUser, configDbUser),
					conf.get(NutchConstant.injectorDbPass, configDbPass));
		} catch (SQLException e1) {
			LOG.error("setUrlConfig", e1);
			throw new IOException(e1);
		}
		java.sql.Connection con = null;
		ResultSet rs = null;
		try {
			if (zk == null)
				zk = new ZooKeeperServer(conf.get(NutchConstant.hbaseZookeeperQuorum));
			con = dataSource.getConnection();
			DataAccess access = new DataAccess(con);
			String selectSql = conf.get(NutchConstant.baseUrlConfigSelectSql,
					"select BASE_URL, CRAWL_TYPE ,FETCH_INTERVAL,SITE_SORCE,FETCH_DEPTH"
							+ ",NEED_LOGIN,LOGIN_TYPE,LOGIN_ADDRESS,LOGIN_JS,LOGIN_CLASS"
							+ ",SUB_FILTERS,SUB_FETCH_INTERVAL,a.ROOT_SITE_ID,a.MEDIA_TYPE_ID,a.MEDIA_LEVEL_ID"
							+ ",a.TOPIC_TYPE_ID,a.area_id,CONFIG_TYPE, MATCH_RULE,SUB_RULES"
							+ " ,CONFS,BASE_URL_ID,UPDATE_FILTER,ONCE_FETCH_COUNT,HTTPCLIENT_CONFIG" + ",URL_FORMAT"
							+ " from  ERIT_KL_FETCH_BASE_URL a  where URL_STATE=1 order by BASE_URL ");
			// Name Code Data Type Length Precision Primary Foreign Key Mandatory
			// LOGIN_JS LOGIN_JS text FALSE FALSE FALSE
			// LOGIN_CLASS LOGIN_CLASS varchar(256) 256 FALSE FALSE FALSE
			LOG.info(selectSql);
			rs = access.execQuerySql(selectSql);
			String cfgIds = getGids(conf);
			while (rs.next()) {
				if ((type & 2) != 2 && cfgIds != null && cfgIds.indexOf("," + rs.getString(22) + ",") == -1) {
					continue;
				}
				long cfgType = rs.getLong(18);
				if ((cfgType & 1) != 1) {// CONFIG_TYPE 2:要素解析地址 1:抓取地址 3:1和2
					continue;
				}
				String url = rs.getString(1);
				if (null != cfgIds) {
					try {
						reverseHosts.add(TableUtil.reverseHost(new URL(url).getHost()));
					} catch (Exception e) {
					}
				}
				UrlPathMatch.UrlNodeConfig value = new UrlPathMatch.UrlNodeConfig();
				value.cfgUrlId = rs.getInt(22);
				value.onceCount = rs.getInt(24);

				String httpClientCfg = NutchConstant.filterComment(rs.getString(25));
				if (httpClientCfg != null && !httpClientCfg.trim().equals("")) {
					String[] pars = httpClientCfg.split("\n");
					HashMap<String, String> map = new HashMap<String, String>();
					for (String par : pars) {
						int index = par.indexOf("=");
						if (index != -1) {
							map.put(par.substring(0, index), par.substring(index + 1));
						}
					}
					if (map.size() > 0)
						value.httpClientCfg = map;
					else
						value.httpClientCfg = null;
				}

				String urlFormat = rs.getString(26);
				if (urlFormat != null) {
					String[] tmp = urlFormat.split("~");
					try {
						value.regFormat = new org.apache.nutch.urlfilter.RegexRule(tmp[0], tmp.length > 1 ? tmp[1] : "");
					} catch (Exception exp) {
						value.regFormat = null;
					}
				}
				value.crawlType = rs.getInt(2);
				value.customInterval = rs.getInt(3);
				value.customScore = rs.getFloat(4);
				value.fetchDepth = rs.getInt(5);// http://aa/
				int needLogin = rs.getInt(6);// 是否需要登录
				if (needLogin > 0) {
					value.needLogin = true;
					int loginType = rs.getInt(7);
					value.loginType = loginType;
					if (loginType == 1) {// 服务器1：服务器端登录 2：客户端登录，必需使用代理客户端抓取
						String loginAddress = rs.getString(8);
						decodeLoginAddress(loginAddress, value);
					} else {
						value.crawlType = (value.crawlType & 6);
					}
					value.loginJS = rs.getString(9);
					value.loginCLASS = rs.getString(10);
				}
				String filStr;
				if ((type & 1) == 1) {
					filStr = NutchConstant.filterComment(rs.getString(11));
					LOG.debug("添加generator[" + url + "] 子页面过滤规则:" + filStr);
					value.subFilters = SubURLFilters.buildExp(filStr);
				}
				if ((type & 2) == 2) {
					filStr = NutchConstant.filterComment(rs.getString(23));
					LOG.debug("添加dbupdater[" + url + "] 子页面过滤规则:" + filStr);
					if (value.subFilters == null || value.subFilters.size() == 0) {
						value.subFilters = SubURLFilters.buildExp(filStr);
					} else {
						List<ExpFilter> subFilters = SubURLFilters.buildExp(filStr);
						if (subFilters != null)
							for (ExpFilter ef : subFilters) {
								if (!value.subFilters.contains(ef))
									value.subFilters.add(ef);
							}
					}
				}
				filStr = NutchConstant.filterComment(rs.getString(20));
				LOG.debug("添加[" + url + "] 子页面配置规则(使用配置属性):" + filStr);
				value.subCfgRules = SubURLFilters.buildExp(filStr);

				String confs = rs.getString(21);
				if (confs != null && !confs.trim().equals("")) {
					confs = NutchConstant.filterComment(confs);
					if (confs != null && !confs.trim().equals("")) {
						value.conf = new HashMap<String, String>();
						String[] cfs = confs.trim().split("\n");
						for (int cf = 0; cf < cfs.length; cf++) {
							String[] tmp = cfs[cf].split(":");
							// if (tmp.length == 2)
							value.conf.put(tmp[0], cfs[cf].substring(tmp[0].length() + 1));
						}
					}
				}
				value.subFetchInterval = rs.getInt(12);
				if (value.subFetchInterval == 0)
					value.subFetchInterval = value.customInterval;
				value.rootSiteId = rs.getLong(13);// 配置根地址ID
				value.mediaTypeId = rs.getLong(14);// 媒体类型ID
				value.mediaLevelId = rs.getLong(15);// 媒体级别ID
				value.topicTypeId = rs.getLong(16);// 栏目类型ID
				value.areaId = rs.getLong(17);// 地域ID
				String mr = rs.getString(19);

				// if (mr != null && mr.trim().equals("~"))
				// mr = null;
				LOG.debug("添加[" + url + "] 主机路径匹配规则:" + mr);
				UrlPathMatch.urlSegment.fillSegment(url, value, mr);
			}
			rs.close();
			String serStr = serialObject(UrlPathMatch.urlSegment, true, true);
			//
			// ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			// ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
			// objectOutputStream.writeObject(UrlPathMatch.urlSegment);
			// String serStr = byteArrayOutputStream.toString("ISO-8859-1");
			// serStr = java.net.URLEncoder.encode(serStr, "UTF-8");
			// objectOutputStream.close();
			// byteArrayOutputStream.close();

			conf.set(stepConfigUrl, serStr);
			LOG.info("url config Serializable size:" + serStr.length());
			LOG.debug("set " + stepConfigUrl + "=" + conf.get(stepConfigUrl));
			// GoraMapReduceUtils.setIOSerializations(conf, true);
			// IOUtils.storeToConf(UrlPathMatch.urlSegment, conf, stepConfigUrl);
			UrlPathMatch.urlSegment = null;
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IOException(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e.getMessage());
		} finally {
			if (con != null) {
				try {
					if (rs != null)
						rs.close();
					con.close();
				} catch (SQLException e) {
					LOG.error(e.getMessage());
					e.printStackTrace();
				}
				con = null;
				rs = null;
			}
			if (null != loginoutSet) {
				loginoutSet.clear();
				loginoutSet = null;
			}
		}

		// 设置generat的startkey 和 endkey
		if (reverseHosts.size() > 0 && NutchConstant.BatchNode.generatNode == NutchConstant.curBatchNode) {
			String startkey = reverseHosts.get(0);
			String endKey = reverseHosts.get(0);
			for (String host : reverseHosts) {
				if (host.compareTo(startkey) < 0) {
					startkey = host;
				}
				if (host.compareTo(endKey) > 0) {
					endKey = host;
				}
			}
			conf.set(NutchConstant.stepHbaseStartRowKey, startkey);
			conf.set(NutchConstant.stepHbaseEndRowKey, endKey + "zzzz");
		}
	}

	/**
	 * loginAddress格式：
	 * {"path":{"login":"http://login.baidu.com","validate":"http://login.baidu.com"},"username":{"name":
	 * "UserName","value":"tianyuandike" },"password"
	 * :{"name":"pwd","value":"111111"},"params":{"p3":"p3value","p2":"p2value"
	 * ,"p1":"p1valuesd"},"loginoutpath":{"out1","out2"}}
	 */
	@SuppressWarnings("unchecked")
	public static void decodeLoginAddress(String loginAddress, UrlPathMatch.UrlNodeConfig value)
			throws JsonParseException, JsonMappingException, IOException {
		if (null == loginAddress || loginAddress.trim().length() <= 0) {
			return;
		}
		ObjectMapper mapper = new ObjectMapper();
		Map<?, ?> productMap = mapper.readValue(loginAddress, Map.class);// 转成map
		Map<?, ?> address = (Map<?, ?>) productMap.get("path");
		if (null != address) {
			Object objAddress = address.get("login");
			Object objValidateAddress = address.get("validate");
			value.loginAddress = null == objAddress ? "" : objAddress.toString();
			value.loginValidateAddress = null == objValidateAddress ? "" : objValidateAddress.toString();
		}
		List<String> lst = (List<String>) productMap.get("loginoutpath");
		if (null != lst) {
			List<String> tempLst = new ArrayList<String>();
			String temp = null;
			for (String newStr : lst) {
				temp = null;
				for (String s : loginoutSet) {
					if (s.equals(newStr)) {
						temp = s;
					}
				}

				if (null != temp) {
					tempLst.add(temp);
				} else {
					loginoutSet.add(newStr);
					tempLst.add(newStr);
				}
			}
			value.loginoutAddress = serialObject(tempLst, true, true);
			System.out.println("loginoutAddress=" + lst);
			System.out.println("loginoutAddress=" + value.loginoutAddress);
			System.out.println(deserializeObject(value.loginoutAddress, true, true));
		}

		value.loginParam = (Map<String, String>) productMap.get("params");
		Map<String, String> userName = (Map<String, String>) productMap.get("username");
		if (null != userName) {
			Object objName = userName.get("name");
			Object objValue = userName.get("value");
			String uName = null == objName ? "" : objName.toString();
			String uValue = null == objValue ? "" : objValue.toString();
			value.loginUserName = uValue;
			value.loginParam.put(uName, uValue);
		}

		Map<String, String> password = (Map<String, String>) productMap.get("password");
		if (null != password) {
			Object objName = password.get("name");
			Object objValue = password.get("value");
			String uName = null == objName ? "" : objName.toString();
			String uValue = null == objValue ? "" : objValue.toString();
			value.loginPassword = uValue;
			value.loginParam.put(uName, uValue);
		}
	}

	// 从conf中反解析配置地址树对象
	public static UrlPathMatch getUrlConfig(Configuration conf) throws IOException {
		if (urlcfg == null) {
			String serStr = conf.get(stepConfigUrl);
			if (serStr == null) {
				NutchConstant.setUrlConfig(conf, 3);
				serStr = conf.get(stepConfigUrl);
				if (serStr == null)
					throw new IOException(stepConfigUrl + " in config is null");
			}
			UrlPathMatch.urlSegment = (UrlPathMatch) deserializeObject(serStr, true, true);
			urlcfg = UrlPathMatch.urlSegment;
			//
			// String redStr = java.net.URLDecoder.decode(serStr, "UTF-8");
			// ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(redStr.getBytes("ISO-8859-1"));
			// ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
			// try {
			// urlcfg = (UrlPathMatch) objectInputStream.readObject();
			// } catch (ClassNotFoundException e) {
			// e.printStackTrace();
			// throw new IOException(e);
			// } finally {
			// objectInputStream.close();
			// byteArrayInputStream.close();
			// }
			// urlcfg = (UrlPathMatch) IOUtils.loadFromConf(conf, stepConfigUrl);
			UrlPathMatch.urlSegment = urlcfg;
		}
		return urlcfg;
	}

	// 获取步骤节点输出数据
	public static String getStepNodeOutData(Configuration conf, String stepZKNode, Logger LOG) {
		String batchID = conf.get(BATCH_ID_KEY);
		String batchZkNode = conf.get(NutchConstant.zookeeperRootPath, defaultNutchRootZkPath) + "/" + batchID;
		String stepPath = batchZkNode + "/" + stepZKNode;
		String stepOutPath = stepPath + "/" + NutchConstant.stepZKOutDateNode;// : NutchConstant.stepZKInDateNode);
		String outDate = null;
		try {
			if (zk == null)
				zk = new ZooKeeperServer(conf.get(NutchConstant.hbaseZookeeperQuorum));
			if (zk.exists(stepOutPath) != null)
				outDate = new String(zk.getData(stepOutPath, false));
		} catch (Exception e) {
			LOG.error(e.getMessage());
		} finally {
		}
		return outDate;
	}

	// 获取步骤节点输入数据
	public static String getStepNodeInData(Configuration conf, String stepZKNode, Logger LOG) {
		String batchID = conf.get(BATCH_ID_KEY);
		String batchZkNode = conf.get(NutchConstant.zookeeperRootPath, defaultNutchRootZkPath) + "/" + batchID;
		String stepPath = batchZkNode + "/" + stepZKNode;
		String stepOutPath = stepPath + "/" + NutchConstant.stepZKInDateNode;
		String outDate = null;
		try {
			if (zk == null)
				zk = new ZooKeeperServer(conf.get(NutchConstant.hbaseZookeeperQuorum));
			outDate = new String(zk.getData(stepOutPath, false));
		} catch (IOException e) {
			LOG.error(e.getMessage());
		} catch (KeeperException e) {
			LOG.error(e.getMessage());
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
		}
		return outDate;
	}

	// 获取批次对应的rowkey区间
	public static String[] getBatchRowKeys(String batchID, Logger LOG) {
		// java.text.SimpleDateFormat sdt = new SimpleDateFormat("yyyyMMddHHmmss");
		// Date dt = new Date(batchTime);
		// String batchRowPri = sdt.format(dt);
		// long bt = Long.parseLong(batchRowPri);
		String startKey = batchID;
		String endKey = startKey.substring(0, startKey.length() - 1)
				+ (char) (startKey.charAt(startKey.length() - 1) + 1);
		System.err.println(("============================= startKey=" + startKey + " endKey=" + endKey));
		return new String[] { startKey, endKey };
	}

	// 获取批次生成时间
	public static String getBatchTime(Configuration conf) {
		String batchTime = conf.get(STEPZKBATCHTIME);
		if (batchTime == null || batchTime.equals("")) {
			String startStep = conf.get(streamZKstepStart, BatchNode.generatNode.nodeName);
			if (prevBatchNode != null)
				startStep = prevBatchNode.nodeName;// conf.get(streamZKstepStart, prevBatchNode.nodeName);
			batchTime = getStepNodeOutData(conf, startStep, LOG);
			conf.set(STEPZKBATCHTIME, batchTime);
		}
		return batchTime;
	}

	// 获取批次ID
	public static String getBatchId(Configuration conf) {
		return conf.get(BATCH_ID_KEY);
	}

	// 获取ZK连接串
	public static String getZKConnectString(Configuration conf) {
		return conf.get(NutchConstant.hbaseZookeeperQuorum);
	}

	// 设置批次输出记录数
	private static void setBatchOutRowCount(Configuration conf, BatchNode curNode, boolean inc, long incRowCount)
			throws IOException, KeeperException, InterruptedException {
		String batchID = conf.get(BATCH_ID_KEY);
		if (zk == null)
			zk = new ZooKeeperServer(conf.get(NutchConstant.hbaseZookeeperQuorum));
		String rootZkNode = conf.get(NutchConstant.zookeeperRootPath, defaultNutchRootZkPath);
		String batchZkNode = rootZkNode + "/" + batchID;
		String curStepPath = batchZkNode + "/" + curNode.nodeName;
		String curStepOutPath = curStepPath + "/" + NutchConstant.stepZKOutRowCountNode;
		if (zk.exists(curStepOutPath) != null) {
			if (inc) {
				String str = zk.getString(curStepOutPath, false);
				long rs = Long.parseLong(str);
				rs = (rs + incRowCount);
				LOG.info("步骤输出记录数：" + curStepOutPath + " " + rs + " 当前增量：" + incRowCount);
				zk.setData(curStepOutPath, rs + "");
			} else {
				LOG.info("更新步骤输出记录数：" + curStepOutPath + " " + incRowCount);
				zk.setData(curStepOutPath, incRowCount + "");
			}
		} else {
			LOG.info("创建步骤输出记录数：" + curStepOutPath + " " + incRowCount);
			zk.createPaths(curStepOutPath, incRowCount + "");
		}
	}

	// 获取批次流程中节点上一步骤输出数据记录数
	private static long getBatchLastStepOutRowCount(Configuration conf) throws IOException, KeeperException,
			InterruptedException {
		String batchID = conf.get(BATCH_ID_KEY);
		if (zk == null)
			zk = new ZooKeeperServer(conf.get(NutchConstant.hbaseZookeeperQuorum));
		String rootZkNode = conf.get(NutchConstant.zookeeperRootPath, defaultNutchRootZkPath);
		String batchZkNode = rootZkNode + "/" + batchID;
		String curStepPath = batchZkNode + "/" + prevBatchNode.nodeName;
		String curStepOutPath = curStepPath + "/" + NutchConstant.stepZKOutRowCountNode;
		if (zk.exists(curStepOutPath) != null) {
			String str = zk.getString(curStepOutPath, false);
			return Long.parseLong(str);
		}
		return 0;
	}

	// 步骤节点开始设置
	public static int preparStartJob(Configuration conf, BatchNode stepZKNode, BatchNode preNode, Logger LOG)
			throws Exception {
		return preparStartJob(conf, stepZKNode, preNode, LOG, false, true);
	}

	// 步骤节点开始设置
	public static int preparStartJob(Configuration conf, BatchNode stepZKNode, BatchNode preNode, Logger LOG,
			boolean delete) throws Exception {
		return preparStartJob(conf, stepZKNode, preNode, LOG, delete, true);
	}

	// 步骤节点开始设置
	public static int preparStartJob(Configuration conf, BatchNode curNode, BatchNode preNode, Logger LOG,
			boolean delete, boolean autoStatEndKey) throws Exception {
		String batchID = conf.get(BATCH_ID_KEY);
		String rootZkNode = conf.get(NutchConstant.zookeeperRootPath, defaultNutchRootZkPath);
		String batchZkNode = rootZkNode + "/" + batchID;
		String curStepPath = batchZkNode + "/" + curNode.nodeName;
		String curStepOutPath = curStepPath + "/" + NutchConstant.stepZKOutDateNode;
		String startStep = conf.get(streamZKstepStart, BatchNode.generatNode.nodeName);
		String batchTime;
		try {
			if (conf.get(NutchConstant.hbaseZookeeperQuorum) == null
					|| "".equals(conf.get(NutchConstant.hbaseZookeeperQuorum))) {
				return 1;
			}
			curBatchNode = curNode;
			prevBatchNode = preNode;
			if (zk == null)
				zk = new ZooKeeperServer(conf.get(NutchConstant.hbaseZookeeperQuorum));

			String streamStatus = null;
			if (zk.exists(batchZkNode) != null)
				streamStatus = zk.getString(batchZkNode, false);// 存在流程状态
			String curStepStatus = null;
			if (zk.exists(curStepPath) != null)
				curStepStatus = zk.getString(curStepPath, false);// 存在流程状态
			String perStepPath = null;
			String perStepStatus = null;
			if (preNode != null) {
				perStepPath = batchZkNode + "/" + preNode.nodeName;
				if (zk.exists(perStepPath) != null)
					perStepStatus = zk.getString(perStepPath, false);// 存在流程状态
			}
			LOG.error("流程状态：" + batchZkNode + "=" + streamStatus + " 节点状态:" + curStepPath + "=" + curStepStatus
					+ " 前置节点状态=" + perStepStatus);
			if (perStepPath == null || (perStepStatus != null && perStepStatus.equals(preNode.endStatus))) {
				LOG.info("满足流程状态，执行批次：" + batchID);
				if (curNode.endStatus.equals(curStepStatus)) {
					if (conf.getBoolean(NutchConstant.STEP_FORCE_RUN_KEY, false)) {
						String message = curNode.nodeName + "  已经成功运行过批次：" + batchID + " 强制重处理,";
						if (startStep.equals(curNode.nodeName))
							deleteWebPageIndexData(conf, zk, curStepOutPath, message);
					} else {
						LOG.error(curNode.nodeName + "  已经成功运行过批次：" + batchID + " 请指定 -force参数运行，强制进行重处理  zk path:"
								+ curStepPath);
						return 0;
					}
				} else if (curNode.beginStatus.equals(curStepStatus) || curNode.beginStatus.equals(streamStatus)) {
					if (delete || startStep.equals(curNode.nodeName)) {
						String message = curNode.nodeName + "  已经运行过批次：" + batchID + " 删除老数据,";
						deleteWebPageIndexData(conf, zk, curStepOutPath, message);
					} else {
						if (!conf.getBoolean(NutchConstant.STEP_FORCE_RUN_KEY, false)) {
							String btTime = getBatchTime(conf);
							if (btTime == null) {
								LOG.error(curNode.nodeName + "  已经在运行批次：" + batchID
										+ " 请指定 -force参数运行，强制进行重处理  zk path:" + curStepPath);
								return 0;
							} else {
								java.text.SimpleDateFormat sdt = new SimpleDateFormat("yyyyMMddHHmmss");
								Date dt = sdt.parse(btTime);
								if (System.currentTimeMillis() - dt.getTime() < 3600000) {
									LOG.error(curNode.nodeName + "  正在运行批次：" + batchID
											+ " 请指定 -force参数运行，强制进行重处理  zk path:" + curStepPath);
									return 0;
								}
							}
						}
					}
				} else {
					if (streamStatus != null && preNode != null && !streamStatus.equals(preNode.endStatus)) {
						if (!conf.getBoolean(NutchConstant.STEP_FORCE_RUN_KEY, false)) {
							LOG.warn("批次[" + batchID + "]当前状态[" + streamStatus + "] ");
						}
					}
				}
			} else {
				throw new IOException("批次[" + batchID + "]状态[" + streamStatus + "] 未满足进入状态[" + preNode.endStatus
						+ "]（上一步骤未完成）");
			}
			if (prevBatchNode != null) {
				if (getBatchLastStepOutRowCount(conf) == 0) {
					exitValue = 2;
					throw new IOException("前一步骤[" + prevBatchNode.nodeName + "]输出记录数为0");
				}
			}
			judgeWaitSerialStep(conf, curNode, zk);

			if (curNode.nodeName.equals(startStep)) {// 开始节点
				java.text.SimpleDateFormat sdt = new SimpleDateFormat("yyyyMMddHHmmss");
				Date dt = new Date(System.currentTimeMillis());
				batchTime = sdt.format(dt);
				if (zk.exists(curStepOutPath) != null)
					zk.setData(curStepOutPath, batchTime.getBytes());
				else
					zk.createPaths(curStepOutPath, batchTime);
				conf.set(STEPZKBATCHTIME, batchTime);
			} else {
				String preZkNodePath = batchZkNode + "/" + preNode.nodeName;
				if (zk.exists(preZkNodePath) == null) {
					throw new IOException("前置流程节点[" + preZkNodePath + "]不存在");
				}
				String preStepStatus = zk.getString(preZkNodePath);
				if (!preNode.endStatus.equals(preStepStatus)) {
					throw new IOException("批次[" + batchID + "] 前置流程节点状态[" + preStepStatus + "] 未满足进入状态["
							+ preNode.endStatus + "]（上一步骤未完成）");
				}
				String preZkNodeOutPath = preZkNodePath + "/" + NutchConstant.stepZKOutDateNode;
				if (zk.exists(preZkNodeOutPath) == null) {
					throw new IOException("前置流程输出节点[" + preZkNodeOutPath + "]不存在");
				}
				String prevStepPath = batchZkNode + "/" + prevBatchNode.nodeName;
				String prevStepOutPath = prevStepPath + "/" + NutchConstant.stepZKOutDateNode;
				if (zk.exists(prevStepOutPath) != null) {
					batchTime = zk.getString(prevStepOutPath);
					if (batchTime == null || batchTime.equals("")) {
						throw new IOException("流程初始节点[" + prevStepOutPath + "] output data is null");
					}
					if (zk.exists(curStepOutPath) != null)
						zk.setData(curStepOutPath, batchTime.getBytes());
					else
						zk.createPaths(curStepOutPath, batchTime);
					conf.set(STEPZKBATCHTIME, batchTime);
				} else {
					throw new IOException("流程初始节点[" + prevStepOutPath + "]不存在");
				}
				if (autoStatEndKey) {
					String batchRowKeys[] = getBatchRowKeys(batchID, LOG);
					LOG.info("set hbase query startKey=" + batchRowKeys[0]);
					LOG.info("set hbase query endKey=" + batchRowKeys[1]);
					conf.set(NutchConstant.stepHbaseStartRowKey, batchRowKeys[0]);
					conf.set(NutchConstant.stepHbaseEndRowKey, batchRowKeys[1]);
				}
			}
			if (zk.exists(curStepPath) != null) {
				zk.setData(curStepPath, curNode.beginStatus.getBytes());
			} else {
				zk.createPaths(curStepPath, curNode.beginStatus);
			}
			zk.setData(batchZkNode, curNode.beginStatus.getBytes());
			conf.set(STEPZKBATCHTIME, batchTime);
			LOG.info("批次：" + batchID + " 满足条件，进入状态：" + curNode.beginStatus + " batchTime: " + getBatchTime(conf));
			return 1;
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new IOException("ERROR:" + e.getMessage());
		} catch (KeeperException e) {
			if (e instanceof ConnectionLossException) {
				try {
					zk.close();
				} catch (Exception exx) {
				}
				zk = null;
				return preparStartJob(conf, curNode, preNode, LOG, delete, autoStatEndKey);
			}
			e.printStackTrace();
			throw new IOException("ERROR:" + e.getMessage());
		} finally {
		}
	}

	// 步骤节点串行控制
	private static void judgeWaitSerialStep(Configuration conf, BatchNode curNode, ZooKeeperServer zk)
			throws KeeperException, InterruptedException, IOException {
		String rootZkNode = conf.get(NutchConstant.zookeeperRootPath, defaultNutchRootZkPath);
		String serialStep = conf.get(NutchConstant.serialStepKey, null);
		String batchID = conf.get(BATCH_ID_KEY);
		try {
			if (serialStep != null) {
				String[] serialSteps = serialStep.split(",");
				boolean needLock = false;

				for (String serStep : serialSteps) {
					if (serStep.equals(curNode.nodeName)) {
						needLock = true;
						break;
					}
				}
				if (needLock) {
					String serStepZkNode = rootZkNode + "/" + serialZkNode + "/" + curNode.nodeName;
					String serialStepData = "";
					if (zk.exists(serStepZkNode) != null) {
						serialStepData = zk.getString(serStepZkNode);
						if (serialStepData != null && !serialStepData.startsWith("," + batchID)) {
							serialStepData += batchID + ",";
						} else if (serialStepData == null) {
							serialStepData = "," + batchID + ",";
						}
						zk.setData(serStepZkNode, serialStepData);
					} else {
						LOG.info("节点" + serStepZkNode + "  is null ");
						zk.createPaths(rootZkNode + "/" + serialZkNode, "");
						zk.createPaths(serStepZkNode, "," + batchID + ",");
					}
					String serStepRunZkNode = rootZkNode + "/" + serialZkNode + "/" + curNode.nodeName + "/running";
					LOG.info("judgeWaitSerialStep 当前等待批次队列：" + curNode.nodeName + " => " + serialStepData);
					while (true) {
						if (zk.exists(serStepZkNode) != null) {
							serialStepData = zk.getString(serStepZkNode);
						}
						if (serialStepData == null || serialStepData.trim().equals("")) {
							LOG.info("节点" + serStepZkNode + " data is null ");
							break;
						}
						if (!serialStepData.startsWith("," + batchID + ",")) {
							LOG.info("judgeWaitSerialStep 等待 批次队列：" + curNode.nodeName + " => " + serialStepData);
							Thread.sleep(2000);
							break;
						}
						if (zk.exists(serStepRunZkNode) != null) {
							String runStepData = zk.getString(serStepRunZkNode);
							String[] sts = runStepData.split(":");
							if (sts.length != 2 || !sts[1].equals(curNode.beginStatus)) {
								LOG.info("节点" + serStepRunZkNode + "  数据格式不正确 [" + runStepData + "]");
								try {
									zk.deleteRecursive(serStepRunZkNode);
								} catch (Exception e) {
								}
								continue;
							}
							if (sts[0].equals(batchID))
								continue;
							LOG.info("judgeWaitSerialStep 等待 批次" + curNode.nodeName + " => " + runStepData
									+ " 完成 ZKPATH:" + serStepRunZkNode);
							Thread.sleep(2000);
						} else {
							break;
						}
					}
					if (serialStepData.length() > batchID.length()) {
						if (serialStepData.startsWith("," + batchID)) {
							zk.setData(serStepZkNode, serialStepData.substring(batchID.length() + 1));
						} else if (serialStepData.indexOf("," + batchID) >= 0) {
							zk.setData(
									serStepZkNode,
									serialStepData.substring(serialStepData.indexOf("," + batchID) + batchID.length()
											+ 1));
						}
					} else {
						zk.setData(serStepZkNode, ",");
					}
					zk.createTempNode(serStepRunZkNode, batchID + ":" + curNode.beginStatus);
				}
			}
		} catch (KeeperException e) {
			if (e instanceof ConnectionLossException || e instanceof NodeExistsException) {
				if (e instanceof ConnectionLossException)
					zk = null;
				judgeWaitSerialStep(conf, curNode, zk);
				return;
			}
			e.printStackTrace();
			throw new IOException(e);
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}

	// 删除WebPageIndex数据
	private static void deleteWebPageIndexData(Configuration conf, ZooKeeperServer zk, String stepOutPath,
			String message) throws ClassNotFoundException, IOException, KeeperException, InterruptedException {
		DataStore<String, WebPageIndex> store = StorageUtils.createWebStore(conf, String.class, WebPageIndex.class);
		if (store == null)
			throw new IOException("Could not create datastore [String,WebPageIndex]");
		// if (zk.exists(stepOutPath) == null)
		// return;
		LOG.info(message + " 开始删除老的数据记录");
		conf = HBaseConfiguration.create(conf);
		String batchRowPri = getBatchId(conf);
		if (batchRowPri != null && !batchRowPri.equals("")) {
			// long bt = Long.parseLong(batchRowPri);
			String batchRowKeys[] = getBatchRowKeys(batchRowPri, LOG);
			String htableName = store.getSchemaName();
			LOG.info("begin to delete " + htableName + " record by range [" + batchRowKeys[0] + "," + batchRowKeys[1]
					+ "]");
			HTable table = new HTable(conf, htableName);
			long rowcount = HBaseTool.deleteRange(table, batchRowKeys[0].getBytes(), batchRowKeys[1].getBytes());
			table.close();
			LOG.info("end to delete " + htableName + " record by range [" + batchRowKeys[0] + "," + batchRowKeys[1]
					+ "] delete " + rowcount + " rows ");
		}
		LOG.info(message + " ,完成老数据记录删除");
	}

	// 删除WebPageSegment数据
	private static void deleteWebPageSegmentIndexData(Configuration conf, ZooKeeperServer zk, String stepOutPath,
			String message) throws ClassNotFoundException, IOException, KeeperException, InterruptedException {
		DataStore<String, WebPageSegmentIndex> store = StorageUtils.createWebStore(conf, String.class,
				WebPageSegmentIndex.class);
		if (store == null)
			throw new IOException("Could not create datastore [String,WebPageIndex]");
		// if (zk.exists(stepOutPath) == null)
		// return;
		LOG.info(message + " 开始删除老的数据记录");
		String batchRowPri = getBatchId(conf);
		if (batchRowPri != null && !batchRowPri.equals("")) {
			// long bt = Long.parseLong(batchRowPri);
			conf = HBaseConfiguration.create(conf);
			String batchRowKeys[] = getBatchRowKeys(batchRowPri, LOG);
			String htableName = store.getSchemaName();
			LOG.info("begin to delete " + htableName + " record by range [" + batchRowKeys[0] + "," + batchRowKeys[1]
					+ "]");
			HTable table = new HTable(conf, htableName);
			long rowcount = HBaseTool.deleteRange(table, batchRowKeys[0].getBytes(), batchRowKeys[1].getBytes());
			table.close();
			LOG.info("end to delete " + htableName + " record by range [" + batchRowKeys[0] + "," + batchRowKeys[1]
					+ "] delete " + rowcount + " rows ");
		}
		LOG.info(message + " ,完成老数据记录删除");
	}

	// 判断是否满足抓取周期
	public static boolean checkInterval(UrlNodeConfig value, WebPage page, long curTime) {
		long fetchTime = page.getPrevFetchTime();
		if (fetchTime == 0)
			return true;
		if (page.getFetchInterval() < value.customInterval * 0.5) {
			LOG.info("getBaseUrl:" + page.getBaseUrl() + " FetchInterval is too small, set to " + value.customInterval);
			page.setFetchInterval(value.customInterval);
		}
		int fetchInterval = page.getFetchInterval();
		if (fetchTime <= (curTime - fetchInterval * 1000L)) {
			page.setFetchTime(curTime);
			return true;
		}
		return false;
	}

	// 获取系统当前时间
	public static Long getCurDate() {
		// java.text.SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return System.currentTimeMillis();// sf.format(new Date());
	}

	/**
	 * 需要添加控制，如果是流程的最后节点，删除ZK数据
	 * 
	 * @param conf
	 * @param stepZKNode
	 * @param endStatus
	 * @param LOG
	 * @return
	 * @throws Exception
	 */
	public static int preparEndJob(Configuration conf, BatchNode stepZKNode, Logger LOG) {
		return preparEndJob(conf, stepZKNode, LOG, true);
	}

	public static int preparEndJob(Configuration conf, BatchNode curNode, Logger LOG, boolean updateStreamStatus) {
		String batchID = conf.get(BATCH_ID_KEY);
		String endStep = conf.get(streamZKstepEnd, BatchNode.segmentAdvParseNode.nodeName);
		String rootZkNode = conf.get(NutchConstant.zookeeperRootPath, defaultNutchRootZkPath);
		String batchZkNode = rootZkNode + "/" + batchID;
		String stepPath = batchZkNode + "/" + curNode.nodeName;
		try {
			if (zk == null)
				zk = new ZooKeeperServer(conf.get(NutchConstant.hbaseZookeeperQuorum));
			if (endStep.equals(curNode.nodeName)) {
				String stepOutPath = stepPath + "/" + NutchConstant.stepZKOutDateNode;
				deleteWebPageIndexData(conf, zk, stepOutPath, "完成流程的最后节点处理,删除WebPageIndex");
				deleteWebPageSegmentIndexData(conf, zk, stepOutPath, "完成流程的最后节点处理,删除WebPageSegmentIndex");
				zk.deleteRecursive(batchZkNode);// 删除Zk节点
			} else {
				zk.setData(batchZkNode, curNode.endStatus.getBytes());
				zk.setData(stepPath, curNode.endStatus.getBytes());
			}
			judgeCleanSerialStep(conf, curNode, zk);
			return 1;
		} catch (Exception e) {
			LOG.error(e.getMessage());
		} finally {
			if (zk != null)
				try {
					zk.close();
				} catch (InterruptedException e) {
					LOG.error(e.getMessage());
				}
			zk = null;
		}
		return 1;
	}

	// 步骤节点串行控制清除
	private static void judgeCleanSerialStep(Configuration conf, BatchNode curNode, ZooKeeperServer zk)
			throws KeeperException, InterruptedException, IOException {
		String rootZkNode = conf.get(NutchConstant.zookeeperRootPath, defaultNutchRootZkPath);
		String serialStep = conf.get(NutchConstant.serialStepKey, null);
		String batchID = conf.get(BATCH_ID_KEY);
		if (serialStep != null) {
			String[] serialSteps = serialStep.split(",");
			for (String serStep : serialSteps) {
				if (serStep.equals(curNode.nodeName)) {
					String serStepZkNode = rootZkNode + "/" + serialZkNode + "/" + serStep;
					if (zk.exists(serStepZkNode) != null) {
						String nutchSerialBetch = zk.getString(serStepZkNode);
						if (nutchSerialBetch != null && nutchSerialBetch.trim().equals("")) {
							String[] sts = nutchSerialBetch.split(":");
							if (sts.length != 2 || !sts[0].equals(curNode.beginStatus)) {
								zk.setData(serStepZkNode, "");
							} else if (sts[1].equals(batchID)) {
								LOG.info("批次" + curNode.nodeName + " 完成，修改串行控制状态 ZKPATH:" + serStepZkNode);
								zk.setData(serStepZkNode, "");
							}
						}
						break;
					}
				}
			}
		}
	}

	// public static final String mapSerialStepKey = "nutch.map.serial.step";
	// public static final String reduceSerialStepKey = "nutch.reduce.serial.step";
	// type 0:map 1:reduce
	// 步骤节点MR 串行控制
	public static void setupSerialStepProcess(Configuration conf, BatchNode curNode,
			TaskInputOutputContext<?, ?, ?, ?> context, boolean isMap) throws IOException {
		TaskAttemptID taskAttemptId = context.getTaskAttemptID();
		String taskID = taskAttemptId.getTaskID().toString();
		String taskAttemptID = taskAttemptId.toString();
		if (zk == null)
			zk = new ZooKeeperServer(conf.get(NutchConstant.hbaseZookeeperQuorum));
		try {
			String rootZkNode = conf.get(NutchConstant.zookeeperRootPath, defaultNutchRootZkPath);
			String serialStep = null;
			if (isMap)
				serialStep = conf.get(NutchConstant.mapSerialStepKey, null);
			else
				serialStep = conf.get(NutchConstant.reduceSerialStepKey, null);
			String batchID = conf.get(BATCH_ID_KEY);
			String proNode = isMap ? "map" : "reduce";
			if (serialStep != null) {
				String[] serialSteps = serialStep.split(",");
				for (String serStep : serialSteps) {
					if (serStep.equals(curNode.nodeName)) {
						rootZkNode = rootZkNode + "/" + serialZkNode + "/" + proNode;
						String serStepZkNode = rootZkNode + "/" + taskID;
						String serialStepData = "";
						int count = 0;
						while (true) {
							if (zk.exists(serStepZkNode) != null)
								serialStepData = zk.getString(serStepZkNode);
							else {
								LOG.info("节点" + serStepZkNode + "  is null ");
								break;
							}
							if (serialStepData == null || serialStepData.trim().equals("")) {
								LOG.info("节点" + serStepZkNode + "  data is null ");
								break;
							}
							String[] sts = serialStepData.split(":");
							if (sts.length != 2 || sts[1].indexOf(taskID.substring(5)) < 0) {
								LOG.info("节点" + serStepZkNode + "  数据格式不正确 [" + serialStepData + "]");
								break;
							}
							if (serialStepData.indexOf(taskAttemptID) >= 0) {
								break;
							}
							String msg = "等待 任务" + curNode.nodeName + " => " + serialStepData + " 完成 <br/>更新时间："
									+ (new Date().toLocaleString());
							LOG.info(msg);
							if ((count++ % 10) == 0)
								context.setStatus(msg);
							Thread.sleep(6000);
						}
						if (zk.exists(serStepZkNode) != null) {
							zk.deleteRecursive(serStepZkNode);
							zk.createTempNode(serStepZkNode, batchID + ":" + taskAttemptID);
						} else {
							if (zk.exists(rootZkNode) == null)
								zk.createPaths(rootZkNode, "");
							zk.createTempNode(serStepZkNode, batchID + ":" + taskAttemptID);
						}
						break;
					}
				}
			}
		} catch (KeeperException e) {
			if (e instanceof ConnectionLossException) {
				zk = null;
				setupSerialStepProcess(conf, curNode, context, isMap);
				return;
			}
			e.printStackTrace();
			throw new IOException(e);
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IOException(e);
		} finally {

		}
	}

	// 步骤节点MR 串行控制清除
	public static void cleanupSerialStepProcess(Configuration conf, BatchNode curNode, String taskID, boolean isMap) {
		cleanupSerialStepProcess(conf, curNode, taskID, isMap, false, 0);
	}

	public static void cleanupSerialStepProcess(Configuration conf, BatchNode curNode, String taskID, boolean isMap,
			boolean inc, long incRowCount) {
		try {
			if (zk == null)
				zk = new ZooKeeperServer(conf.get(NutchConstant.hbaseZookeeperQuorum));
			String rootZkNode = conf.get(NutchConstant.zookeeperRootPath, defaultNutchRootZkPath);
			String serialStep = null;
			if (isMap)
				serialStep = conf.get(NutchConstant.mapSerialStepKey, null);
			else
				serialStep = conf.get(NutchConstant.reduceSerialStepKey, null);
			String proNode = isMap ? "map" : "reduce";
			if (serialStep != null) {
				String[] serialSteps = serialStep.split(",");
				for (String serStep : serialSteps) {
					if (serStep.equals(curNode.nodeName)) {
						String serStepZkNode = rootZkNode + "/" + serialZkNode + "/" + proNode + "/" + taskID;
						zk.delete(serStepZkNode);
						break;
					}
				}
			}
			setBatchOutRowCount(conf, curNode, inc, incRowCount);
		} catch (KeeperException e) {
			if (e instanceof ConnectionLossException) {
				zk = null;
				cleanupSerialStepProcess(conf, curNode, taskID, isMap, inc, incRowCount);
				return;
			}
			e.printStackTrace();
			LOG.warn(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			LOG.warn(e.getMessage());
		} finally {
			try {
				if (zk != null)
					zk.close();
			} catch (Exception e) {
			}
			zk = null;
		}
	}

	// 生成内容要素字段解析规则列表
	public static List<ExprCalcRule> bulidExprCalcRules(String colName, String rules,
			HashMap<String, ExprCalcRule> contains) {
		if (rules == null || rules.trim().equals(""))
			return null;
		List<ExprCalcRule> regRules = new ArrayList<ExprCalcRule>();
		String[] tmps = rules.split("\n");
		for (int i = 0; i < tmps.length; i++) {
			if (tmps[i].trim().equals(""))
				continue;
			String rule1 = tmps[i].trim();
			try {
				ExprCalcRule regRule = contains.get(rule1);
				if (regRule == null) {
					if (rule1.startsWith("regex:")) {
						String rule = rule1.substring(6);
						regRule = new RegexRule(rule);
					} else if (rule1.startsWith("regexlist:")) {
						String rule = rule1.substring(10);
						regRule = new RegexListRule(rule);
					} else if (rule1.startsWith("dom:")) {
						String rule = rule1.substring(4);
						regRule = new DocumentRule(rule);
					} else if (rule1.startsWith("class:")) {
						String rule = rule1.substring(6);
						regRule = new ClassRule(rule);
					} else if (rule1.startsWith("domlist:")) {
						String rule = rule1.substring(8);
						regRule = new DomListSegmentRule(rule);
					} else if (rule1.startsWith("list:")) {
						String rule = rule1.substring(5);
						regRule = new ListSegmentRule(rule);
					} else if (rule1.startsWith("subattrlist:")) {
						String rule = rule1.substring("subattrlist:".length());
						regRule = new SubAttrListRule(rule);
					} else if (rule1.startsWith("subattrlisttopic:")) {
						String rule = rule1.substring("subattrlisttopic:".length());
						regRule = new SubAttrListRuleTopic(rule);
					} else if (rule1.startsWith("listattrlist:")) {
						String rule = rule1.substring("listattrlist:".length());
						regRule = new ListAttrListRule(rule);
					} else if (rule1.startsWith("subindexattrlist:")) {
						String rule = rule1.substring("subindexattrlist:".length());
						regRule = new SubIndexAttrListRule(rule);
					} else {
						throw new Exception("The rule is error!");
					}
					contains.put(rule1, regRule);
				}
				regRules.add(regRule);
			} catch (Exception e) {
				NutchConstant.LOG.warn("column:" + colName + "  parse rule error:" + e.getMessage() + " rule:" + rule1);
			}
		}
		if (regRules.size() > 0) {
			return regRules;
		} else {
			return null;
		}
	}

	// 从数据读取内容要素解析规则，并序列化到配置对象中
	public static void setSegmentParseRules(Configuration conf) throws IOException {
		// 读取URL配置到HashMap中，序列化到conf中，各map反序列化
		DataSourceImpl dataSource = null;
		try {
			dataSource = new DataSourceImpl(conf.get(NutchConstant.injectorDbDriverName, configDbDriverName), conf.get(
					NutchConstant.injectorDbUrl, configDbUrl), conf.get(NutchConstant.injectorDbUser, configDbUser),
					conf.get(NutchConstant.injectorDbPass, configDbPass));
		} catch (SQLException e1) {
			LOG.error("setSegmentParseRules", e1);
			throw new IOException(e1);
		}
		java.sql.Connection con = null;
		ResultSet rs = null;
		try {
			// new PluginRepository(conf);
			con = dataSource.getConnection();
			DataAccess access = new DataAccess(con);
			String selectSql = conf.get(NutchConstant.segmentRuleBaseConfigSql,
					"select BASE_URL,CONFIG_TYPE , MATCH_RULE,BASE_URL_ID "
							+ "from  ERIT_KL_FETCH_BASE_URL where URL_STATE=1 order by BASE_URL ");
			LOG.info(selectSql);
			rs = access.execQuerySql(selectSql);
			UrlPathMatch urlSegment = new UrlPathMatch();
			String cfgIds = getGids(conf);
			while (rs.next()) {
				if (cfgIds != null && cfgIds.indexOf("," + rs.getString(4) + ",") == -1) {
					continue;
				}
				String url = rs.getString(1);
				if (url == null)
					continue;
				long cfgType = rs.getLong(2);
				if ((cfgType & 2) != 2) {// CONFIG_TYPE 2:要素解析地址 1:抓取地址 3:1和2
					continue;
				}
				String mr = rs.getString(3);
				// UrlPathMatch.AttrIdsConfig value = new UrlPathMatch.AttrIdsConfig();
				// value.rootSiteId = rs.getLong(2);
				// value.mediaTypeId = rs.getLong(3);
				// value.mediaLevelId = rs.getLong(4);
				// value.topicTypeId = rs.getLong(5);// http://aa/
				// value.areaId = rs.getLong(5);// http://aa/
				urlSegment.fillSegment(url, null, mr);
			}
			rs.close();
			HashMap<String, ColSegmentParse> regParContains = new HashMap<String, ColSegmentParse>();
			HashMap<String, ExprCalcRule> contains = new HashMap<String, ExprCalcRule>();
			// ConfigAttrParses.topicTypeRules
			// 加载栏目识别规则
			selectSql = conf
					.get(NutchConstant.topicTypeIdParseSQL,
							" SELECT a.PARENT_TOPIC_TYPE_ID,topic_type_id,topic_parse_rule,a.TOPIC_TYPE_NAME"
									+ " FROM ERIT_KL_TOPIC_TYPE a WHERE a.topic_parse_rule IS NOT NULL OR "
									+ " EXISTS (SELECT 1 FROM ERIT_KL_TOPIC_TYPE b WHERE b.PARENT_TOPIC_TYPE_ID= a.TOPIC_TYPE_ID )"
									+ " ORDER BY a.ORDER_ID,a.MEDIA_TYPE_ID,a.PARENT_TOPIC_TYPE_ID,a.TOPIC_TYPE_ID ");
			LOG.info(selectSql);
			rs = access.execQuerySql(selectSql);
			ConfigAttrParses.TopicTypeRule tprule = null;
			ConfigAttrParses.topicTypeRules = new HashMap<Long, TopicTypeRule>();
			while (rs.next()) {
				Long parid = rs.getLong(1);
				Long tpid = rs.getLong(2);
				String rules = rs.getString(3);
				rules = filterComment(rules);
				if (rules == null || rules.trim().equals(""))
					continue;
				ColSegmentParse parse = regParContains.get(rules);
				tprule = new TopicTypeRule();
				tprule.topicTypeId = tpid;

				if (parse == null) {
					String colName = WebPageSegment.topicTypeIdColName.toString();
					LOG.debug("添加[" + rs.getString(4) + "]栏目字段" + colName + " 解析规则:" + rules);
					List<ExprCalcRule> regRules = bulidExprCalcRules(colName, rules, contains);
					if (regRules == null) {
						NutchConstant.LOG.warn("column:" + colName + "  no right parse rule");
						continue;
					} else {
						parse = new ColSegmentParse(new Utf8(colName), regRules);
						regParContains.put(rules, parse);
						tprule.parse = parse;
					}
				} else {
					tprule.parse = parse;
				}
				if (parid != 0) {
					TopicTypeRule parRule = ConfigAttrParses.topicTypeRules.get(parid);
					if (parRule != null && !parRule.subRule.contains(tprule)) {
						parRule.subRule.add(tprule);
					}
				}
				ConfigAttrParses.topicTypeRules.put(tpid, tprule);
			}
			rs.close();
			// String serStr = serialObject(UrlPathMatch.urlSegment, true, true);
			String serStr = serialObject(new Object[] { urlSegment, ConfigAttrParses.topicTypeRules }, true, true);
			urlSegment = null;
			// ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			// ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
			// objectOutputStream.writeObject(org.apache.nutch.parse.element.ProidsParses.baseUrlAttr);
			// String serStr = byteArrayOutputStream.toString("ISO-8859-1");
			// serStr = java.net.URLEncoder.encode(serStr, "UTF-8");
			// objectOutputStream.close();
			// byteArrayOutputStream.close();
			conf.set(segmentRuleBaseConfigKey, serStr);
			LOG.info("segment attrs Serializable size:" + serStr.length());
			LOG.debug("set " + segmentRuleBaseConfigKey + "=" + conf.get(segmentRuleBaseConfigKey));
			ConfigAttrParses.baseUrlAttr = null;
			ConfigAttrParses.topicTypeRules = null;

			// 要素分组规则
			ExprCalcParses.cfgSegGroupColRuleParses = new HashMap<String, List<SegmentGroupRule>>();
			selectSql = conf.get(NutchConstant.segmentGroupRuleSQL,
					" SELECT g.SEGMENT_GROUP_ID,g.SEGMENT_GROUP_NAME,g.SEGMENT_GROUP_URL_RULE,a.BASE_URL"
							+ " FROM ERIT_KL_BASE_SEGMENT_GROUP g INNER JOIN ERIT_KL_FETCH_BASE_URL a  "
							+ " ON a.BASE_URL_ID=g.BASE_URL_ID AND a.URL_STATE=1 "
							+ "  ORDER BY g.BASE_URL_ID,g.SEGMENT_GROUP_ID");
			rs = access.execQuerySql(selectSql);
			while (rs.next()) {
				long groupId = rs.getLong(1);
				String groupName = rs.getString(2);
				String groupRule = rs.getString(3);
				String config = rs.getString(4);
				if (config.charAt(config.length() - 1) == '/') {
					config = config.substring(0, config.length() - 1);
				}
				List<SegmentGroupRule> groupRules = ExprCalcParses.cfgSegGroupColRuleParses.get(config);
				if (groupRules == null) {
					groupRules = new ArrayList<SegmentGroupRule>();
					ExprCalcParses.cfgSegGroupColRuleParses.put(config, groupRules);
				}
				SegmentGroupRule sgrule = new SegmentGroupRule();
				sgrule.groupId = groupId;
				sgrule.groupName = groupName;
				if (groupRule != null && groupRule.trim().equals("") == false
						&& groupRule.trim().startsWith("#") == false)
					sgrule.pattern = Pattern.compile(groupRule.trim());
				LOG.debug("添加 configUrl:" + config + " 分组:" + groupName + " 解析规则：" + groupRule);
				groupRules.add(sgrule);
			}
			// 要素表达式规则
			selectSql = conf
					.get(NutchConstant.segmentParseRuleSQL,
							"SELECT a.BASE_URL,b.SEGMENT_COL_NAME,b.SEGMENT_RULES,b.SEGMENT_GROUP_ID,b.BASE_SEGMENT_ID FROM ERIT_KL_FETCH_BASE_URL a "
									+ "INNER JOIN ERIT_KL_BASE_SEGMENT b ON a.BASE_URL_ID=b.BASE_URL_ID "
									+ "WHERE a.URL_STATE=1 ORDER BY a.BASE_URL,b.SEGMENT_COL_NAME ");
			rs = access.execQuerySql(selectSql);
			// ExprCalcParses.cfgSegGroupColRuleParses = new HashMap<String, List<ColSegmentParse>>();
			while (rs.next()) {
				String config = rs.getString(1);
				String colName = rs.getString(2);
				String rules = rs.getString(3);
				long groupId = rs.getLong(4);
				String baseSegId = rs.getLong(5) + "";
				if (config.charAt(config.length() - 1) == '/') {
					config = config.substring(0, config.length() - 1);
				}
				SegmentGroupRule curGroupRule = null;
				List<SegmentGroupRule> groupRules = ExprCalcParses.cfgSegGroupColRuleParses.get(config);
				if (groupRules == null) {// 只有groupId==0 才为空
					groupRules = new ArrayList<SegmentGroupRule>();
					ExprCalcParses.cfgSegGroupColRuleParses.put(config, groupRules);
					curGroupRule = new SegmentGroupRule();
					curGroupRule.groupId = -1;
					curGroupRule.groupName = "无分组";
					curGroupRule.pattern = null;
					groupRules.add(curGroupRule);
				} else {
					if (groupId == 0) {
						curGroupRule = groupRules.get(0);
					} else {
						for (SegmentGroupRule grule : groupRules) {
							if (grule.groupId == groupId) {
								curGroupRule = grule;
								break;
							}
						}
					}
					if (curGroupRule == null) {//
						LOG.error("configUrl:" + config + " 字段：" + colName + " 对应分组ID:" + groupId + " 不存在  解析规则："
								+ rules);
						continue;
					}
				}
				rules = filterComment(rules);
				ColSegmentParse parse = regParContains.get(baseSegId);
				if (curGroupRule.segmentRuls == null) {
					curGroupRule.segmentRuls = new ArrayList<ColSegmentParse>();
				}
				List<ColSegmentParse> listExp = curGroupRule.segmentRuls;
				if (parse == null) {
					LOG.debug(config + " 添加字段" + colName + " 解析规则:" + rules);
					List<ExprCalcRule> regRules = bulidExprCalcRules(colName, rules, contains);
					if (regRules == null) {
						NutchConstant.LOG.warn("column:" + colName + "  no right parse rule");
					} else {
						parse = new ColSegmentParse(new Utf8(colName), regRules);
						regParContains.put(baseSegId, parse);
						listExp.add(parse);
					}
				} else {
					listExp.add(parse);
				}
			}
			rs.close();
			contains.clear();
			regParContains.clear();
			// 词频规则，主题规则 信息正负面规则
			HashMap<String, WordFreqRule> wordRuleContains = new HashMap<String, WordFreqRule>();

			selectSql = conf.get(NutchConstant.segmentParsePolicticSQL,
					"SELECT a.POLITIC_TYPE_ID,a.POLITIC_TYPE_KEY_RULES FROM   ERIT_KL_POLITIC_TYPE a ");
			rs = access.execQuerySql(selectSql);
			PolicticTypeParses.rules = new WordFreqRule[2];
			PolicticTypeParses.rules[0] = null;
			PolicticTypeParses.rules[1] = null;
			if (rs.next()) {
				int polId = rs.getInt(1);
				String rule = rs.getString(2);
				try {
					PolicticTypeParses.rules[0] = new WordFreqAttrCalc(WordFreqRule.ParseRuleType.POLICITIC_TYPE,
							polId, rule);
					wordRuleContains.put(rule, PolicticTypeParses.rules[0]);
				} catch (Exception e) {
					LOG.error("正负面规则配置错误：" + e.getMessage());
				}
				if (rs.next()) {
					polId = rs.getInt(1);
					rule = rs.getString(2);
					try {
						PolicticTypeParses.rules[1] = new WordFreqAttrCalc(WordFreqRule.ParseRuleType.POLICITIC_TYPE,
								polId, rule);
						wordRuleContains.put(rule, PolicticTypeParses.rules[1]);
					} catch (Exception e) {
						LOG.error("正负面规则配置错误：" + e.getMessage());
					}
				}
			}
			rs.close();
			selectSql = conf.get(NutchConstant.segmentParseAttrSQL,
					"SELECT a.ATTR_ID,a.ATTR_TYPE_KEY_RULES FROM  ERIT_KL_PAGE_ATTRS a ");
			rs = access.execQuerySql(selectSql);
			ExpandAttrsParses.rules = new ArrayList<WordFreqRule>();
			while (rs.next()) {
				int attrId = rs.getInt(1);
				String rule = rs.getString(2);
				try {
					PolicticTypeParses.rules[1] = new WordFreqAttrCalc(WordFreqRule.ParseRuleType.ATTR_TYPE, attrId,
							rule);
					wordRuleContains.put(rule, PolicticTypeParses.rules[1]);
				} catch (Exception e) {
					LOG.error("属性判断规则配置错误：" + e.getMessage());
				}
			}
			rs.close();
			serStr = serialObject(new Object[] { ExprCalcParses.cfgSegGroupColRuleParses, PolicticTypeParses.rules,
					ExpandAttrsParses.rules }, true, true);
			conf.set(segmentRuleSerializKey, serStr);
			LOG.info("segment parse rules Serializable size:" + serStr.length());
			LOG.debug("set " + segmentRuleSerializKey + "=" + conf.get(segmentRuleSerializKey));
			ExprCalcParses.cfgSegGroupColRuleParses.clear();
			ExprCalcParses.cfgSegGroupColRuleParses = null;
			PolicticTypeParses.rules = null;
			ExpandAttrsParses.rules.clear();
			ExpandAttrsParses.rules = null;
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IOException(e.getMessage());
		} finally {
			if (con != null) {
				try {
					if (rs != null)
						rs.close();
					con.close();
				} catch (SQLException e) {
					LOG.error(e.getMessage());
					e.printStackTrace();
				}
				con = null;
				rs = null;
			}
		}
	}

	// 从配置对象中反解析内容要素解析规则
	public static void getSegmentParseRules(Configuration conf) throws IOException {
		if (ConfigAttrParses.baseUrlAttr == null) {
			String serStr = conf.get(segmentRuleBaseConfigKey);
			if (serStr == null) {
				setSegmentParseRules(conf);
				serStr = conf.get(segmentRuleBaseConfigKey);
				if (serStr == null)
					throw new IOException(segmentRuleBaseConfigKey + " in config is null");
			}
			Object[] obj = (Object[]) deserializeObject(serStr, true, true);
			ConfigAttrParses.baseUrlAttr = (UrlPathMatch) obj[0];
			ConfigAttrParses.topicTypeRules = (HashMap<Long, TopicTypeRule>) obj[1];
		}
		if (ExprCalcParses.cfgSegGroupColRuleParses == null) {
			String serStr = conf.get(segmentRuleSerializKey);
			if (serStr == null) {
				setSegmentParseRules(conf);
				serStr = conf.get(segmentRuleSerializKey);
				if (serStr == null)
					throw new IOException(segmentRuleSerializKey + " in config is null");
			}
			WordFreqRule.wordTopNum = conf.getInt(wordFriTopNum, WordFreqRule.wordTopNum);
			Object[] obj = (Object[]) deserializeObject(serStr, true, true);
			ExprCalcParses.cfgSegGroupColRuleParses = (HashMap<String, List<SegmentGroupRule>>) obj[0];
			PolicticTypeParses.rules = (WordFreqRule[]) obj[1];
			ExpandAttrsParses.rules = (ArrayList<WordFreqRule>) obj[2];
		}
	}

	// 过滤规则中的注释
	public static String filterComment(String rules) {
		if (rules == null || rules.equals(""))
			return null;
		String[] tmps = rules.replaceAll("\\r\\n", "\n").split("\n");
		StringBuffer buf = new StringBuffer();
		for (String tmp : tmps) {
			if (tmp.trim().equals("") || tmp.trim().charAt(0) == '#')
				continue;
			buf.append(tmp.trim());
			buf.append("\n");
		}
		return buf.toString();
	}

	/**
	 * 将对象序列化成字符串
	 * 
	 * @param obj
	 * @return
	 * @throws IOException
	 */
	public static String serialObject(Object obj) throws IOException {
		return serialObject(obj, false, false);
	}

	public static String serialObject(Object obj, boolean isGzip, boolean urlEnCode) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
		objectOutputStream.writeObject(obj);
		String serStr = null;
		byte[] bts = null;
		if (isGzip) {
			bts = GZIPUtils.zip(byteArrayOutputStream.toByteArray());
		} else {
			bts = byteArrayOutputStream.toByteArray();
		}
		if (urlEnCode) {
			serStr = new String(org.apache.commons.codec.binary.Base64.encodeBase64(bts), "ISO-8859-1");
		} else {
			serStr = new String(bts, "ISO-8859-1");
		}
		objectOutputStream.close();
		byteArrayOutputStream.close();
		return serStr;
	}

	/**
	 * 反序列化对象
	 * 
	 * @param serStr
	 * @return
	 * @throws IOException
	 */
	public static Object deserializeObject(String serStr) throws IOException {
		return deserializeObject(serStr, false, false);
	}

	public static Object deserializeObject(String serStr, boolean isGzip, boolean urlEnCode) throws IOException {
		byte[] bts = null;
		if (urlEnCode) {
			bts = org.apache.commons.codec.binary.Base64.decodeBase64(serStr.getBytes("ISO-8859-1"));
		} else {
			bts = serStr.getBytes("ISO-8859-1");
		}
		if (isGzip)
			bts = GZIPUtils.unzip(bts);
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bts);
		ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
		try {
			return objectInputStream.readObject();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new IOException(e);
		} finally {
			objectInputStream.close();
			byteArrayInputStream.close();
		}
	}

	public static final Pattern valPartsRegex = Pattern.compile("\\$(\\d+)");

	// 正则表达式的定向替换
	public static String ReplaceRegex(Matcher m, String substitution) {
		try {
			Matcher vm = valPartsRegex.matcher(substitution);
			String val = substitution;
			String regpar = substitution;
			int gl = m.groupCount();
			while (vm.find()) {
				regpar = regpar.substring(vm.end());
				int g = Integer.parseInt(vm.group(1));
				if (g > gl) {
					val = val.replaceAll("\\$\\d", "");
					break;
				}
				String gv = m.group(Integer.parseInt(vm.group(1)));
				if (gv != null)
					if (val.equals("$" + g)) {
						val = gv;
					} else {
						val = val.replaceAll("\\$" + g, gv);
					}
				else
					val = val.replaceAll("\\$" + g, "");
				vm = valPartsRegex.matcher(regpar);
			}
			return val;
		} catch (Exception e) {
			return null;
		}
	}

	public static String getUrlPath(String url) {
		URL r;
		try {
			r = new URL(url);
			String protocol = r.getProtocol();
			String host = r.getHost();
			String path = r.getPath();
			// host = TableUtil.reverseHost(host);
			return protocol + "://" + host + path;
		} catch (MalformedURLException e) {
			return null;
		}
	}

	public static String Join(Object[] objs, String sp) {
		StringBuilder sb = new StringBuilder();
		for (Object obj : objs) {
			sb.append(obj + sp);
		}
		String res = sb.toString();
		return res.substring(0, res.length() - sp.length());
	}

	public static void main(String args[]) throws Exception {

		String url = "http://news.qq.com/a/20120801/001870.htm";
		Pattern pt = Pattern
				.compile("(?im)<div class=\"picCenterTitle\"><a .*?href=\"(.*)?\" target=\"_blank\">.*?<span class=\"status\">(.*?)</span>((.|\\s)*?)</a></div>");

		String cnt = "<li> <div class=\"picCenter\"><a onfocus=\"this.blur();\" hidefocus=\"true\" href=\"/bj_9449/disp_pic.shtml#type=1&page=1&pid=144861\" "
				+ "target=\"_blank\"><img onload=\"onHXImgLoad(this);\" "
				+ " src=\"http://p3.qpic.cn/estate/0/03834cf865f048599b1e1899d057c63b.jpg/450\" "
				+ "bosszone = \"typepic\" /></a></div><div class=\"picCenterTitle\"><a "
				+ "onfocus=\"this.blur();\" hidefocus=\"true\" style=\"height:282px;display:inline-block;\""
				+ " href=\"/bj_9449/disp_pic.shtml#type=1&page=1&pid=144861\" target=\"_blank\"> <span class=\"status\">【在售】</span><span title=\"华业东方玫瑰A8-2-02户型 2室2厅1卫1厨 91.00㎡\">华业东方玫瑰A8-2-02户型 2室2厅1卫1厨</span>… </a></div> </li>                    <li> <div class=\"picCenter\"><a onfocus=\"this.blur();\" hidefocus=\"true\" href=\"/bj_9449/disp_pic.shtml#type=1&page=1&pid=144860\" target=\"_blank\"><img onload=\"onHXImgLoad(this);\"  src=\"http://p3.qpic.cn/estate/0/96031f1b5f8beb75bcfc1d10eaca0663.jpg/450\" bosszone = \"typepic\" /></a></div><div class=\"picCenterTitle\"><a onfocus=\"this.blur();\" hidefocus=\"true\" style=\"height:282px;display:inline-block;\" href=\"/bj_9449/disp_pic.shtml#type=1&page=1&pid=144860\" target=\"_blank\"> <span class=\"status\">【在售】</span><span title=\"华业东方玫瑰A8-2-01户型 2室2厅1卫1厨 89.00㎡\">华业东方玫瑰A8-2-01户型 2室2厅1卫1厨</span>… </a></div> </li> <Div Class=\"Piccentertitle\"><A Onfocus=\"This.Blur();\" Hidefocus=\"True\" Style=\"Height:282Px;Display:Inline-Block;\" Href=\"/Bj_9449/Disp_Pic.Shtml#Type=1&Page=1&Pid=1711\" Target=\"_Blank\"> <Span Class=\"Status\"></Span>华业东方玫瑰B6-2-6户型 </a></div> </li>                    <li> <div class=\"picCenter\"><a onfocus=\"this.blur();\" hidefocus=\"true\" href=\"/bj_9449/disp_pic.shtml#type=1&page=1&pid=1710\" target=\"_blank\"><img onload=\"onHXImgLoad(this);\"  src=\"http://p1.qpic.cn/estate/0/b99ab55acd56d4342d0e43ff9f646c6d.jpg/450\" bosszone = \"typepic\" /></a></div><div class=\"picCenterTitle\"><a onfocus=\"this.blur();\" hidefocus=\"true\" style=\"height:282px;display:inline-block;\" href=\"/bj_9449/disp_pic.shtml#type=1&page=1&pid=1710\" target=\"_blank\"> <span class=\"status\"></span>华业东方玫瑰A4-1-2户型 </a></div> </li>";
		// cnt = "<span class=\"arial20_red\">6500 </span>";
		// pt = Pattern.compile("\\s*(.*)?<span class=\"arial20_red\">(.*?)</span>(.*?)\\s*<");
		Matcher m = pt.matcher(cnt);
		// System.out.println(NutchConstant.ReplaceRegex(m, "$1:$2:$3"));
		while (m.find()) {
			cnt = cnt.substring(m.end());
			System.out.println("value=" + NutchConstant.ReplaceRegex(m, "$2:$3:$4:$5:$6:$7:$8"));
			m = pt.matcher(cnt);
		}
		if (pt != null)
			return;
		Configuration conf = NutchConfiguration.create();
		NutchConstant.setUrlConfig(conf, 3);
		UrlPathMatch urlCfg = NutchConstant.getUrlConfig(conf);
		SubURLFilters subUrlFilter = new SubURLFilters(conf);
		System.err.println(subUrlFilter.filter(url));

		UrlPathMatch urlSegment = urlCfg.match(url);
		UrlNodeConfig value = (UrlNodeConfig) urlSegment.getNodeValue();
		String configUrl = urlSegment.getConfigPath();

		System.err.println(configUrl);
	}
}
