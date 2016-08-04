package org.apache.nutch.hbase.tool;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraInputSplit;
import org.apache.gora.mapreduce.GoraMapper;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.DbUpdaterJob;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.NutchConstant;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.urlfilter.SubURLFilters;
import org.apache.nutch.urlfilter.UrlPathMatch;
import org.apache.nutch.urlfilter.UrlPathMatch.UrlNodeConfig;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.ToolUtil;
import org.apache.nutch.zk.ZooKeeperServer;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HbaseUtil extends NutchTool implements Tool {

	static String ARGS_cmdType = "ARGS_cmdType";
	static String ARGS_subCmdType = "ARGS_subCmdType";
	static String ARGS_tableName = "ARGS_tableName";
	static String ARGS_params = "ARGS_params";
	static ZooKeeperServer zk = null;
	static Configuration conf = NutchConfiguration.create();
	public static final Logger LOG = LoggerFactory.getLogger(FetcherJob.class);
	private static final Set<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

	static {
		FIELDS.add(WebPage.Field.FETCH_TIME);
		FIELDS.add(WebPage.Field.RETRIES_SINCE_FETCH);
		FIELDS.add(WebPage.Field.PREV_FETCH_TIME);
		FIELDS.add(WebPage.Field.SCORE);
		FIELDS.add(WebPage.Field.STATUS);
		FIELDS.add(WebPage.Field.MARKERS);
		FIELDS.add(WebPage.Field.FETCH_INTERVAL);
		FIELDS.add(WebPage.Field.CRAWLTYPE);
		FIELDS.add(WebPage.Field.CONFIGURL);
	}

	public static void print(String parNode, List<String> nodePaths) throws KeeperException, InterruptedException, IOException {
		if (zk == null)
			zk = new ZooKeeperServer(conf.get(NutchConstant.hbaseZookeeperQuorum));
		for (String inode : nodePaths) {
			String iparNode = parNode + "/" + inode;
			System.out.println(iparNode + "=" + zk.getString(iparNode));
			List<String> inodePaths = zk.getChildren(iparNode, false);
			if (inodePaths.size() > 0)
				print(iparNode, inodePaths);
		}
	}

	public static String bytesToHexString(byte[] src) {
		StringBuilder stringBuilder = new StringBuilder("");
		if (src == null || src.length <= 0) {
			return null;
		}
		for (int i = 0; i < src.length; i++) {
			int v = src[i] & 0xFF;
			String hv = Integer.toHexString(v);
			if (hv.length() < 2) {
				stringBuilder.append(0);
			}
			stringBuilder.append(hv);
		}
		return stringBuilder.toString();
	}

	public static byte[] hexStringToBytes(String hexString) {
		if (hexString == null || hexString.equals("")) {
			return null;
		}
		hexString = hexString.toUpperCase();
		int length = hexString.length() / 2;
		char[] hexChars = hexString.toCharArray();
		byte[] d = new byte[length];
		for (int i = 0; i < length; i++) {
			int pos = i * 2;
			d[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
		}
		return d;
	}

	private static byte charToByte(char c) {
		return (byte) "0123456789ABCDEF".indexOf(c);
	}

	public static String Join(Object[] o) {
		return Join(o, ",");
	}

	public static String Join(Object[] o, String sp) {
		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (Object c : o) {
			if (i++ > 0)
				sb.append(sp);
			sb.append(c.toString());
		}
		return sb.toString();
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(NutchConfiguration.create(), new HbaseUtil(), args);
		System.exit(res);
	}

	@Override
	public Map<String, Object> run(Map<String, Object> args) throws Exception {
		int cmdType = Integer.parseInt(args.get(ARGS_cmdType).toString());// 1 delete 2 drop
		int subCmdType = Integer.parseInt(args.get(ARGS_subCmdType).toString());// 1 row 2scan 3cfg
		String tableName = args.get(ARGS_tableName).toString();
		String[] params = (String[]) args.get(ARGS_params);
		Map<String, Object> res = new HashMap<String, Object>();
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = null;
		HTable table = null;
		try {
			if (subCmdType != 3) {
				admin = new HBaseAdmin(conf);
				table = new HTable(conf, tableName);
			}
			String usage = "Usage: hbase <cmd> <tablename> [params] cmd and params As follows \n"
					+ "del tablename -row rowkey 'c1,list'   : delete one Specified row ,example del -row xxxx \n"
					+ "del tablename -scan startkey endkey 'c1,list' : delete Multi-line from scan by range  \n"
					+ "del tablename -cfg cfgids/all : delete not match cfgids subrules rows \n"
					+ "drop tablename  ColumnFamilys list : drop ColumnFamily from table  \n";
			if (cmdType == 1) {
				if (params == null || params.length == 0) {
					System.out.println(usage);
					throw new Exception("cmd params is null");
				}
				if (subCmdType == 1) {// row
					String rowKey = params[0];
					String cols = "";
					if (params.length > 1)
						cols = params[1];
					String[] cfs = cols.split(",");
					Delete d = new Delete(rowKey.getBytes());
					for (String cf : cfs) {
						String[] tmp = cf.split(":");
						if (tmp[0].equals(""))
							continue;
						if (tmp.length == 2) {
							d.deleteColumn(tmp[0].getBytes(), tmp[1].getBytes());
						} else {
							d.deleteFamily(tmp[0].getBytes());
						}
					}
					table.delete(d);
				} else if (subCmdType == 2) {// scan
					if (params.length < 2) {
						System.out.println(usage);
						throw new Exception("cmd params is null");
					}
					String startKey = params[0];
					String endKey = params[1];
					Scan scan = new Scan();
					scan.setStartRow(startKey.getBytes());
					scan.setStopRow(endKey.getBytes());
					String cols = "";
					if (params.length > 1)
						cols = params[1];
					String[] cfs = cols.split(",");
					for (String cf : cfs) {
						String[] tmp = cf.split(":");
						if (tmp[0].equals(""))
							continue;
						if (tmp.length == 2) {
							scan.addColumn(tmp[0].getBytes(), tmp[1].getBytes());
						} else {
							scan.addFamily(tmp[0].getBytes());
						}
					}
					HBaseTool.deleteRange(table, scan, cfs);
				} else if (subCmdType == 3) {// cfg
					// del tablename -cfg cfgids/all
					if (!params[0].toLowerCase().equals("all"))
						getConf().set(NutchConstant.configIdsKey, params[0]);
					NutchConstant.setUrlConfig(this.getConf(), 3);
					String gids = getConf().get(NutchConstant.configIdsKey, "all");
					currentJob = new NutchJob(getConf(), "delete not match row[" + gids + "]");
					StorageUtils.initMapperJob(currentJob, FIELDS, WebPage.class, String.class, WebPage.class, DeleteMapper.class);
					currentJob.setNumReduceTasks(0);
					currentJob.waitForCompletion(true);
					ToolUtil.recordJobStatus(null, currentJob, results);
					results.put("result", "webpage row not in config deleted");
					return results;
				}

			} else if (cmdType == 2) {
				if (!admin.tableExists(tableName)) {
					res.put("result", "table don't Exists");
					return res;
				}
				if (params == null || params.length == 0) {// drop table
					admin.deleteTable(tableName);
					res.put("result", "table droped");
				} else {
					for (String columnName : params)
						admin.deleteColumn(tableName, columnName);
					res.put("result", "table ColumnFamilys[" + Join(params) + "] droped");
				}
			}
			return res;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (table != null)
				table.close();
			if (admin != null)
				admin.close();
		}
		return null;
	}

	@Override
	public int run(String[] args) throws Exception {

		String usage = "Usage: hbase <cmd> <tablename> [params] cmd and params As follows \n"
				+ "del tablename -row rowkey 'c1,list'   : delete one Specified row ,example del -row xxxx \n"
				+ "del tablename -scan startkey endkey 'c1,list' : delete Multi-line from scan by range  \n"
				+ "del tablename  -cfg cfgids/all -crawlId id : delete not match cfgids subrules rows \n"
				+ "drop tablename  ColumnFamilys,list : drop ColumnFamily from table  \n";
		if (args.length == 0) {
			System.err.println(usage);
			return -1;
		}
		int cmdType = 0;// 1 delete 2 drop
		int subCmdType = 0;// 1 row 2scan 3cfg
		String tableName = "";
		String[] params = null;
		if (args.length >= 2 && (args[0].equals("del") || args[0].equals("delete"))) {
			cmdType = 1;
			tableName = args[1];
		} else if (args.length >= 2 && args[0].equals("drop")) {
			cmdType = 2;
			tableName = args[1];
		}

		if (cmdType == 1) {// delete
			if (args.length >= 3) {
				if (args[2].equals("-row")) {
					subCmdType = 1;
					params = new String[args.length - 3];
					if (params.length > 0)
						System.arraycopy(args, 3, params, 0, params.length);
				} else if (args[2].equals("-scan")) {
					subCmdType = 2;
					params = new String[args.length - 3];
					if (params.length > 0)
						System.arraycopy(args, 3, params, 0, params.length);
				} else if (args[2].equals("-cfg")) {
					subCmdType = 3;
					if (args.length > 3) {
						params = new String[1];
						params[0] = args[3];
						if (args.length > 5 && args[4].equals("-crawlId")) {
							getConf().set(Nutch.CRAWL_ID_KEY, args[5]);
						}
					} else {
						System.err.println(usage);
						return -1;
					}
				}
			}
		} else if (cmdType == 2) {// drop
			params = new String[args.length - 2];
			if (params.length > 0)
				System.arraycopy(args, 3, params, 0, params.length);
		}
		if (cmdType == 0) {
			System.err.println(usage);
			return -1;
		}
		try {
			run(ToolUtil.toArgMap(ARGS_cmdType, cmdType, ARGS_tableName, tableName, ARGS_subCmdType, subCmdType, ARGS_params, params));
		} catch (Exception e) {
			LOG.error("GeneratorJob: " + StringUtils.stringifyException(e));
			return -1;
		}
		return 0;
	}

	public static class DeleteMapper extends GoraMapper<String, WebPage, String, WebPage> {
		int rowCount = 0;
		int deleteRowCount = 0;
		int convertRowCount = 0;
		private URLFilters filters;
		private URLNormalizers normalizers;
		private boolean filter;
		private boolean normalise;
		public static UrlPathMatch urlcfg = null;
		String priCfgIds = null;
		ArrayList<Delete> deletes = new ArrayList<Delete>();
		DataStore<String, WebPage> store = null;

		@Override
		public void map(String reversedUrl, WebPage page, Context context) throws IOException, InterruptedException {
			rowCount++;
			if ((rowCount % 1000) == 0)
				context.setStatus(getGenStatus(context));
			String url = TableUtil.unreverseUrl(reversedUrl);

			UrlPathMatch urlSegment = urlcfg.match(url);// 获取配置项
			boolean needDelete = false;

			String configUrl = "";
			UrlNodeConfig value = null;
			if (urlSegment == null) {
				// 不满足配置要求
				if (priCfgIds == null)
					needDelete = true;
			} else {
				value = (UrlNodeConfig) urlSegment.getNodeValue();
				String curl = NutchConstant.getUrlPath(url);
				if (curl == null) {
					System.out.println("非法url:" + url);
					needDelete = true;
				}
				configUrl = urlSegment.getConfigPath();
				page.setConfigUrl(new Utf8(configUrl));
				if (configUrl.equals(url) || configUrl.equals(curl)) {// 应用新设置的属性
					System.out.println("url:" + url + " is config url, set [sorce,FetchInterval]" + " to [" + value.customScore + ","
							+ value.customInterval + "]");
					page.setScore(value.customScore);
					page.setFetchInterval(value.customInterval);
					page.putToMarkers(DbUpdaterJob.DISTANCE, new Utf8(Integer.toString(1)));
				} else {
					if (value.subFilters != null) {
						if (SubURLFilters.filter(url, value.subFilters) == false) {
							System.out.println("过滤删除 url:" + url + " configUrl:" + configUrl);
							needDelete = true;
						}
					}
					boolean isSubCfgUrl = false;
					if (value.subCfgRules != null) {
						isSubCfgUrl = SubURLFilters.filter(url, value.subCfgRules);
					}
					if (isSubCfgUrl && !needDelete) {
						System.out.println(" 重置URL属性:" + url + " configUrl:" + configUrl);
						page.setScore(value.customScore);
						page.setFetchInterval(value.customInterval);
						page.putToMarkers(DbUpdaterJob.DISTANCE, new Utf8(Integer.toString(1)));
					} else if (page.getFetchInterval() < value.subFetchInterval * 0.5) {
						// GeneratorJob.LOG.warn("url:" + url + " FetchInterval is too small, set to " + value.subFetchInterval);
						page.setFetchInterval(value.subFetchInterval);
						page.putToMarkers(DbUpdaterJob.DISTANCE, new Utf8(Integer.toString(2)));
					}
				}
				page.setCrawlType(value.crawlType);
				// If filtering is on don't generate URLs that don't pass URLFilters
				try {
					if (normalise) {
						url = normalizers.normalize(url, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
					}
					// 允许配置文件中配置禁止抓取的地址
					if (filter && filters.filter(url) == null) {
						System.out.println("filter url:" + url + " rowkey:" + reversedUrl);
						needDelete = true;
					}
				} catch (URLFilterException e) {
					context.getCounter("mapRead", "filter_err").increment(1);
					System.out.println("Couldn't filter url: " + url + " (" + e.getMessage() + ")");
					needDelete = true;
				} catch (MalformedURLException e) {
					context.getCounter("mapRead", "filter_err").increment(1);
					System.out.println("Couldn't filter url: " + url + " (" + e.getMessage() + ")");
					needDelete = true;
				}
			}
			if (value != null && value.regFormat != null) {
				String tmpurl = value.regFormat.replace(url);
				if (!tmpurl.equals(url)) {
					needDelete = true;
					System.out.println("delete:" + reversedUrl + "  add:" + tmpurl);
					convertRowCount++;
					context.write(TableUtil.reverseUrl(tmpurl), page);
				}
			}
			if (needDelete) {
				System.out.println("delete url:" + url + " rowkey:" + reversedUrl + " configUrl:" + configUrl);
				store.delete(reversedUrl);
				deleteRowCount++;
				if (deleteRowCount % 1000 == 0) {
					store.flush();
				}
			} else {
				context.write(reversedUrl, page);
			}
		}

		String getGenStatus(Context context) {
			String res = "read  " + rowCount + " pages delete:" + deleteRowCount + "  coonvert:" + convertRowCount;
			return res + "  更新时间：" + (new Date().toLocaleString());
		}

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			priCfgIds = conf.get(NutchConstant.configIdsKey, null);
			try {
				store = StorageUtils.createWebStore(conf, String.class, WebPage.class);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				throw new IOException(e);
			}
			filter = conf.getBoolean(GeneratorJob.GENERATOR_FILTER, true);
			normalise = conf.getBoolean(GeneratorJob.GENERATOR_NORMALISE, true);

			if (filter) {
				filters = new URLFilters(conf);
			}
			if (normalise) {
				normalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_GENERATE_HOST_COUNT);
			}
			GoraInputSplit split = (GoraInputSplit) context.getInputSplit();
			context.setStatus(split.getQuery().getStartKey() + "==>" + split.getQuery().getEndKey());
			System.out.println("start_rowkey: " + split.getQuery().getStartKey() + "  end_rowkey:" + split.getQuery().getEndKey()
					+ "  进入时间:" + (new Date().toLocaleString()));
			urlcfg = NutchConstant.getUrlConfig(conf);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			if (store != null) {
				store.flush();
				store.close();
			}
			GoraInputSplit split = (GoraInputSplit) context.getInputSplit();
			context.setStatus(getGenStatus(context) + " from  " + split.getQuery().getStartKey() + "==>" + split.getQuery().getEndKey()
					+ " 退出时间:" + (new Date().toLocaleString()));
			System.out.println("start_rowkey: " + split.getQuery().getStartKey() + "  end_rowkey:" + split.getQuery().getEndKey()
					+ "  退出时间:" + (new Date().toLocaleString()));
		}

	}
}
