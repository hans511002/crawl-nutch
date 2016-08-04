package org.apache.nutch.crawl;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.avro.util.Utf8;
import org.apache.gora.mapreduce.GoraOutputFormat;
import org.apache.gora.store.DataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.storage.Mark;
import org.apache.nutch.storage.StorageUtils;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPageIndex;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.NutchTool;
import org.apache.nutch.util.TableUtil;
import org.apache.nutch.util.ToolUtil;
import org.apache.nutch.zk.ZooKeeperServer;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sobey.jcg.support.jdbc.DataAccess;
import com.sobey.jcg.support.jdbc.DataSourceImpl;

public class InjectorDbJob extends NutchTool implements Tool {

	public static final Logger LOG = LoggerFactory.getLogger(InjectorDbJob.class);

	private static final Set<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

	private static final Utf8 YES_STRING = new Utf8("y");

	static {
		FIELDS.add(WebPage.Field.MARKERS);
		FIELDS.add(WebPage.Field.STATUS);
		FIELDS.add(WebPage.Field.CRAWLTYPE);
		FIELDS.add(WebPage.Field.CONFIGURL);
	}

	/** metadata key reserved for setting a custom score for a specific URL */

	/**
	 * metadata key reserved for setting a custom fetchInterval for a specific URL metadata key reserved for setting a
	 * custom fetchInterval for a specific URL metadata key reserved for setting a custom fetchInterval for a specific
	 * URL
	 */
	public static class UrlMapper extends Mapper<LongWritable, Text, String, WebPage> {
		private URLNormalizers urlNormalizers;
		private URLFilters filters;
		private ScoringFilters scfilters;
		private long curTime;
		private static boolean loaded = false;

		private DataSourceImpl dataSource;
		ZooKeeperServer zk;
		private Configuration conf;
		private String injectInPath;
		String MODIFY_DATE = null;
		DataStore<String, WebPageIndex> store = null;
		String batchID;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			urlNormalizers = new URLNormalizers(conf, URLNormalizers.SCOPE_INJECT);
			filters = new URLFilters(conf);
			scfilters = new ScoringFilters(conf);
			// if (curTime == 0)
			// throw new IOException("config error " + NutchConstant.stepZkBatchDateKey + " is null");
			// batchID = conf.get(NutchConstant.BATCH_ID_KEY);

			try {
				dataSource = new DataSourceImpl(conf.get(NutchConstant.injectorDbDriverName,
						NutchConstant.configDbDriverName), conf.get(NutchConstant.injectorDbUrl,
						NutchConstant.configDbUrl), conf.get(NutchConstant.injectorDbUser, NutchConstant.configDbUser),
						conf.get(NutchConstant.injectorDbPass, NutchConstant.configDbPass));
			} catch (SQLException e) {
				LOG.error("连接数据库", e);
				throw new IOException(e);
			}
			zk = new ZooKeeperServer(NutchConstant.getZKConnectString(conf));// new ZooKeeper(zkConnectString, 18000,
																				// null);
			String rootZkNode = conf.get(NutchConstant.zookeeperRootPath, NutchConstant.defaultNutchRootZkPath);
			String injectPath = rootZkNode + "/" + NutchConstant.BatchNode.injectNode.nodeName;
			// try {
			// store = StorageUtils.createWebStore(conf, String.class, WebPageIndex.class);
			// if (store == null)
			// throw new IOException("Could not create datastore");
			injectInPath = injectPath + "/" + NutchConstant.stepZKInDateNode;
			// } catch (ClassNotFoundException e) {
			// throw new IOException("ERROR:" + e.getMessage());
			// }
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (loaded)
				return;
			loaded = true;
			java.sql.Connection con = null;
			ResultSet rs = null;
			try {
				String selectSql = conf.get(NutchConstant.injectorSelectSql, NutchConstant.injectSelectSql);
				if (selectSql.indexOf("{LASTLOAD_DATE}") > 0) {
					String lastDate = null;
					try {
						// System.err.println(injectInPath);
						if (zk.exists(injectInPath) != null) {
							lastDate = new String(zk.getData(injectInPath, false, null));
						}
					} catch (Exception e) {
						LOG.warn(injectInPath + " is null :" + e.getMessage());
					}
					// java.text.SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					// lastDate = sf.format(new Date(Long.parseLong(lastDate)));
					if (lastDate == null)
						selectSql = selectSql.replace("{LASTLOAD_DATE}", "2013-05-25 23:59:59");
					else
						selectSql = selectSql.replace("{LASTLOAD_DATE}", lastDate);
				}
				System.out.println("selectSql=" + selectSql);
				con = dataSource.getConnection();
				DataAccess access = new DataAccess(con);
				rs = access.execQuerySql(selectSql);
				// java.text.SimpleDateFormat sdt = new SimpleDateFormat("yyyyMMddHHmmss");
				// Date dt = new Date(curTime);
				// String indexKey = sdt.format(dt);
				int rowCount = 0;
				while (rs.next()) {
					Map<String, String> metadata = new TreeMap<String, String>();
					long cfgType = rs.getLong(7);
					if ((cfgType & 1) != 1) {// CONFIG_TYPE 2:要素解析地址 1:抓取地址 3:1和2
						continue;
					}
					String url = rs.getString(1);
					int CRAWL_TYPE = rs.getInt(2);
					int customInterval = rs.getInt(3);
					float customScore = rs.getFloat(4);
					String metaData = rs.getString(5);
					String modDate = rs.getString(6);
					if (MODIFY_DATE == null)
						MODIFY_DATE = modDate;
					else if (MODIFY_DATE.compareTo(modDate) < 0)
						MODIFY_DATE = modDate;
					if (metaData != null) {
						String[] splits = metaData.split(" ");
						for (int s = 0; s < splits.length; s++) {
							// find separation between name and value
							int indexEquals = splits[s].indexOf("=");
							if (indexEquals == -1) {
								continue;
							}
							String metaname = splits[s].substring(0, indexEquals);
							String metavalue = splits[s].substring(indexEquals + 1);
							metadata.put(metaname, metavalue);
						}
					}
					try {
						url = urlNormalizers.normalize(url, URLNormalizers.SCOPE_INJECT);
						url = filters.filter(url); // filter the url
						new URI(url);
					} catch (Exception e) {
						LOG.warn("Skipping " + url + ":" + e);
						url = null;
					}
					if (url == null)
						continue;
					rowCount++;
					String reversedUrl = TableUtil.reverseUrl(url);
					WebPage row = new WebPage();
					row.setFetchTime(curTime);
					row.setFetchInterval(customInterval);
					// now add the metadata
					Iterator<String> keysIter = metadata.keySet().iterator();
					while (keysIter.hasNext()) {
						String keymd = keysIter.next();
						String valuemd = metadata.get(keymd);
						row.putToMetadata(new Utf8(keymd), ByteBuffer.wrap(valuemd.getBytes()));
					}

					row.setScore(customScore);
					try {
						scfilters.injectedScore(url, row);
					} catch (ScoringFilterException e) {
						if (LOG.isWarnEnabled()) {
							LOG.warn("Cannot filter injected score for url " + url + ", using default ("
									+ e.getMessage() + ")");
						}
					}
					row.putToMarkers(DbUpdaterJob.DISTANCE, new Utf8(String.valueOf(0)));
					Mark.INJECT_MARK.putMark(row, YES_STRING);
					System.out.println("rowkey:" + reversedUrl + " CRAWL_TYPE=" + CRAWL_TYPE + " url=" + url);
					// row.configUrl = new Utf8(url);
					row.setConfigUrl(new Utf8(url));// 装入配置根地址
					// row.crawlType = CRAWL_TYPE;
					row.setCrawlType(CRAWL_TYPE);
					row.setStatus(CrawlStatus.STATUS_UNFETCHED);
					System.out.println("rowkey:" + reversedUrl + " CRAWL_TYPE=" + CRAWL_TYPE + " url=" + url);
					context.write(reversedUrl, row);
					context.setStatus("导入地址数:" + rowCount);
					// WebPageIndex wpi = new WebPageIndex();
					// String pindexKey = indexKey + reversedUrl;
					// wpi.setBaseUrl(row.getBaseUrl());
					// wpi.setFetchTime(curTime);
					// wpi.setMetadata(row.getMetadata());
					// wpi.setMarkers(row.getMarkers());
					// store.put(pindexKey, wpi);
				}
			} catch (SQLException e1) {
				e1.printStackTrace();
				throw new IOException(e1);
				// } catch (KeeperException e) {
				// e.printStackTrace();
			} finally {
				if (con != null) {
					try {
						if (rs != null)
							rs.close();
						con.close();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					con = null;
					rs = null;
				}
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			if (zk != null) {
				try {
					// System.err.println("injectInPath" + injectInPath + " MODIFY_DATE=" + MODIFY_DATE);
					if (MODIFY_DATE != null) {
						if (zk.exists(injectInPath) != null) {
							String existsDate = new String(zk.getData(injectInPath, false));
							if (existsDate == null || MODIFY_DATE.compareTo(existsDate) > 0) {
								zk.setData(injectInPath, MODIFY_DATE.getBytes());
							}
						} else {
							zk.createPaths(injectInPath, MODIFY_DATE);
						}
					}
				} catch (KeeperException e) {
					e.printStackTrace();
					LOG.error("向ZK写入同步时间发生错误: " + e.getMessage());
				}
				zk.close();
				zk = null;
			}
			if (store != null) {
				store.close();
				store = null;
			}
		}
	}

	public InjectorDbJob() {

	}

	public InjectorDbJob(Configuration conf) {
		setConf(conf);
	}

	public Map<String, Object> run(Map<String, Object> args) throws Exception {
		getConf().setLong("injector.current.time", System.currentTimeMillis());
		Path input;
		Object path = args.get(Nutch.ARG_SEEDDIR);
		if (path instanceof Path) {
			input = (Path) path;
		} else {
			input = new Path(path.toString());
		}
		// NutchConstant.init(getConf());
		// Long curTime = this.getConf().getLong(NutchConstant.stepZkBatchDateKey, 0);
		// if (NutchConstant.preparStartJob(this.getConf(), NutchConstant.injectZkNode, curTime, null,
		// NutchConstant.BatchStatus.injecting,
		// NutchConstant.BatchStatus.injected, LOG, true) == 0)
		// return null;
		numJobs = 1;
		currentJobNum = 0;
		currentJob = new NutchJob(getConf(), "inject from config db");
		FileInputFormat.addInputPath(currentJob, input);
		currentJob.setMapperClass(UrlMapper.class);
		currentJob.setMapOutputKeyClass(String.class);
		currentJob.setMapOutputValueClass(WebPage.class);
		currentJob.setOutputFormatClass(GoraOutputFormat.class);
		DataStore<String, WebPage> store = StorageUtils.createWebStore(currentJob.getConfiguration(), String.class,
				WebPage.class);
		GoraOutputFormat.setOutput(currentJob, store, true);
		currentJob.setReducerClass(Reducer.class);
		currentJob.setNumReduceTasks(0);
		currentJob.waitForCompletion(true);
		// NutchConstant.preparEndJob(this.getConf(), NutchConstant.injectZkNode, NutchConstant.BatchStatus.injected,
		// LOG);
		ToolUtil.recordJobStatus(null, currentJob, results);
		return results;
	}

	public void inject(Path urlDir) throws Exception {
		LOG.info("InjectorJob: starting");
		LOG.info("InjectorJob: urlDir: " + urlDir);

		run(ToolUtil.toArgMap(Nutch.ARG_SEEDDIR, urlDir));
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Usage: InjectorJob <url_dir> [-crawlId <id>]");
			return -1;
		}
		for (int i = 1; i < args.length; i++) {
			if ("-crawlId".equals(args[i])) {
				getConf().set(Nutch.CRAWL_ID_KEY, args[i + 1]);
				i++;
			} else {
				System.err.println("Unrecognized arg " + args[i]);
				return -1;
			}
		}
		if (getConf().get(NutchConstant.BATCH_ID_KEY, null) == null) {
			getConf().set(NutchConstant.BATCH_ID_KEY, "none");
		}
		System.out.println("batchZKId= " + getConf().get(NutchConstant.BATCH_ID_KEY));
		try {
			inject(new Path(args[0]));
			LOG.info("InjectorJob: finished");
			return -0;
		} catch (Exception e) {
			LOG.error("InjectorJob: " + StringUtils.stringifyException(e));
			return -1;
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(NutchConfiguration.create(), new InjectorDbJob(), args);
		System.exit(res);
	}
}
