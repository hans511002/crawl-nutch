package org.apache.nutch.hbase;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Copyrights @ 2012,Tianyuan DIC Information Co.,Ltd. All rights reserved.<br>
 * 
 * @author hans
 * @description �����嵥��Hbase��
 * @date 2012-11-15
 */
public class cdrListToHbaseMap {
	public static String formatToDate(String s) {
		// java.text.DateFormat
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dateTime = null;
		try {
			dateTime = sdf.parse(s);
			DateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
			return formatter.format(dateTime);
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static void main(String[] args) throws Exception {
		String hdfsHost = "hadoop01:9000";
		String jobTrackerHost = "hadoop01:9001";
		String dirInput = "/tmp/cdma";
		String dirOutput = "tmp/cdrListMap";

		// int res = 1;
		// if (res > 0) {
		// String time = "2011-7-1 16:53:00";
		// String rowKey = time.replaceAll("(?i)[- :]", "");
		// System.out.println(rowKey);
		// System.out.println(NetListToHbaseMap.formatToDate(time));
		// return;
		// }
		// String val =
		// "280^201309^18981946175^21^280901288349960038^2013-09-01 00:05:07^145^2013-09-01 00:07:32^01^102025956686^18981946175^104005164120^1^^028^^^^^028^^028^^600339^028^207790001^^52632^^^0^^0^0^0^0^0^0^8618981946175^880213^0^^0^2013-09-01 00:29:48^1208^0^0^1^1^0^8628207790001^^^0^^^^0^280043316139^0^0^0^^20131001220011";
		// System.out.println(val.split("\\^").length);
		// String expr = "DURATION + '>40 && DURATION<90'";
		// Expression execExp = AviatorEvaluator.compile(expr.trim(), true);
		// Map<String, Object> env = new HashMap<String, Object>();
		// env.put("DURATION", 145);
		// System.out.println(execExp.execute(env));
		//
		// if (val != null)
		// return;

		try {
			if (args.length == 1 && (args[0].equals("--help") || args[0].equals("-h") || args[0].equals("/?"))) {
				System.out.println("Usage: WordCount <options>");
				System.out.println();
				System.out.println("Options:");
				System.out.println();
				System.out.println("--input=DIR                   The directory containing the input files for the");
				System.out.println("                              WordCount Hadoop job");
				System.out.println("--hdfsHost=HOST               The host<:port> of the HDFS service");
				System.out.println("                              e.g.- localhost:9000");
				System.out.println("--jobTrackerHost=HOST         The host<:port> of the job tracker service");
				System.out.println("                              e.g.- localhost:9001");
				System.out.println();
				System.out.println();
				System.out.println("If an option is not provided through the command prompt the following defaults");
				System.out.println("will be used:");
				System.out.println("--input='/tmp/cdma'");
				System.out.println("--hdfsHost=hadoop01:9000");
				System.out.println("--jobTrackerHost=hadoop01:9001");
			} else {

				if (args.length > 0) {
					for (String arg : args) {
						if (arg.startsWith("--input=")) {
							dirInput = cdrListToHbaseMap.getArgValue(arg);
						} else if (arg.startsWith("--hdfsHost=")) {
							hdfsHost = cdrListToHbaseMap.getArgValue(arg);
						} else if (arg.startsWith("--jobTrackerHost=")) {
							jobTrackerHost = cdrListToHbaseMap.getArgValue(arg);
						}
					}
				}

				JobConf conf = new JobConf(cdrListToHbaseMap.class);
				conf.addResource("hbase-default.xml");
				conf.addResource("hbase-site.xml");
				// conf.set("hbase.zookeeper.quorum", "scdbdatanode4,scdbdatanode5,scdbdatanode6,scbdnamenode1,scbdnamenode2");
				conf.setJobName("CdrListMap");
				String hdfsBaseUrl = "hdfs://" + hdfsHost;
				conf.set("fs.default.name", hdfsBaseUrl);
				conf.set("mapred.job.tracker", jobTrackerHost);

				String[] dirInputs = dirInput.split(",");
				Path inPaths[] = new Path[dirInputs.length];
				for (int i = 0; i < dirInputs.length; i++) {
					inPaths[i] = new Path(dirInputs[i]);
				}
				FileSystem fs = FileSystem.get(conf);
				Path outPath = new Path(dirOutput);
				if (fs.exists(outPath)) {
					fs.delete(outPath, true);
				}
				FileInputFormat.setInputPaths(conf, inPaths);
				FileOutputFormat.setOutputPath(conf, outPath);
				conf.setMapperClass(netListMap.class);
				conf.setReducerClass(nullReducer.class);

				// conf.setInputFormat(theClass);
				conf.setMapOutputKeyClass(Text.class);
				conf.setMapOutputValueClass(IntWritable.class);
				conf.setNumMapTasks(600);
				conf.setNumReduceTasks(0);
				conf.setOutputKeyClass(Text.class);
				conf.setOutputValueClass(IntWritable.class);
				netListMap.init(conf);
				conf.set("mapred.job.priority", "LOW");
				JobClient.runJob(conf);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class netListMap extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
		public static String tableName = "cdCallList";
		boolean inited = false;
		int rowCount = 0;
		int[] typeRowCount = new int[4];
		long sl = 0;
		HTable table = null;
		JobConf job = null;
		static String cols = "LATN_ID:1,MONTH_NO:1,BILLING_NBR:1,TABLE_TYPE:1,FILE_ID:2,EVENT_ID:1,START_DATE:1,DURATION:1,END_DATE:1,DATE_NO:1,"
				+ "SERV_ID:1,SP_FLAG:2,CALLING_NBR:1,CUST_ID:1,GATE_WAY:2,CALL_TYPE:1,ACCOUNT_NBR:1,BILLING_VISIT_AREA_CODE:1,NAS_IP:2,MSG_ID:2,"
				+ "ROAM_FLAG:1,CALLING_SP_TYPE_ID:2,SERV_CODE:1,SEND_AMOUNT:1,CALLED_SP_TYPE_ID:2,SERVICE_TYPE:1,CALLING_AREA_CODE:1,NAI:2,RECV_AMOUNT:1,"
				+ "CALLING_VISIT_AREA_CODE:1,SUM_AMOUNT:1,EVENT_TYPE_ID:1,CALLED_AREA_CODE:1,CALLED_NBR:1,ACCOUNT_AREA_CODE:1,ACCT_ITEM_TYPE_ID1:1,CALLED_CELL_ID:1,CELL_ID:2,CALLED_VISIT_AREA_CODE:1,CHARGE1:1,"
				+ "CALLED_LAC:1,CARRIER_TYPE_ID:2,ACCT_ITEM_TYPE_ID2:1,CHARGE2:1,ACCT_ITEM_TYPE_ID3:1,CHARGE3:1,ACCT_ITEM_TYPE_ID4:1,CHARGE4:1,LONG_TYPE_ID:2,IMSI:2,"
				+ "ORG_CALLING_NBR:1,TRUNK_IN:2,TRUNK_OUT:2,DOMAIN_NAME:2,MSC:2,SWITCH_ID:1,ACCT_ITEM_TYPE_ID5:1,LAC:1,CHARGE5:1,CREATED_DATE:1,"
				+ "PRODUCT_ID:1,ACCT_ITEM_TYPE_ID6:1,CHARGE6:1,SOURCE_EVENT_TYPE:1,BUSINESS_KEY:1,RESERVED_FIELD1:2,ROAM_TYPE_ID:1,RESERVED_FIELD2:2,RESERVED_FIELD3:2,TRUNK_IN_TYPE_ID:2,"
				+ "RESERVED_FIELD4:2,RESERVED_FIELD5:2,ORG_CALLED_NBR:1,THIRD_PARTY_AREA_CODE:1,THIRD_PARTY_NBR:1,ROAM_NBR:1,EVENT_STATE:1,STATE_DATE:1,THIRD_PARTY_VISIT_AREA_CODE:1,SERVICE_CODE:1,"
				+ "STD_ACCT_ITEM_TYPE_ID1:2,STD_ACCT_ITEM_TYPE_ID2:2,STD_ACCT_ITEM_TYPE_ID3:2,STD_ACCT_ITEM_TYPE_ID4:2,STD_CHARGE1:2,STD_CHARGE2:2,STD_CHARGE3:2,STD_CHARGE4:2,SYS_ID:2,OFFER_INSTANCE_ID1:1,"
				+ "OFFER_INSTANCE_ID2:1,OFFER_INSTANCE_ID3:1,OFFER_INSTANCE_ID4:1,PCF_ADDRESS:2,STATE_FLAG:1,LOAD_DATE:1";
		static int cellType[] = null;// 96���ֶη��д�
		static String cellName[] = null;
		static int idIndex[] = { 2, 6, 5 };
		static int cfTypeIndex = 3;
		StringBuilder val[] = new StringBuilder[2];

		public void initPar(JobConf job) throws IOException {
			if (inited)
				return;
			cellName = cols.split(",");
			cellType = new int[cellName.length];
			for (int i = 0; i < cellName.length; i++) {
				String tmp[] = cellName[i].split(":");
				cellType[i] = Convert.ToInt(tmp[1]);
				cellName[i] = tmp[0];
			}
			sl = System.currentTimeMillis();
			table = new HTable(job, tableName);
			table.setAutoFlush(false);
			inited = true;
		}

		public static void init(JobConf job) throws Exception {
			try {
				// HBaseDBDao.createTable(tableName, "cdr21", "cdr22", "cdr23", "cdr24");
				cellName = cols.split(",");
				cellType = new int[cellName.length];
				for (int i = 0; i < cellName.length; i++) {
					String tmp[] = cellName[i].split(":");
					cellType[i] = Convert.ToInt(tmp[1]);
					cellName[i] = tmp[0];
				}
				Class.forName("oracle.jdbc.driver.OracleDriver");
				// HBaseDBDao.createTable(tableName, "cdr1", "cdr2");
				HTable table = new HTable(job, tableName);// HBaseConfiguration.create()
				// job.setLong("hbase.client.write.buffer", 5000000);
				table.setAutoFlush(true);
				StringBuilder val[] = new StringBuilder[2];
				val[0] = new StringBuilder();
				val[1] = new StringBuilder();
				boolean valFlag[] = new boolean[2];
				valFlag[0] = true;
				valFlag[1] = true;
				for (int i = 0; i < cellName.length; i++) {
					int t = cellType[i] - 1;
					if (valFlag[t]) {
						val[t].append(cellName[i]);
						valFlag[t] = false;
					} else {
						val[t].append("^");
						val[t].append(cellName[i]);
					}
				}
				Put pt = new Put("COLUMN_NAMES".getBytes());// ���ROWKEY
				pt.add("cdr21".getBytes(), "COLUMN1".getBytes(), val[0].toString().getBytes());
				pt.add("cdr21".getBytes(), "COLUMN2".getBytes(), val[1].toString().getBytes());
				pt.add("cdr22".getBytes(), "COLUMN1".getBytes(), val[0].toString().getBytes());
				pt.add("cdr22".getBytes(), "COLUMN2".getBytes(), val[1].toString().getBytes());
				pt.add("cdr23".getBytes(), "COLUMN1".getBytes(), val[0].toString().getBytes());
				pt.add("cdr23".getBytes(), "COLUMN2".getBytes(), val[1].toString().getBytes());
				pt.add("cdr24".getBytes(), "COLUMN1".getBytes(), val[0].toString().getBytes());
				pt.add("cdr24".getBytes(), "COLUMN2".getBytes(), val[1].toString().getBytes());
				table.put(pt);
				table.close();
			} catch (Exception e) {
				e.printStackTrace();
				throw new IOException(e);
			}
		}

		@Override
		public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			if (!inited) {
				initPar(job);
			}
			try {
				if (writeRecord(value))
					rowCount++;
			} catch (Exception e) {
				System.out.println("ERROR:" + e.getMessage());
			}
			if (rowCount % 10000 == 0) {
				System.out.println("д����� " + rowCount + "��    ��ʱ��" + (System.currentTimeMillis() - sl) / 1000 + "��   ");
			}
		}

		public void close() throws IOException {
			System.out.println("д����� " + rowCount + "��    ��ʱ��" + (System.currentTimeMillis() - sl) / 1000 + "��");
			if (table != null) {
				try {
					Get get = new Get("ROW_COUNT".getBytes());
					Result r = table.get(get);
					int acount = 0;
					int _typeRowCount[] = new int[4];
					if (r != null) {
						KeyValue v = r.getColumnLatest("cdr21".getBytes(), "ALL_COUNTS".getBytes());
						if (v != null) {
							acount = Convert.ToInt(new String(v.getValue()));
						}
						v = r.getColumnLatest("cdr21".getBytes(), "COUNTS".getBytes());
						if (v != null) {
							_typeRowCount[0] = Convert.ToInt(new String(v.getValue()));
						}
						v = r.getColumnLatest("cdr22".getBytes(), "COUNTS".getBytes());
						if (v != null) {
							_typeRowCount[1] = Convert.ToInt(new String(v.getValue()));
						}
						v = r.getColumnLatest("cdr23".getBytes(), "COUNTS".getBytes());
						if (v != null) {
							_typeRowCount[2] = Convert.ToInt(new String(v.getValue()));
						}
						v = r.getColumnLatest("cdr24".getBytes(), "COUNTS".getBytes());
						if (v != null) {
							_typeRowCount[3] = Convert.ToInt(new String(v.getValue()));
						}
					}

					Put pt = new Put("ROW_COUNT".getBytes());// ���ROWKEY
					pt.add("cdr21".getBytes(), "COUNTS".getBytes(), (typeRowCount[0] + _typeRowCount[0] + "").getBytes());
					pt.add("cdr22".getBytes(), "COUNTS".getBytes(), (typeRowCount[1] + _typeRowCount[1] + "").getBytes());
					pt.add("cdr23".getBytes(), "COUNTS".getBytes(), (typeRowCount[2] + _typeRowCount[2] + "").getBytes());
					pt.add("cdr24".getBytes(), "COUNTS".getBytes(), (typeRowCount[3] + _typeRowCount[3] + "").getBytes());
					pt.add("cdr21".getBytes(), "ALL_COUNTS".getBytes(), (rowCount + acount + "").getBytes());
					pt.add("cdr22".getBytes(), "ALL_COUNTS".getBytes(), (rowCount + acount + "").getBytes());
					pt.add("cdr23".getBytes(), "ALL_COUNTS".getBytes(), (rowCount + acount + "").getBytes());
					pt.add("cdr24".getBytes(), "ALL_COUNTS".getBytes(), (rowCount + acount + "").getBytes());
					table.put(pt);
					table.flushCommits();

					System.out.println("��ԭ��¼��" + acount + " ���δ����¼��" + rowCount + "  �ܼ�¼�� : " + (rowCount + acount));
					System.out.println("21����ԭ��¼��" + _typeRowCount[0] + " ���δ����¼��" + typeRowCount[0] + "  �ܼ�¼�� : "
							+ (typeRowCount[0] + _typeRowCount[0]));
					System.out.println("22����ԭ��¼��" + _typeRowCount[1] + " ���δ����¼��" + typeRowCount[1] + "  �ܼ�¼�� : "
							+ (typeRowCount[1] + _typeRowCount[1]));
					System.out.println("23����ԭ��¼��" + _typeRowCount[2] + " ���δ����¼��" + typeRowCount[2] + "  �ܼ�¼�� : "
							+ (typeRowCount[2] + _typeRowCount[2]));
					System.out.println("24����ԭ��¼��" + _typeRowCount[3] + " ���δ����¼��" + typeRowCount[3] + "  �ܼ�¼�� : "
							+ (typeRowCount[3] + _typeRowCount[3]));

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			if (table != null)
				table.close();
		}

		public void configure(JobConf job) {
			this.job = job;
		}

		public boolean writeRecord(Text value) throws IOException {
			String row = value.toString();
			String cells[] = row.split("\\^", cellName.length);
			if (cells.length != cellName.length) {
				System.out.println("��¼����д�С����ȷ����" + row + "��");
				return false;
			}
			String time = cdrListToHbaseMap.formatToDate(cells[idIndex[1]]);
			if (time == null) {
				System.out.println("��¼ʱ�䲻�ɽ�������" + row + "��");
				return false;
			}
			String rowKey = cells[idIndex[0]] + "-" + time + "-" + cells[idIndex[2]].hashCode();
			val[0] = new StringBuilder();
			val[1] = new StringBuilder();
			boolean valFlag[] = new boolean[2];
			valFlag[0] = true;
			valFlag[1] = true;
			int type = Convert.ToInt(cells[cfTypeIndex]);
			if (type < 21 || type > 24) {
				System.out.println("����ȷ���嵥���ͣ���" + row + "��");
				return false;
			}
			typeRowCount[type - 21]++;
			for (int i = 0; i < cells.length; i++) {
				int t = cellType[i] - 1;
				if (valFlag[t]) {
					val[t].append(cells[i]);
					valFlag[t] = false;
				} else {
					val[t].append("^");
					val[t].append(cells[i]);
				}
			}
			Put pt = new Put(rowKey.getBytes());// ���ROWKEY
			String typeCf = "cdr" + type;
			pt.add(typeCf.getBytes(), "val1".getBytes(), val[0].toString().getBytes());
			pt.add(typeCf.getBytes(), "val2".getBytes(), val[1].toString().getBytes());

			// for (int i = 0; i < cells.length; i++) {
			// String cf = "cdr" + cellType[i];
			// pt.add(cf.getBytes(), cellName[i].getBytes(), cells[i].getBytes());
			// }
			table.put(pt);
			return true;
		}
	}

	public class nullReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

		}
	}

	private static String getArgValue(String arg) {
		String result = null;

		String[] tokens = arg.split("=");
		if (tokens.length > 1) {
			result = tokens[1].replace("'", "").replace("\"", "");
		}
		return result;
	}
}