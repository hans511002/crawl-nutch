package org.apache.nutch.hbase.tool;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class HBaseTableRowCount {

	static Configuration conf;
	static FileSystem fs;
	static final String HBASE_TABLE_NAME_KEY = "HBASE_TABLE_NAME_KEY";
	public static final String HbaseStartRowKey = "step.hbase.start.rowkey";
	public static final String HbaseEndRowKey = "step.hbase.end.rowkey";
	public static final String HbaseCFQUKey = "step.hbase.cfqu.key";

	public static void main(String[] args) throws Exception {
		String tableName = "ea_webpage";
		String skey = "";
		String ekey = "";
		Configuration conf = HBaseConfiguration.create();
		conf.addResource("hbase-default.xml");
		conf.addResource("hbase-site.xml");
		// conf.set("hbase.zookeeper.quorum", "scdbdatanode4,scdbdatanode5,scdbdatanode6,scbdnamenode1,scbdnamenode2");
		conf.set("mapred.job.priority", "LOW");
		String dirOutput = "/tmp/o";
		FileSystem fs = FileSystem.get(conf);
		Path outPath = new Path(dirOutput);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		Job job = new Job(conf);
		job.setJobName("");
		HbaseInputFormat.initMapperJob(job, Text.class, Map.class, HbaseTableRowCountMapper.class, null, tableName, skey, ekey);
		job.setReducerClass(HbaseTableRowCountReducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(String.class);
		job.setOutputValueClass(Put.class);
		job.setOutputFormatClass(HbaseOutputFormat.class);

		job.waitForCompletion(true);
	}

	public static class HbaseInputFormat extends InputFormat<String, Result> implements Configurable {

		Configuration conf;

		public static <K1, V1, K2, V2> void initMapperJob(Job job, Class<K2> outKeyClass, Class<V2> outValueClass,
				Class<? extends Mapper> mapperClass, Class<? extends Partitioner> partitionerClass, String tableName, String skey,
				String ekey, String... cfqus) {
			job.setInputFormatClass(HbaseInputFormat.class);
			job.setMapperClass(mapperClass);
			job.setOutputKeyClass(outKeyClass);
			job.setOutputValueClass(outValueClass);
			if (partitionerClass != null) {
				job.setPartitionerClass(partitionerClass);
			}

			Configuration conf = job.getConfiguration();
			conf.set(HBASE_TABLE_NAME_KEY, tableName);
			if (skey != null && skey.trim().equals("") == false)
				conf.set(HbaseStartRowKey, skey);
			if (ekey != null && ekey.trim().equals("") == false)
				conf.set(HbaseEndRowKey, ekey);
			conf.setStrings(HbaseCFQUKey, cfqus);
		}

		@Override
		public RecordReader<String, Result> createRecordReader(InputSplit inputsplit, TaskAttemptContext taskattemptcontext)
				throws IOException, InterruptedException {
			return new HbaseRecordReader(inputsplit, taskattemptcontext);
		}

		public static class HbaseRecordReader extends RecordReader<String, Result> {
			ResultScanner scaner;
			String key;
			Result val;
			long rows = 0;

			public HbaseRecordReader(InputSplit inputsplit, TaskAttemptContext taskattemptcontext) throws IOException, InterruptedException {
				this.initialize(inputsplit, taskattemptcontext);
			}

			@Override
			public void close() throws IOException {
				scaner.close();
			}

			@Override
			public String getCurrentKey() throws IOException, InterruptedException {
				return key;
			}

			@Override
			public Result getCurrentValue() throws IOException, InterruptedException {
				return val;
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				return rows;
			}

			@Override
			public void initialize(InputSplit inputsplit, TaskAttemptContext taskattemptcontext) throws IOException, InterruptedException {
				HbaseInputSplit hsp = (HbaseInputSplit) inputsplit;
				Configuration conf = taskattemptcontext.getConfiguration();
				HTable table = new HTable(conf, hsp.tableName);
				Scan scan = new Scan(hsp.startKey, hsp.endKey);
				String[] cfqus = conf.getStrings(HbaseCFQUKey, null);
				if (cfqus != null) {
					for (String cfqu : cfqus) {
						String[] tmp = cfqu.split(":");
						if (tmp.length == 1) {
							if (!tmp[0].trim().equals("") && !tmp[1].trim().equals(""))
								scan.addColumn(tmp[0].getBytes(), tmp[1].getBytes());
							else if (!tmp[0].trim().equals(""))
								scan.addFamily(tmp[0].getBytes());
						} else if (!tmp[0].trim().equals("")) {
							scan.addFamily(tmp[0].getBytes());
						}
					}
				}
				scaner = table.getScanner(scan);
			}

			@Override
			public boolean nextKeyValue() throws IOException, InterruptedException {
				if ((val = scaner.next()) != null) {
					key = new String(val.getRow());
					return true;
				} else {
					key = null;
					val = null;
					return false;
				}
			}

		}

		@Override
		public List<InputSplit> getSplits(JobContext jobcontext) throws IOException, InterruptedException {
			Configuration conf = jobcontext.getConfiguration();
			List<InputSplit> sps = HbaseInputSplit.getPartitions(conf);
			return sps;
		}

		@Override
		public Configuration getConf() {
			return conf;
		}

		@Override
		public void setConf(Configuration configuration) {
			this.conf = configuration;

		}

	}

	public static class HbaseOutputFormat<K, T> extends OutputFormat<K, T> implements Configurable {
		Configuration conf;

		@Override
		public void checkOutputSpecs(JobContext jobcontext) throws IOException, InterruptedException {
			// TODO Auto-generated method stub

		}

		@Override
		public OutputCommitter getOutputCommitter(TaskAttemptContext taskattemptcontext) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public RecordWriter<K, T> getRecordWriter(TaskAttemptContext taskattemptcontext) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Configuration getConf() {
			return conf;
		}

		@Override
		public void setConf(Configuration configuration) {
			this.conf = configuration;
		}

	}

	public static class HbaseTableRowCountMapper extends Mapper<String, Result, Text, IntWritable> {

		int rowCount = 0;

		protected void map(String key, Result value, Context context) throws IOException, InterruptedException {
			rowCount++;
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text("ROW_COUNT"), new IntWritable(rowCount));
			System.out.println("ROW_COUNT=" + rowCount);
			context.setStatus("ROW_COUNT=" + rowCount);
		}
	}

	public class HbaseTableRowCountReducer extends Reducer<Text, IntWritable, String, IntWritable> {
		int rowCount = 0;

		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			for (IntWritable value : values) {
				rowCount += value.get();
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.setStatus("ROW_COUNT=" + rowCount);
		}
	}

	public static class HbaseInputSplit extends InputSplit implements Writable, Configurable {
		protected byte[] startKey;
		protected byte[] endKey;
		protected String tableName;
		private Configuration conf;

		public static Pair<byte[][], byte[][]> getInRangeEndKeys(HTable table, final byte[] startKey, final byte[] endKey)
				throws IOException {
			List<HRegionLocation> regions = table.getRegionsInRange(startKey, endKey);
			final List<byte[]> startKeyList = new ArrayList<byte[]>(regions.size());
			final List<byte[]> endKeyList = new ArrayList<byte[]>(regions.size());
			for (HRegionLocation region : regions) {
				startKeyList.add(region.getRegionInfo().getStartKey());
				endKeyList.add(region.getRegionInfo().getEndKey());
			}
			return new Pair<byte[][], byte[][]>(startKeyList.toArray(new byte[startKeyList.size()][]), endKeyList
					.toArray(new byte[endKeyList.size()][]));
		}

		public static List<InputSplit> getPartitions(Configuration conf) throws IOException {

			HTable table = null;
			try {
				String tableName = conf.get(HBASE_TABLE_NAME_KEY, null);
				String _startKey = conf.get(HbaseStartRowKey, null);
				String _endKey = conf.get(HbaseEndRowKey, null);
				byte[] startKey = _startKey == null ? HConstants.EMPTY_START_ROW : _startKey.getBytes();
				byte[] endKey = _endKey == null ? HConstants.EMPTY_END_ROW : _endKey.getBytes();
				table = new HTable(conf, tableName);
				// Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
				// taken from o.a.h.hbase.mapreduce.TableInputFormatBase
				Pair<byte[][], byte[][]> keys = null;
				if (startKey != HConstants.EMPTY_START_ROW || endKey != HConstants.EMPTY_END_ROW) {
					keys = getInRangeEndKeys(table, startKey, endKey);
				} else {
					keys = table.getStartEndKeys();
				}
				if (keys == null || keys.getFirst() == null || keys.getFirst().length == 0) {
					throw new IOException("Expecting at least one region.");
				}
				if (table == null) {
					throw new IOException("No table was provided.");
				}
				List<InputSplit> splits = new ArrayList<InputSplit>();
				for (int i = 0; i < keys.getFirst().length; i++) {
					// determine if the given start an stop key fall into the region
					if ((startKey.length == 0 || keys.getSecond()[i].length == 0 || Bytes.compareTo(startKey, keys.getSecond()[i]) < 0)
							&& (endKey.length == 0 || Bytes.compareTo(endKey, keys.getFirst()[i]) > 0)) {
						byte[] splitStart = startKey.length == 0 || Bytes.compareTo(keys.getFirst()[i], startKey) >= 0 ? keys.getFirst()[i]
								: startKey;
						byte[] splitStop = (endKey.length == 0 || Bytes.compareTo(keys.getSecond()[i], endKey) <= 0)
								&& keys.getSecond()[i].length > 0 ? keys.getSecond()[i] : endKey;
						splits.add(new HbaseInputSplit(conf, tableName, splitStart, splitStop));
					}
				}
				return splits;
			} catch (IOException e) {
				throw e;
			} finally {
				if (table != null)
					table.close();
			}
		}

		public HbaseInputSplit() {
		}

		public HbaseInputSplit(Configuration conf, String tableName, byte[] startKey, byte[] endKey) {
			setConf(conf);
			this.tableName = tableName;
			this.startKey = startKey;
			this.endKey = endKey;
		}

		@Override
		public Configuration getConf() {
			return conf;
		}

		@Override
		public void setConf(Configuration conf) {
			this.conf = conf;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof HbaseInputSplit) {
				HbaseInputSplit hsp = ((HbaseInputSplit) obj);
				if (!this.tableName.equals(hsp.tableName))
					return false;
				if (!this.startKey.equals(hsp.startKey))
					return false;
				if (!this.endKey.equals(hsp.endKey))
					return false;
				return true;
			}
			return false;
		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return new String[0];
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.tableName = in.readUTF();
			int len = in.readInt();
			if (len > 0) {
				this.startKey = new byte[len];
				in.readFully(this.startKey);
			}
			len = in.readInt();
			if (len > 0) {
				this.endKey = new byte[len];
				in.readFully(this.endKey);
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(this.tableName);
			out.writeInt(this.startKey.length);
			out.write(this.startKey);
			out.writeInt(this.endKey.length);
			out.write(this.endKey);
		}
	}
}
