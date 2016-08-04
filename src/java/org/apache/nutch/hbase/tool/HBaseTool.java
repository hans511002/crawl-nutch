package org.apache.nutch.hbase.tool;

import static org.apache.gora.hbase.util.HBaseByteInterface.toBytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.gora.persistency.Persistent;
import org.apache.gora.query.Query;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseTool {
	public static final Logger LOG = LoggerFactory.getLogger(HBaseTool.class);

	public static long deleteRange(HTable table, Scan scan) throws IOException {
		return deleteRange(table, scan, null);
	}

	public static long deleteRange(HTable table, Scan scan, String[] cfqus) throws IOException {
		ArrayList<Delete> deletes = new ArrayList<Delete>();
		ResultScanner rs;
		long count = 0;
		try {
			rs = table.getScanner(scan);
			byte[][] cfs = null;
			byte[][][] cqus = null;
			if (cfqus != null) {
				HashMap<String, List<String>> cfq = new HashMap<String, List<String>>();
				for (int i = 0; i < cfqus.length; i++) {
					String[] tmp = cfqus[i].split(":");
					if (tmp[0].equals(""))
						continue;
					List<String> list = null;
					if (cfq.containsKey(tmp[0])) {
						list = cfq.get(tmp[0]);
					} else {
						list = new ArrayList<String>();
						cfq.put(tmp[0], list);
					}
					if (tmp.length > 1 && tmp[1].equals("") == false)
						list.add(tmp[1]);
				}
				cfs = new byte[cfq.size()][];
				cqus = new byte[cfq.size()][][];
				int c = 0;
				for (String key : cfq.keySet()) {
					cfs[c] = key.getBytes();
					List<String> qu = cfq.get(key);
					cqus[c] = new byte[qu.size()][];
					int i = 0;
					for (String q : qu) {
						cqus[c][i++] = q.getBytes();
					}
					c++;
				}
			}
			for (Result r : rs) {
				count++;
				Delete delete = new Delete(r.getRow());
				LOG.info("delete row: " + new String(r.getRow()));
				if (cfqus != null) {// 指定的
					for (int i = 0; i < cfs.length; i++) {
						if (cqus[i] == null) {
							delete.deleteFamily(cfs[i]);
						} else {
							for (byte[] qu : cqus[i]) {
								delete.deleteColumn(cfs[i], qu);
							}
						}
					}
				} else {
					for (KeyValue kv : r.raw()) {
						delete.deleteColumn(kv.getFamily(), kv.getQualifier());
					}
				}
				deletes.add(delete);
				if (deletes.size() % 1000 == 0) {
					table.delete(deletes);
					table.flushCommits();
					deletes.clear();
				}
			}
			if (deletes.size() > 0)
				table.delete(deletes);
			table.flushCommits();
			return count;
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
	}

	public static long deleteRange(HTable table, byte[] startKey, byte[] endKey) throws IOException {
		Scan scan = new Scan();
		ArrayList<Delete> deletes = new ArrayList<Delete>();
		scan.setStartRow(startKey);
		scan.setStopRow(endKey);
		ResultScanner rs;
		long count = 0;
		try {
			rs = table.getScanner(scan);
			for (Result r : rs) {
				count++;
				Delete delete = new Delete(r.getRow());
				LOG.info("delete row: " + new String(r.getRow()));
				deletes.add(delete);
				if (deletes.size() % 1000 == 0) {
					table.delete(deletes);
					table.flushCommits();
					deletes.clear();
				}
			}
			if (deletes.size() > 0)
				table.delete(deletes);
			table.flushCommits();
			return count;
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
	}

	public static <K, T extends Persistent> long deleteAllByQuery(HTable table, Query<K, T> query) throws Exception {
		try {
			org.apache.gora.query.Result<K, T> result = null;
			result = query.execute();
			ArrayList<Delete> deletes = new ArrayList<Delete>();
			long count = 0;
			while (result.next()) {
				Delete delete = new Delete(toBytes(result.getKey()));
				deletes.add(delete);
				count++;
				if (deletes.size() % 50000 == 0) {
					table.delete(deletes);
					deletes.clear();
				}
			}
			if (deletes.size() > 0)
				table.delete(deletes);
			return count;
		} catch (Exception ex) {
			LOG.error(ex.getMessage());
			LOG.error(ex.getStackTrace().toString());
			throw ex;
		}
	}

}
