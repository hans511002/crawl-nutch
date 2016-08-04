package org.apache.nutch.hbase.tool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDBDao {
	// �������ö���HBaseConfiguration
	static Configuration cfg = HBaseConfiguration.create();
	static {
		// Configuration configuration = new Configuration();
		// cfg = new HBaseConfiguration(configuration);
		// cfg = HBaseConfiguration.create();
	}

	public class ColumnFarily {
		String columnFarily;
		Algorithm CompressionType = Algorithm.SNAPPY;
		Algorithm CompactionCompressionType = Algorithm.SNAPPY;
		long maxFileSize = 67108864;
		int blockSize = 32768;
		int maxVersion = 3;
	}

	// ����һ�ű?ָ����������
	public static void createTable(String tableName, String... columnFarily) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(cfg);
		if (admin.tableExists(tableName)) {
			admin.close();
			return;
		}
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		for (String cf : columnFarily) {
			HColumnDescriptor col = new HColumnDescriptor(cf);
			tableDesc.setMaxFileSize(67108864 * 4);
			col.setMaxVersions(3);
			col.setCompressionType(Algorithm.SNAPPY);
			col.setBlocksize(65536);
			col.setCompactionCompressionType(Algorithm.SNAPPY);
			tableDesc.addFamily(col);
		}
		admin.createTable(tableDesc);
		admin.close();
		System.out.println("������ɹ�  MaxFileSize= " + tableDesc.getMaxFileSize());
	}

	public static void createTable(String tableName, ColumnFarily... columnFarily) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(cfg);
		if (admin.tableExists(tableName)) {
			admin.close();
			return;
		}
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		for (ColumnFarily cf : columnFarily) {
			HColumnDescriptor col = new HColumnDescriptor(cf.columnFarily);
			tableDesc.setMaxFileSize(cf.maxFileSize);
			col.setMaxVersions(cf.maxVersion);
			col.setCompressionType(cf.CompressionType);
			col.setBlocksize(cf.blockSize);
			col.setCompactionCompressionType(cf.CompactionCompressionType);
			tableDesc.addFamily(col);
		}
		admin.createTable(tableDesc);
		admin.close();
		System.out.println("������ɹ�  MaxFileSize= " + tableDesc.getMaxFileSize());
	}

	public static void dropTable(String tableName) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(cfg);

		if (!admin.tableExists(tableName)) {
			admin.close();
			return;
		}
		admin.deleteTable(tableName);
		admin.close();
	}

	public static void dropTableCol(String tableName, String... columnFarily) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(cfg);
		if (!admin.tableExists(tableName)) {
			admin.close();
			return;
		}
		for (String columnName : columnFarily)
			admin.deleteColumn(tableName, columnName);
		admin.close();
	}

	public static void addData(String tableName, String rowKey, String columnFamily, String column, String data) throws Exception {
		HTable table = new HTable(cfg, tableName);

		try {
			Put pt = new Put(rowKey.getBytes());
			pt.add(columnFamily.getBytes(), column.getBytes(), data.getBytes());
			table.put(pt);
			table.flushCommits();
			System.out.println("��ӳɹ���");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			table.close();
		}

	}

	public static void addData(String tableName, String rowKey, String columnFamily, String[] column, String[] data) throws Exception {
		HTable table = new HTable(cfg, tableName);
		try {
			Put pt = new Put(rowKey.getBytes());
			for (int i = 0; i < column.length; i++)
				pt.add(columnFamily.getBytes(), column[i].getBytes(), data[i].getBytes());
			table.put(pt);
			table.flushCommits();
			System.out.println("��ӳɹ���");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			table.close();
		}
	}

	public static void addData(String tableName, String rowKey, String columnFamily, Map<String, String> data) throws Exception {
		HTable table = new HTable(cfg, tableName);
		try {
			Put pt = new Put(rowKey.getBytes());
			for (String key : data.keySet())
				pt.add(columnFamily.getBytes(), key.getBytes(), data.get(key).getBytes());
			table.put(pt);
			table.flushCommits();
			System.out.println("��ӳɹ���");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			table.close();
		}
	}

	// �����ݣ�ͨ��HTable����BatchUpdateΪ�Ѿ����ڵı�������data
	public static void appendData(String tableName, String rowKey, String columnFamily, String column, String data) throws Exception {
		HTable table = new HTable(cfg, tableName);
		try {
			Put pt = new Put("120".getBytes());
			pt.add("cf1".getBytes(), "val".getBytes(), "www.baidu.coms".getBytes());
			// KeyValue kv = new KeyValue("100".getBytes(), "cf1".getBytes(),
			// "val".getBytes(), "www.360buy.com".getBytes());
			// Put pt2 = new Put("120".getBytes());
			// pt2.add(kv);
			Append append = new Append(rowKey.getBytes());
			append.add(columnFamily.getBytes(), column.getBytes(), data.getBytes());
			table.append(append);
			table.flushCommits();
			System.out.println("��ӳɹ���");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			table.close();
		}
	}

	public static Map<String, String> getData(String tableName, String rowKey) throws Exception {
		HTable table = new HTable(cfg, tableName);
		try {
			Get get = new Get(rowKey.getBytes());
			Result r = table.get(get);
			// NavigableMap<HRegionInfo, ServerName> reginInfo = table.getRegionLocations();
			return getData(r);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			table.close();
		}
		return null;
	}

	public static ResultScanner getScan(String tableName, String startRow, String stopRow) throws IOException {
		HTable table = new HTable(cfg, tableName);
		Scan scan = new Scan();
		scan.setStartRow(startRow.getBytes());
		scan.setStopRow(stopRow.getBytes());
		return table.getScanner(scan);
	}

	public static Map<String, String> getData(Result r) throws Exception {
		Map<String, String> res = new HashMap<String, String>();
		for (KeyValue kv : r.raw()) {
			String key = new String(kv.getFamily()) + ":" + new String(kv.getQualifier());
			String value = new String(kv.getValue());
			res.put(key, value);
		}
		return res;
	}

	public static void deleteData(String tableName, String rowKey) throws Exception {
		deleteData(tableName, rowKey, null, null, -1);
	}

	public static void deleteData(String tableName, String rowKey, String columnFamily) throws Exception {
		deleteData(tableName, rowKey, columnFamily, null, -1);
	}

	public static void deleteData(String tableName, String rowKey, String columnFamily, String column) throws Exception {
		deleteData(tableName, rowKey, columnFamily, column, -1);
	}

	public static void deleteData(String tableName, String rowKey, String columnFamily, String column, long timestamp) throws Exception {
		HTable table = new HTable(cfg, tableName);
		try {
			Delete d = new Delete(rowKey.getBytes());
			if (columnFamily != null && !columnFamily.equals("")) {
				if (column != null && !column.equals("")) {
					if (timestamp < 0)
						d.deleteColumn(columnFamily.getBytes(), column.getBytes());
					else
						d.deleteColumn(columnFamily.getBytes(), column.getBytes(), timestamp);
				} else {
					if (timestamp < 0)
						d.deleteFamily(columnFamily.getBytes());
					else
						d.deleteFamily(columnFamily.getBytes(), timestamp);
				}
			} else {
				HColumnDescriptor[] cfs = table.getTableDescriptor().getColumnFamilies();

				for (int i = 0; i < cfs.length; i++) {
					if (column != null && !column.equals("")) {
						if (timestamp < 0)
							d.deleteColumn(cfs[0].getName(), column.getBytes());
						else
							d.deleteColumn(cfs[0].getName(), column.getBytes(), timestamp);
					} else {
						if (timestamp < 0)
							d.deleteFamily(columnFamily.getBytes());
						else
							d.deleteFamily(columnFamily.getBytes(), timestamp);
					}
				}
			}
			table.delete(d);
			table.flushCommits();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			table.close();
		}
	}

	// ��ʾ������ݣ�ͨ��HTable Scan���ȡ���б����Ϣ
	public static void getAllData(String tableName) throws Exception {
		HTable table = new HTable(cfg, tableName);
		try {
			Scan scan = new Scan();
			scan.setMaxVersions();
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				for (KeyValue kv : r.raw()) {
					System.out.println(new String(kv.getRow()) + "  " + kv.getTimestamp() + " =>" + new String(kv.getFamily()) + ":"
							+ new String(kv.getQualifier()) + "  = " + new String(kv.getValue()));
				}

				// one way get values of all version
				// final List<KeyValue> list = r.list();
				// for (final KeyValue kv : list)
				// {
				// System.err.println(Bytes.toString(kv.getKey()));
				// System.err.println(kv.getTimestamp());
				// System.err.println(Bytes.toString(kv.getValue()));
				// }

				final NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = r.getMap();
				final NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap = map.get("cf1".getBytes());
				for (final byte[] qu : familyMap.keySet()) {
					final NavigableMap<Long, byte[]> versionMap = familyMap.get(qu);
					for (final Map.Entry<Long, byte[]> entry : versionMap.entrySet()) {
						System.err.println(Bytes.toString(qu) + "   " + entry.getKey() + " -> " + Bytes.toString(entry.getValue()));
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			table.close();
		}
	}
}
